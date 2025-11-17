using Azure;
using documentchecker.Models;
using documentchecker.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using System.Web;
using CsvHelper;
using OfficeOpenXml;

namespace documentchecker.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MainController : ControllerBase
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IMemoryCache _cache;
        private readonly ChatService _chatService;
        private readonly QueryHistoryService _queryHistoryService;
        private readonly string _meAiEndpoint;
        private readonly string _meAiDeploymentName;
        private readonly string _meAiApiVersion;
        private readonly string _meAiApiKey;
        private readonly string _zohoClientId;
        private readonly string _zohoClientSecret;
        private readonly string _zohoRefreshToken;
        private readonly string _zohoRedirectUri;
        private readonly AppDbContext _dbContext;

        public MainController(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            IMemoryCache cache,
            ChatService chatService,
            QueryHistoryService queryHistoryService,
            AppDbContext dbContext)
        {
            _httpClientFactory = httpClientFactory;
            _cache = cache;
            _chatService = chatService;
            _queryHistoryService = queryHistoryService;
            _meAiEndpoint = configuration["ManageEngineAI:Endpoint"] ?? throw new InvalidOperationException("ManageEngineAI:Endpoint not configured.");
            _meAiDeploymentName = configuration["ManageEngineAI:DeploymentName"] ?? throw new InvalidOperationException("ManageEngineAI:DeploymentName not configured.");
            _meAiApiVersion = configuration["ManageEngineAI:ApiVersion"] ?? throw new InvalidOperationException("ManageEngineAI:ApiVersion not configured.");
            _meAiApiKey = configuration["ManageEngineAI:ApiKey"] ?? throw new InvalidOperationException("ManageEngineAI:ApiKey not configured.");
            _zohoClientId = configuration["Zoho:ClientId"] ?? throw new InvalidOperationException("Zoho:ClientId not configured.");
            _zohoClientSecret = configuration["Zoho:ClientSecret"] ?? throw new InvalidOperationException("Zoho:ClientSecret not configured.");
            _zohoRefreshToken = configuration["Zoho:RefreshToken"] ?? throw new InvalidOperationException("Zoho:RefreshToken not configured.");
            _zohoRedirectUri = configuration["Zoho:RedirectUri"] ?? throw new InvalidOperationException("Zoho:RedirectUri not configured.");
            _dbContext = dbContext;
        }

        // Sync endpoint - fetches most recent records from ManageEngine and stores them
        [HttpPost("sync")]
        public async Task<IActionResult> SyncRecentRecords()
        {
            try
            {
                var lastStoredDate = await _dbContext.ManageEngineRequests
                    .OrderByDescending(r => r.CreatedTime)
                    .Select(r => r.CreatedTime)
                    .FirstOrDefaultAsync();

                var dateFrom = lastStoredDate != default ? lastStoredDate.AddHours(-1) : new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
                var dateTo = DateTimeOffset.UtcNow;

                Console.WriteLine($"[SYNC] Fetching records from {dateFrom:yyyy-MM-dd HH:mm:ss} to {dateTo:yyyy-MM-dd HH:mm:ss}");

                var requests = await FetchFromManageEngine(dateFrom, dateTo);

                int newCount = 0;
                var existingIds = await _dbContext.ManageEngineRequests
                    .Select(r => r.Id)
                    .ToHashSetAsync();

                foreach (var req in requests)
                {
                    var requestId = req["id"]?.ToString();
                    if (!string.IsNullOrEmpty(requestId) && !existingIds.Contains(requestId))
                    {
                        var meRequest = new ManageEngineRequest
                        {
                            Id = requestId,
                            Subject = ExtractField(req, "subject"),
                            TechnicianName = ExtractTechnicianName(req),
                            CreatedTime = ExtractCreatedTime(req),
                            JsonData = JsonSerializer.Serialize(req)
                        };

                        _dbContext.ManageEngineRequests.Add(meRequest);
                        newCount++;
                    }
                }

                if (newCount > 0)
                {
                    await _dbContext.SaveChangesAsync();
                    Console.WriteLine($"[SYNC] Stored {newCount} new records");
                }

                return Ok(new { Success = true, NewRecords = newCount, TotalFetched = requests.Count, LastSyncTime = DateTime.UtcNow });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SYNC] Error: {ex.Message}");
                return StatusCode(500, new { Success = false, Error = ex.Message });
            }
        }

        // Main AI-powered query endpoint - understands natural language perfectly
        [HttpPost("query")]
        public async Task<IActionResult> Query([FromBody] QueryRequest request)
        {
            if (string.IsNullOrEmpty(request?.Query))
                return BadRequest(new { Success = false, Error = "Query is required." });

            try
            {
                Console.WriteLine($"[QUERY] User query: {request.Query}");

                // Use AI to understand the query and extract intent + parameters
                var queryIntent = await ParseQueryWithAI(request.Query);
                Console.WriteLine($"[QUERY] Parsed intent: Type={queryIntent.Type}, Params={JsonSerializer.Serialize(queryIntent.Parameters)}");

                // Route to appropriate handler based on intent
                return queryIntent.Type switch
                {
                    "top-request-areas" => await HandleTopRequestAreas(queryIntent.Parameters),
                    "request-influx" => await HandleRequestInflux(queryIntent.Parameters),
                    "inactive-technicians" => await HandleInactiveTechnicians(queryIntent.Parameters),
                    "technician-workload" => await HandleTechnicianWorkload(queryIntent.Parameters),
                    "requests-by-subject" => await HandleRequestsBySubject(queryIntent.Parameters),
                    "requests-by-technician" => await HandleRequestsByTechnician(queryIntent.Parameters),
                    "general-search" => await HandleGeneralSearch(queryIntent.Parameters),
                    _ => BadRequest(new { Success = false, Error = "Unable to understand query type" })
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[QUERY] Error: {ex.Message}");
                return StatusCode(500, new { Success = false, Error = ex.Message });
            }
        }

        // AI-powered query parser - extracts intent and parameters from natural language
        private async Task<QueryIntent> ParseQueryWithAI(string userQuery)
        {
            var apiClient = _httpClientFactory.CreateClient();
            var fullUrl = $"{_meAiEndpoint}openai/deployments/{_meAiDeploymentName}/chat/completions?api-version={_meAiApiVersion}";

            string currentDate = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

            var systemPrompt = $@"You are a query parser for a support ticket system. Current date/time: {currentDate}

Parse the user's query and return ONLY a JSON object with this structure:
{{
  ""type"": ""<query_type>"",
  ""parameters"": {{...extracted parameters...}}
}}

Query types:
1. ""top-request-areas"" - User wants to see most common request subjects/categories
   Parameters: dateFrom (yyyy-MM-dd), dateTo (yyyy-MM-dd)
   
2. ""request-influx"" - User wants to see when requests peaked (hour-by-hour analysis)
   Parameters: dateFrom (yyyy-MM-dd), dateTo (yyyy-MM-dd), groupBy (""hour"" or ""day"")
   
3. ""inactive-technicians"" - User wants to see technicians with no activity
   Parameters: dateFrom (yyyy-MM-dd), dateTo (yyyy-MM-dd)
   
4. ""technician-workload"" - User wants to see how much work each technician has
   Parameters: dateFrom (yyyy-MM-dd), dateTo (yyyy-MM-dd)
   
5. ""requests-by-subject"" - User wants requests about a specific topic
   Parameters: subject (string), dateFrom (yyyy-MM-dd), dateTo (yyyy-MM-dd)
   
6. ""requests-by-technician"" - User wants requests handled by a specific technician
   Parameters: technician (string), dateFrom (yyyy-MM-dd), dateTo (yyyy-MM-dd)
   
7. ""general-search"" - User wants to search with custom filters
   Parameters: subject (string), technician (string), dateFrom (yyyy-MM-dd), dateTo (yyyy-MM-dd)

Date interpretation (from current date {currentDate}):
- ""today"" = {DateTime.UtcNow:yyyy-MM-dd} to {DateTime.UtcNow:yyyy-MM-dd}
- ""yesterday"" = {DateTime.UtcNow.AddDays(-1):yyyy-MM-dd} to {DateTime.UtcNow.AddDays(-1):yyyy-MM-dd}
- ""past 2 hours"" = 2 hours ago to now
- ""past 24 hours"" or ""last day"" = 24 hours ago to now
- ""past week"" or ""last 7 days"" = 7 days ago to now
- ""past 2 weeks"" = 14 days ago to now
- ""past month"" or ""last 30 days"" = 30 days ago to now
- ""past 3 months"" = 90 days ago to now
- ""past year"" = 365 days ago to now

Examples:
Query: ""Show me top request areas for yesterday""
Output: {{""type"": ""top-request-areas"", ""parameters"": {{""dateFrom"": ""{DateTime.UtcNow.AddDays(-1):yyyy-MM-dd}"", ""dateTo"": ""{DateTime.UtcNow.AddDays(-1):yyyy-MM-dd}""}}}}

Query: ""What hour did we get the most requests today?""
Output: {{""type"": ""request-influx"", ""parameters"": {{""dateFrom"": ""{DateTime.UtcNow:yyyy-MM-dd}"", ""dateTo"": ""{DateTime.UtcNow:yyyy-MM-dd}"", ""groupBy"": ""hour""}}}}

Query: ""Which technicians have not treated any request in 2 weeks?""
Output: {{""type"": ""inactive-technicians"", ""parameters"": {{""dateFrom"": ""{DateTime.UtcNow.AddDays(-14):yyyy-MM-dd}"", ""dateTo"": ""{DateTime.UtcNow:yyyy-MM-dd}""}}}}

Query: ""Password reset requests from the past 2 hours""
Output: {{""type"": ""requests-by-subject"", ""parameters"": {{""subject"": ""password reset"", ""dateFrom"": ""{DateTime.UtcNow.AddHours(-2):yyyy-MM-dd HH:mm:ss}"", ""dateTo"": ""{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}""}}}}

Return ONLY the JSON object, no other text.";

            var requestBody = new
            {
                messages = new[]
                {
                    new { role = "system", content = systemPrompt },
                    new { role = "user", content = userQuery }
                },
                max_tokens = 300,
                temperature = 0.3
            };

            var json = JsonSerializer.Serialize(requestBody);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            apiClient.DefaultRequestHeaders.Add("api-key", _meAiApiKey);

            var response = await apiClient.PostAsync(fullUrl, content);

            if (!response.IsSuccessStatusCode)
                throw new Exception($"AI API failed: {response.StatusCode}");

            var responseJson = await response.Content.ReadAsStringAsync();
            var aiResponse = JsonSerializer.Deserialize<AiResponse>(responseJson);

            if (aiResponse?.Choices == null || aiResponse.Choices.Count == 0)
                throw new Exception("Invalid AI response");

            var outputContent = aiResponse.Choices[0].Message.Content;
            Console.WriteLine($"[AI] Raw output: {outputContent}");

            var intent = JsonSerializer.Deserialize<QueryIntent>(outputContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? throw new Exception("Failed to parse query intent");

            return intent;
        }

        // Handler: Top request areas
        private async Task<IActionResult> HandleTopRequestAreas(Dictionary<string, object> parameters)
        {
            var (dateFrom, dateTo) = ExtractDateRange(parameters);

            var topAreas = await _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.Subject))
                .GroupBy(r => r.Subject)
                .Select(g => new { Subject = g.Key, Count = g.Count() })
                .OrderByDescending(x => x.Count)
                .Take(15)
                .ToListAsync();

            return Ok(new
            {
                Success = true,
                Type = "top-request-areas",
                Data = topAreas,
                Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                TotalRecords = topAreas.Sum(x => x.Count),
                Timestamp = DateTime.UtcNow
            });
        }

        // Handler: Request influx (hour-by-hour or day-by-day analysis)
        private async Task<IActionResult> HandleRequestInflux(Dictionary<string, object> parameters)
        {
            var (dateFrom, dateTo) = ExtractDateRange(parameters);
            var groupBy = parameters.ContainsKey("groupBy") ? parameters["groupBy"]?.ToString() : "hour";

            if (groupBy == "hour")
            {
                var hourlyData = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo)
                    .GroupBy(r => new { Date = r.CreatedTime.Date, Hour = r.CreatedTime.Hour })
                    .Select(g => new
                    {
                        DateTime = g.Key.Date.AddHours(g.Key.Hour),
                        Count = g.Count(),
                        DisplayTime = $"{g.Key.Date:yyyy-MM-dd} {g.Key.Hour:D2}:00"
                    })
                    .OrderBy(x => x.DateTime)
                    .ToListAsync();

                var peakHour = hourlyData.OrderByDescending(x => x.Count).FirstOrDefault();

                return Ok(new
                {
                    Success = true,
                    Type = "request-influx",
                    Data = hourlyData,
                    Peak = peakHour,
                    PeakMessage = peakHour != null ? $"Highest influx at {peakHour.DisplayTime} with {peakHour.Count} requests" : "No data",
                    Period = $"{dateFrom:yyyy-MM-dd HH:mm:ss} to {dateTo:yyyy-MM-dd HH:mm:ss}",
                    TotalRecords = hourlyData.Sum(x => x.Count),
                    Timestamp = DateTime.UtcNow
                });
            }
            else // day
            {
                var dailyData = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo)
                    .GroupBy(r => r.CreatedTime.Date)
                    .Select(g => new
                    {
                        Date = g.Key,
                        Count = g.Count(),
                        DisplayDate = g.Key.ToString("yyyy-MM-dd")
                    })
                    .OrderBy(x => x.Date)
                    .ToListAsync();

                var peakDay = dailyData.OrderByDescending(x => x.Count).FirstOrDefault();

                return Ok(new
                {
                    Success = true,
                    Type = "request-influx",
                    Data = dailyData,
                    Peak = peakDay,
                    PeakMessage = peakDay != null ? $"Highest influx on {peakDay.DisplayDate} with {peakDay.Count} requests" : "No data",
                    Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                    TotalRecords = dailyData.Sum(x => x.Count),
                    Timestamp = DateTime.UtcNow
                });
            }
        }

        // Handler: Inactive technicians
        private async Task<IActionResult> HandleInactiveTechnicians(Dictionary<string, object> parameters)
        {
            var (dateFrom, dateTo) = ExtractDateRange(parameters);

            var allTechnicians = await _dbContext.ManageEngineRequests
                .Where(r => !string.IsNullOrEmpty(r.TechnicianName))
                .Select(r => r.TechnicianName)
                .Distinct()
                .ToListAsync();

            var activeTechnicians = await _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.TechnicianName))
                .Select(r => r.TechnicianName)
                .Distinct()
                .ToListAsync();

            var inactiveTechs = allTechnicians.Except(activeTechnicians).ToList();

            return Ok(new
            {
                Success = true,
                Type = "inactive-technicians",
                Data = inactiveTechs,
                InactiveCount = inactiveTechs.Count,
                TotalTechnicians = allTechnicians.Count,
                Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                Timestamp = DateTime.UtcNow
            });
        }

        // Handler: Technician workload
        private async Task<IActionResult> HandleTechnicianWorkload(Dictionary<string, object> parameters)
        {
            var (dateFrom, dateTo) = ExtractDateRange(parameters);

            var workload = await _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.TechnicianName))
                .GroupBy(r => r.TechnicianName)
                .Select(g => new { Technician = g.Key, RequestCount = g.Count() })
                .OrderByDescending(x => x.RequestCount)
                .ToListAsync();

            return Ok(new
            {
                Success = true,
                Type = "technician-workload",
                Data = workload,
                Total = workload.Sum(x => x.RequestCount),
                HighestWorkload = workload.FirstOrDefault(),
                Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                Timestamp = DateTime.UtcNow
            });
        }

        // Handler: Requests by subject
        private async Task<IActionResult> HandleRequestsBySubject(Dictionary<string, object> parameters)
        {
            var (dateFrom, dateTo) = ExtractDateRange(parameters);
            var subject = parameters.ContainsKey("subject") ? parameters["subject"]?.ToString() : null;

            if (string.IsNullOrEmpty(subject))
                return BadRequest(new { Success = false, Error = "Subject is required" });

            var requests = await _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && r.Subject.Contains(subject))
                .OrderByDescending(r => r.CreatedTime)
                .Take(100)
                .Select(r => new
                {
                    r.Id,
                    r.Subject,
                    r.TechnicianName,
                    r.CreatedTime,
                    JsonData = JsonDocument.Parse(r.JsonData, new JsonDocumentOptions()).RootElement
                })
                .ToListAsync();

            return Ok(new
            {
                Success = true,
                Type = "requests-by-subject",
                Data = requests,
                Count = requests.Count,
                Subject = subject,
                Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                Timestamp = DateTime.UtcNow
            });
        }

        // Handler: Requests by technician
        private async Task<IActionResult> HandleRequestsByTechnician(Dictionary<string, object> parameters)
        {
            var (dateFrom, dateTo) = ExtractDateRange(parameters);
            var technician = parameters.ContainsKey("technician") ? parameters["technician"]?.ToString() : null;

            if (string.IsNullOrEmpty(technician))
                return BadRequest(new { Success = false, Error = "Technician is required" });

            var requests = await _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && r.TechnicianName.Contains(technician))
                .OrderByDescending(r => r.CreatedTime)
                .Take(100)
                .Select(r => new
                {
                    r.Id,
                    r.Subject,
                    r.TechnicianName,
                    r.CreatedTime,
                    JsonData = JsonDocument.Parse(r.JsonData, new JsonDocumentOptions()).RootElement
                })
                .ToListAsync();

            return Ok(new
            {
                Success = true,
                Type = "requests-by-technician",
                Data = requests,
                Count = requests.Count,
                Technician = technician,
                Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                Timestamp = DateTime.UtcNow
            });
        }

        // Handler: General search
        private async Task<IActionResult> HandleGeneralSearch(Dictionary<string, object> parameters)
        {
            var (dateFrom, dateTo) = ExtractDateRange(parameters);
            var subject = parameters.ContainsKey("subject") ? parameters["subject"]?.ToString() : null;
            var technician = parameters.ContainsKey("technician") ? parameters["technician"]?.ToString() : null;

            var query = _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo);

            if (!string.IsNullOrEmpty(subject))
                query = query.Where(r => r.Subject.Contains(subject));

            if (!string.IsNullOrEmpty(technician))
                query = query.Where(r => r.TechnicianName.Contains(technician));

            var requests = await query
                .OrderByDescending(r => r.CreatedTime)
                .Take(100)
                .Select(r => new
                {
                    r.Id,
                    r.Subject,
                    r.TechnicianName,
                    r.CreatedTime,
                    JsonData = JsonDocument.Parse(r.JsonData, new JsonDocumentOptions()).RootElement
                })
                .ToListAsync();

            return Ok(new
            {
                Success = true,
                Type = "general-search",
                Data = requests,
                Count = requests.Count,
                Filters = new { subject, technician },
                Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                Timestamp = DateTime.UtcNow
            });
        }

        // Fetch records from ManageEngine API
        private async Task<List<Dictionary<string, object>>> FetchFromManageEngine(DateTimeOffset dateFrom, DateTimeOffset dateTo)
        {
            var apiClient = _httpClientFactory.CreateClient();
            apiClient.Timeout = TimeSpan.FromSeconds(60);

            var accessToken = await GetZohoAccessToken();
            apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Zoho-oauthtoken", accessToken);

            var allRequests = new List<Dictionary<string, object>>();
            var seenIds = new HashSet<string>();
            bool hasMoreRows = true;
            int pageNumber = 1;
            const int rowCount = 100;

            long dateFromMs = dateFrom.ToUnixTimeMilliseconds();
            long dateToMs = dateTo.ToUnixTimeMilliseconds();

            var searchCriteria = new Dictionary<string, object>
            {
                ["field"] = "created_time",
                ["condition"] = "between",
                ["values"] = new[] { dateFromMs.ToString(), dateToMs.ToString() }
            };

            using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromMinutes(15));

            while (hasMoreRows && pageNumber <= 50)
            {
                var listInfo = new
                {
                    row_count = rowCount,
                    start_index = (pageNumber - 1) * rowCount,
                    sort_field = "created_time",
                    sort_order = "desc",
                    get_total_count = true,
                    search_criteria = searchCriteria
                };

                var inputData = new { list_info = listInfo };
                var inputDataJson = JsonSerializer.Serialize(inputData, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
                var encodedInputData = HttpUtility.UrlEncode(inputDataJson);
                string url = $"https://sdpondemand.manageengine.com/api/v3/requests?input_data={encodedInputData}";

                var response = await apiClient.GetAsync(url, cts.Token);

                if (!response.IsSuccessStatusCode)
                {
                    if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                    {
                        _cache.Remove("ZohoAccessToken");
                        accessToken = await GetZohoAccessToken();
                        apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Zoho-oauthtoken", accessToken);
                        response = await apiClient.GetAsync(url, cts.Token);
                    }

                    if (!response.IsSuccessStatusCode)
                        throw new Exception($"ManageEngine API error: {response.StatusCode}");
                }

                string jsonResponse = await response.Content.ReadAsStringAsync(cts.Token);
                var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse) ?? new Dictionary<string, object>();

                var requestsElem = data.ContainsKey("requests") ? (JsonElement)data["requests"] : JsonDocument.Parse("[]").RootElement;
                var currentRequests = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(requestsElem.GetRawText(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new List<Dictionary<string, object>>();

                foreach (var req in currentRequests)
                {
                    if (req.TryGetValue("id", out var idObj) && seenIds.Add(idObj.ToString()))
                        allRequests.Add(req);
                }

                hasMoreRows = false;
                if (data.TryGetValue("list_info", out var listInfoObj) && listInfoObj is JsonElement listInfoElem)
                    if (listInfoElem.TryGetProperty("has_more_rows", out var hasMoreProp))
                        hasMoreRows = hasMoreProp.GetBoolean();

                pageNumber++;
                if (hasMoreRows) await Task.Delay(50, cts.Token);
            }

            return allRequests;
        }

        // Get Zoho access token
        private async Task<string> GetZohoAccessToken()
        {
            const string cacheKey = "ZohoAccessToken";

            if (_cache.TryGetValue(cacheKey, out string cachedToken))
                return cachedToken;

            try
            {
                var client = _httpClientFactory.CreateClient();
                var formContent = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("refresh_token", _zohoRefreshToken),
                    new KeyValuePair<string, string>("grant_type", "refresh_token"),
                    new KeyValuePair<string, string>("client_id", _zohoClientId),
                    new KeyValuePair<string, string>("client_secret", _zohoClientSecret),
                    new KeyValuePair<string, string>("redirect_uri", _zohoRedirectUri)
                });

                var response = await client.PostAsync("https://accounts.zoho.com/oauth/v2/token", formContent);

                if (!response.IsSuccessStatusCode)
                    throw new Exception($"Failed to get Zoho token: {response.StatusCode}");

                string jsonResponse = await response.Content.ReadAsStringAsync();
                var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse) ?? throw new Exception("Invalid Zoho response");

                if (!data.TryGetValue("access_token", out var accessTokenObj))
                    throw new Exception("Access token not found in Zoho response");

                string accessToken = accessTokenObj.ToString();
                int expiresIn = 3600;

                if (data.TryGetValue("expires_in", out var expiresInObj) && int.TryParse(expiresInObj.ToString(), out int parsedExpiresIn))
                    expiresIn = parsedExpiresIn;

                _cache.Set(cacheKey, accessToken, TimeSpan.FromSeconds(expiresIn - 60));
                return accessToken;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AUTH] Error: {ex.Message}");
                throw;
            }
        }

        // Import requests from CSV/XLSX
        [HttpPost("import")]
        public async Task<IActionResult> ImportRequests(IFormFile file)
        {
            if (file == null || file.Length == 0)
                return BadRequest(new { Success = false, Error = "No file uploaded" });

            var requests = new List<ManageEngineRequest>();

            try
            {
                using var stream = file.OpenReadStream();

                if (file.FileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                {
                    using var reader = new StreamReader(stream);
                    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
                    var records = csv.GetRecords<dynamic>().ToList();

                    foreach (var record in records)
                    {
                        var dict = (IDictionary<string, object>)record;
                        requests.Add(new ManageEngineRequest
                        {
                            Id = dict.ContainsKey("Request ID") ? dict["Request ID"]?.ToString() : Guid.NewGuid().ToString(),
                            Subject = dict.ContainsKey("Subject") ? dict["Subject"]?.ToString() : null,
                            TechnicianName = dict.ContainsKey("Technician") ? dict["Technician"]?.ToString() : null,
                            CreatedTime = dict.ContainsKey("Created Date")
                                ? DateTimeOffset.Parse(dict["Created Date"].ToString())
                                : DateTimeOffset.UtcNow,
                            JsonData = JsonSerializer.Serialize(dict)
                        });
                    }
                }
                else if (file.FileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
                {
                    using var package = new ExcelPackage(stream);
                    var worksheet = package.Workbook.Worksheets[0];
                    var rowCount = worksheet.Dimension.Rows;
                    var colCount = worksheet.Dimension.Columns;

                    var headers = new List<string>();
                    for (int col = 1; col <= colCount; col++)
                        headers.Add(worksheet.Cells[1, col].Text);

                    for (int row = 2; row <= rowCount; row++)
                    {
                        var dict = new Dictionary<string, object>();
                        for (int col = 1; col <= colCount; col++)
                            dict[headers[col - 1]] = worksheet.Cells[row, col].Text;

                        requests.Add(new ManageEngineRequest
                        {
                            Id = dict.ContainsKey("id") ? dict["id"]?.ToString() : Guid.NewGuid().ToString(),
                            Subject = dict.ContainsKey("subject") ? dict["subject"]?.ToString() : null,
                            TechnicianName = dict.ContainsKey("technician") ? dict["technician"]?.ToString() : null,
                            CreatedTime = dict.ContainsKey("created_time")
                                ? DateTimeOffset.Parse(dict["created_time"].ToString())
                                : DateTimeOffset.UtcNow,
                            JsonData = JsonSerializer.Serialize(dict)
                        });
                    }
                }
                else
                {
                    return BadRequest(new { Success = false, Error = "Unsupported file format. Use CSV or XLSX." });
                }

                var existingIds = await _dbContext.ManageEngineRequests
                    .Select(r => r.Id)
                    .ToHashSetAsync();

                var newRequests = requests
                    .Where(r => !existingIds.Contains(r.Id))
                    .ToList();

                await _dbContext.ManageEngineRequests.AddRangeAsync(newRequests);
                await _dbContext.SaveChangesAsync();

                return Ok(new
                {
                    Success = true,
                    Imported = newRequests.Count,
                    Skipped = requests.Count - newRequests.Count,
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Success = false, Error = ex.Message });
            }
        }

        // Helper: Extract date range from parameters
        private (DateTimeOffset dateFrom, DateTimeOffset dateTo) ExtractDateRange(Dictionary<string, object> parameters)
        {
            var now = DateTime.UtcNow;
            var dateFrom = now;
            var dateTo = now;

            if (parameters.ContainsKey("dateFrom") && DateTime.TryParse(parameters["dateFrom"]?.ToString(), out var from))
                dateFrom = from;

            if (parameters.ContainsKey("dateTo") && DateTime.TryParse(parameters["dateTo"]?.ToString(), out var to))
                dateTo = to;

            return (new DateTimeOffset(dateFrom), new DateTimeOffset(dateTo));
        }

        // Helper: Extract field from nested JSON
        private string ExtractField(Dictionary<string, object> req, string fieldName)
        {
            if (req.TryGetValue(fieldName, out var value) && value != null)
                return value.ToString();
            return null;
        }

        // Helper: Extract technician name from nested JSON structure
        private string ExtractTechnicianName(Dictionary<string, object> req)
        {
            if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
            {
                if (techElem.TryGetProperty("name", out var nameProp) && !string.IsNullOrEmpty(nameProp.GetString()))
                    return nameProp.GetString();
            }
            return null;
        }

        // Helper: Extract created time from nested JSON structure
        private DateTimeOffset ExtractCreatedTime(Dictionary<string, object> req)
        {
            if (req.TryGetValue("created_time", out var ctObj) && ctObj is JsonElement ctElem && ctElem.ValueKind == JsonValueKind.Object)
            {
                if (ctElem.TryGetProperty("value", out var valElem) && long.TryParse(valElem.GetString(), out var ms))
                    return DateTimeOffset.FromUnixTimeMilliseconds(ms);
            }
            return DateTimeOffset.UtcNow;
        }
    }

    // Request Models
    public class QueryRequest
    {
        public string Query { get; set; }
    }

    public class QueryIntent
    {
        public string Type { get; set; }
        public Dictionary<string, object> Parameters { get; set; } = new Dictionary<string, object>();
    }

    // AI Response Models
    public class AiResponse
    {
        [JsonPropertyName("choices")]
        public List<AiChoice> Choices { get; set; }
    }

    public class AiChoice
    {
        [JsonPropertyName("message")]
        public AiMessage Message { get; set; }
    }

    public class AiMessage
    {
        [JsonPropertyName("content")]
        public string Content { get; set; }
    }
}