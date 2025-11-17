using Azure;
using Azure.AI.Agents.Persistent;
using Azure.Identity;
using documentchecker.Models;
using documentchecker.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Xml.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;
using Microsoft.EntityFrameworkCore; // Ensure this is included for EF Core extensions
using System.Dynamic; // For dynamic if needed, but we'll avoid it

namespace documentchecker.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MainController : ControllerBase
    {
        private readonly string _projectEndpoint;
        private readonly string _agentId;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IMemoryCache _cache;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly string _refreshToken;
        private readonly string _redirectUri;
        private readonly ChatService _chatService;
        private readonly QueryHistoryService _queryHistoryService;
        private readonly string _meAiEndpoint;
        private readonly string _meAiDeploymentName;
        private readonly string _meAiApiVersion;
        private readonly string _meAiApiKey;
        private readonly RequestStorageService _requestStorageService; // New service for storing requests
        private readonly AppDbContext _dbContext;

        public MainController(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            IMemoryCache cache,
            ChatService chatService,
            QueryHistoryService queryHistoryService,
            RequestStorageService requestStorageService,
            AppDbContext dbContext) // Inject the new service
        {
            _projectEndpoint = configuration["AzureAIFoundry:ProjectEndpoint"] ?? throw new InvalidOperationException("ProjectEndpoint not configured.");
            _agentId = configuration["AzureAIFoundry:AgentId"] ?? "asst_MqwY6PBQdS9uxha6Hl1RQCJk";
            _httpClientFactory = httpClientFactory;
            _cache = cache;
            _clientId = configuration["Zoho:ClientId"] ?? throw new InvalidOperationException("Zoho:ClientId not configured.");
            _clientSecret = configuration["Zoho:ClientSecret"] ?? throw new InvalidOperationException("Zoho:ClientSecret not configured.");
            _refreshToken = configuration["Zoho:RefreshToken"] ?? throw new InvalidOperationException("Zoho:RefreshToken not configured.");
            _redirectUri = configuration["Zoho:RedirectUri"] ?? throw new InvalidOperationException("Zoho:RedirectUri not configured.");
            _chatService = chatService;
            _queryHistoryService = queryHistoryService;
            // New AI configuration for ManageEngine query parsing
            _meAiEndpoint = configuration["ManageEngineAI:Endpoint"] ?? throw new InvalidOperationException("ManageEngineAI:Endpoint not configured.");
            _meAiDeploymentName = configuration["ManageEngineAI:DeploymentName"] ?? throw new InvalidOperationException("ManageEngineAI:DeploymentName not configured.");
            _meAiApiVersion = configuration["ManageEngineAI:ApiVersion"] ?? throw new InvalidOperationException("ManageEngineAI:ApiVersion not configured.");
            _meAiApiKey = configuration["ManageEngineAI:ApiKey"] ?? throw new InvalidOperationException("ManageEngineAI:ApiKey not configured.");
            _requestStorageService = requestStorageService;
            _dbContext = dbContext;
        }

        // Updated endpoint for natural language queries
        [HttpPost("ai-query")]
        public async Task<IActionResult> AiQuery([FromBody] AiQueryRequest request)
        {
            if (string.IsNullOrEmpty(request?.Query))
            {
                return BadRequest("Query is required.");
            }

            try
            {
                var queryType = await DetectQueryTypeFromAI(request.Query);

                // Route to specialized endpoints if detected
                if (queryType.Type == "top-request-areas" && queryType.Months.HasValue)
                {
                    return await GetTopRequestAreas(queryType.Months.Value);
                }

                if (queryType.Type == "inactive-technicians" && queryType.Months.HasValue)
                {
                    return await GetInactiveTechnicians(queryType.Months.Value);
                }

                // Fall back to standard filter-based query
                var filters = await GetFiltersFromAI(request.Query);
                Console.WriteLine($"Extracted filters: Subject={filters.Subject}, DateFrom={filters.DateFrom}, DateTo={filters.DateTo}, Technician={filters.Technician}");
                return await TestManageEngine(filters.Subject, filters.DateFrom, filters.DateTo, filters.Technician);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = $"AI query processing failed: {ex.Message}" });
            }
        }

        private async Task<QueryType> DetectQueryTypeFromAI(string userQuery)
        {
            var apiClient = _httpClientFactory.CreateClient();
            var fullUrl = $"{_meAiEndpoint}openai/deployments/{_meAiDeploymentName}/chat/completions?api-version={_meAiApiVersion}";

            var detectionPrompt = $@"Analyze this query and determine its type. Return ONLY a JSON object.
If the query is asking for:
1. Top/most common request areas/categories - return {{""type"": ""top-request-areas"", ""months"": <number>}}
2. Inactive technicians - return {{""type"": ""inactive-technicians"", ""months"": <number>}}
3. Otherwise - return {{""type"": ""standard"", ""months"": null}}

Extract the number of months from relative time expressions (e.g., 'last 3 months' = 3, 'past year' = 12, default = 1 if not specified).
Output ONLY the JSON object, no other text.

Examples:
Query: Show me the top request areas for the last 3 months
Output: {{""type"": ""top-request-areas"", ""months"": 3}}

Query: Which technicians have been inactive in the past 6 months?
Output: {{""type"": ""inactive-technicians"", ""months"": 6}}

Query: Show password reset tickets from October
Output: {{""type"": ""standard"", ""months"": null}}

User query: {userQuery}";

            var requestBody = new
            {
                messages = new[]
                {
            new { role = "user", content = detectionPrompt }
        },
                max_tokens = 100,
                temperature = 0.1
            };

            var json = JsonSerializer.Serialize(requestBody);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            apiClient.DefaultRequestHeaders.Add("api-key", _meAiApiKey);

            var response = await apiClient.PostAsync(fullUrl, content);
            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                throw new Exception($"Query type detection failed: {response.StatusCode} - {errorContent}");
            }

            var responseJson = await response.Content.ReadAsStringAsync();
            var aiResponse = JsonSerializer.Deserialize<AiResponse>(responseJson);

            if (aiResponse?.Choices == null || aiResponse.Choices.Count == 0)
            {
                return new QueryType { Type = "standard", Months = null };
            }

            var outputContent = aiResponse.Choices[0].Message.Content;
            Console.WriteLine($"Query type detection output: {outputContent}");

            var queryType = JsonSerializer.Deserialize<QueryType>(outputContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? new QueryType { Type = "standard", Months = null };

            return queryType;
        }

        private async Task<Filters> GetFiltersFromAI(string userQuery)
        {
            var apiClient = _httpClientFactory.CreateClient();
            var fullUrl = $"{_meAiEndpoint}openai/deployments/{_meAiDeploymentName}/chat/completions?api-version={_meAiApiVersion}";

            string currentDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
            var systemPrompt = $@"You are an AI that converts natural language queries into structured filters for querying ManageEngine ServiceDesk requests. 
The current date is {currentDate}. Use this to calculate absolute dates for any relative time expressions (e.g., 'last week' means 7 days ago, 'past month' means 30 days ago, 'this year' means from January 1 of this year).
The filters are: 
- subject: string to search in subject using 'contains' condition. Extract key phrases or topics from the query.
- technician: string to search in technician name using 'contains' condition. Extract names or parts of names mentioned.
- dateFrom: yyyy-MM-dd, start of range for created_time. Infer if a start date or range is implied.
- dateTo: yyyy-MM-dd, end of range for created_time. Infer if an end date is implied, default to today if open-ended.
Set to null only if absolutely no information can be inferred for that filter.
Output only a JSON object with keys 'subject', 'technician', 'dateFrom', 'dateTo'. Do not include any other text or explanations.

Examples:
User query: show requests about password reset
Output: {{""subject"": ""password reset"", ""technician"": null, ""dateFrom"": null, ""dateTo"": null}}

User query: tickets handled by John in the last month
Output: {{""subject"": null, ""technician"": ""John"", ""dateFrom"": ""{DateTime.Parse(currentDate).AddMonths(-1).ToString("yyyy-MM-dd")}"", ""dateTo"": ""{currentDate}""}}

User query: issues from October 1, 2025 to November 1, 2025
Output: {{""subject"": ""issues"", ""technician"": null, ""dateFrom"": ""2025-10-01"", ""dateTo"": ""2025-11-01""}}

User query: tickets handled by John in the last month, with the mention of software
Output: {{""subject"": ""software"", ""technician"": ""John"", ""dateFrom"": ""{DateTime.Parse(currentDate).AddMonths(-1).ToString("yyyy-MM-dd")}"", ""dateTo"": ""{currentDate}""}}";

            var userPrompt = $"User query: {userQuery}";

            var requestBody = new
            {
                messages = new[]
                {
            new { role = "system", content = systemPrompt },
            new { role = "user", content = userPrompt }
        },
                max_tokens = 200,
                temperature = 0.2
            };

            var json = JsonSerializer.Serialize(requestBody);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            apiClient.DefaultRequestHeaders.Add("api-key", _meAiApiKey);

            var response = await apiClient.PostAsync(fullUrl, content);
            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                throw new Exception($"AI API request failed: {response.StatusCode} - {errorContent}");
            }

            var responseJson = await response.Content.ReadAsStringAsync();
            var aiResponse = JsonSerializer.Deserialize<AiResponse>(responseJson);

            if (aiResponse?.Choices == null || aiResponse.Choices.Count == 0)
            {
                throw new Exception("Invalid AI response format.");
            }

            var outputContent = aiResponse.Choices[0].Message.Content;
            Console.WriteLine($"AI output: {outputContent}");

            var filters = JsonSerializer.Deserialize<Filters>(outputContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? throw new Exception("Failed to parse filters from AI response.");

            return filters;
        }

        // Helper class for query type detection
        public class QueryType
        {
            public string Type { get; set; }
            public int? Months { get; set; }
        }

        [HttpGet("test-manageengine")]
        public async Task<IActionResult> TestManageEngine(
            [FromQuery] string? subject = null,
            [FromQuery] string? dateFrom = null,
            [FromQuery] string? dateTo = null,
            [FromQuery] string? technician = null)
        {
            var logger = _httpClientFactory.CreateClient("LoggingClient");
            var apiClient = _httpClientFactory.CreateClient();
            apiClient.Timeout = TimeSpan.FromSeconds(60); // 60 seconds per request
            int pageNumber = 1;
            try
            {
                string accessToken = await GetAccessTokenAsync();
                using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
                await LogDetails(logger, "Start", new { Timestamp = DateTime.UtcNow, Details = "Fetching requests from ManageEngine API" });
                apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                    "Zoho-oauthtoken", accessToken);
                var allRequests = new List<Dictionary<string, object>>();
                var seenIds = new HashSet<string>();
                int totalUnfiltered = 0;
                bool hasMoreRows = true;
                int consecutiveEmptyPages = 0;
                const int emptyPageThreshold = 5;
                const int rowCount = 100;
                const int maxTotalRecords = 500;
                long? dateFromMs = null, dateToMs = null;
                if (!string.IsNullOrEmpty(dateFrom) || !string.IsNullOrEmpty(dateTo))
                {
                    DateTime fromDate;
                    if (DateTime.TryParse(dateFrom, out fromDate))
                        dateFromMs = new DateTimeOffset(fromDate, TimeSpan.Zero).ToUnixTimeMilliseconds();
                    DateTime toDate;
                    if (DateTime.TryParse(dateTo, out toDate))
                        dateToMs = new DateTimeOffset(toDate, TimeSpan.Zero).ToUnixTimeMilliseconds();
                    if (dateFromMs == null && dateToMs != null || dateFromMs != null && dateToMs == null)
                        return BadRequest("Both dateFrom and dateTo must be provided for date range filtering.");
                }
                var searchCriteriaList = new List<Dictionary<string, object>>();
                if (!string.IsNullOrEmpty(subject))
                {
                    searchCriteriaList.Add(new Dictionary<string, object>
                    {
                        ["field"] = "subject",
                        ["condition"] = "contains",
                        ["values"] = new[] { subject }
                    });
                }
                if (!string.IsNullOrEmpty(technician))
                {
                    searchCriteriaList.Add(new Dictionary<string, object>
                    {
                        ["field"] = "technician.name",
                        ["condition"] = "contains",
                        ["values"] = new[] { technician }
                    });
                }
                if (dateFromMs != null && dateToMs != null)
                {
                    searchCriteriaList.Add(new Dictionary<string, object>
                    {
                        ["field"] = "created_time",
                        ["condition"] = "between",
                        ["values"] = new[] { dateFromMs.ToString(), dateToMs.ToString() }
                    });
                }
                object? searchCriteria = null;
                if (searchCriteriaList.Count > 0)
                {
                    if (searchCriteriaList.Count == 1)
                    {
                        searchCriteria = searchCriteriaList[0];
                    }
                    else
                    {
                        for (int i = 1; i < searchCriteriaList.Count; i++)
                        {
                            searchCriteriaList[i]["logical_operator"] = "and";
                        }
                        searchCriteria = searchCriteriaList;
                    }
                }
                while (hasMoreRows && pageNumber <= 10 && (maxTotalRecords == 0 || totalUnfiltered < maxTotalRecords))
                {
                    var startTime = DateTime.UtcNow;
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
                            _cache.Remove("ZohoTokenExpiration");
                            accessToken = await GetAccessTokenAsync();
                            apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                                "Zoho-oauthtoken", accessToken);
                            response = await apiClient.GetAsync(url, cts.Token);
                        }
                        if (!response.IsSuccessStatusCode)
                        {
                            var errorDetails = new { StatusCode = (int)response.StatusCode, Reason = response.ReasonPhrase, Page = pageNumber };
                            await LogDetails(logger, "Failure", errorDetails);
                            var reducedRowCount = Math.Min(rowCount / 2, 50);
                            listInfo = new
                            {
                                row_count = reducedRowCount,
                                start_index = (pageNumber - 1) * rowCount,
                                sort_field = "created_time",
                                sort_order = "desc",
                                get_total_count = true,
                                search_criteria = searchCriteria
                            };
                            inputData = new { list_info = listInfo };
                            inputDataJson = JsonSerializer.Serialize(inputData, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
                            encodedInputData = HttpUtility.UrlEncode(inputDataJson);
                            url = $"https://sdpondemand.manageengine.com/api/v3/requests?input_data={encodedInputData}";
                            response = await apiClient.GetAsync(url, cts.Token);
                            if (!response.IsSuccessStatusCode)
                            {
                                url = "https://sdpondemand.manageengine.com/api/v3/requests";
                                response = await apiClient.GetAsync(url, cts.Token);
                                if (!response.IsSuccessStatusCode)
                                    return StatusCode((int)response.StatusCode, new { Error = $"API request failed on page {pageNumber}: {response.ReasonPhrase}" });
                            }
                        }
                    }
                    string jsonResponse = await response.Content.ReadAsStringAsync(cts.Token);
                    var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse) ?? new Dictionary<string, object>();
                    var requestsElem = data.ContainsKey("requests") ? (JsonElement)data["requests"] : JsonDocument.Parse("[]").RootElement;
                    var currentRequests = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(requestsElem.GetRawText(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new List<Dictionary<string, object>>();
                    int rowsThisPage = 0;
                    foreach (var req in currentRequests)
                    {
                        if (req.TryGetValue("id", out var idObj) && seenIds.Add(idObj.ToString()))
                        {
                            allRequests.Add(req);
                            totalUnfiltered++;
                            rowsThisPage++;
                        }
                    }
                    hasMoreRows = false;
                    if (data.TryGetValue("list_info", out var listInfoObj) && listInfoObj is JsonElement listInfoElem)
                    {
                        if (listInfoElem.TryGetProperty("has_more_rows", out var hasMoreProp))
                        {
                            hasMoreRows = hasMoreProp.GetBoolean();
                        }
                    }
                    if (rowsThisPage == 0) consecutiveEmptyPages++;
                    else consecutiveEmptyPages = 0;
                    var elapsedMs = (int)(DateTime.UtcNow - startTime).TotalMilliseconds;
                    await LogDetails(logger, "PageFetched", new { PageNumber = pageNumber, RowsThisPage = rowsThisPage, HasMoreRows = hasMoreRows, TotalUnfilteredSoFar = totalUnfiltered, ElapsedMs = elapsedMs });
                    pageNumber++;
                    if (hasMoreRows && consecutiveEmptyPages < emptyPageThreshold) await Task.Delay(50, cts.Token);
                    else if (consecutiveEmptyPages >= emptyPageThreshold) hasMoreRows = false;
                }
                var filteredRequests = allRequests;
                var resultDetails = new
                {
                    TotalPages = pageNumber - 1,
                    TotalUnfilteredRows = totalUnfiltered,
                    FilteredRowsCount = filteredRequests.Count,
                    Timestamp = DateTime.UtcNow
                };
                await LogDetails(logger, "Success", resultDetails);
                // Log to DB
                var queryLog = new ManageEngineQuery
                {
                    Subject = subject,
                    DateFrom = dateFrom,
                    DateTo = dateTo,
                    Technician = technician,
                    TotalResults = totalUnfiltered,
                    FilteredResults = filteredRequests.Count,
                    CacheKey = $"me-query-{subject}-{dateFrom}-{dateTo}-{technician}"
                };
                await _queryHistoryService.LogQueryAsync(queryLog);
                return Ok(new { FilteredRequests = filteredRequests, Summary = resultDetails });
            }
            catch (TaskCanceledException ex)
            {
                await LogDetails(logger, "Timeout", new { Details = "Operation timed out after 15 minutes", Page = pageNumber, Timestamp = DateTime.UtcNow });
                return StatusCode(408, new { Error = $"Operation timed out on page {pageNumber}. Check token or API limits." });
            }
            catch (Exception ex)
            {
                await LogDetails(logger, "Error", new { ExceptionMessage = ex.Message, StackTrace = ex.StackTrace?.Substring(0, 500), Page = pageNumber, Timestamp = DateTime.UtcNow });
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        [HttpGet("top-request-areas/{months}")]
        public async Task<IActionResult> GetTopRequestAreas(int months)
        {
            if (months < 1 || months > 3) return BadRequest("Months must be 1, 2, or 3.");
            var cacheKey = $"top-areas-{months}";
            if (_cache.TryGetValue(cacheKey, out object? cachedResult))
            {
                return Ok(cachedResult);
            }
            try
            {
                var dateTo = DateTimeOffset.UtcNow;
                var temp = dateTo.AddMonths(-months);
                var dateFrom = new DateTimeOffset(temp.Year, temp.Month, temp.Day, 0, 0, 0, TimeSpan.Zero);
                var requests = await FetchRequestsForDateRange(dateFrom, dateTo);
                var topAreas = requests
                    .Where(req => req.TryGetValue("subject", out var subjObj) && subjObj != null && !string.IsNullOrEmpty(subjObj.ToString()))
                    .GroupBy(req => req["subject"].ToString().ToLowerInvariant())
                    .Select(g => new { Subject = g.Key, Count = g.Count() })
                    .OrderByDescending(x => x.Count)
                    .Take(10)
                    .ToList();
                var result = new { TopAreas = topAreas, Period = $"{months} month(s)", Timestamp = DateTime.UtcNow };
                _cache.Set(cacheKey, result, TimeSpan.FromDays(10));
                // Log query to DB
                var queryLog = new ManageEngineQuery
                {
                    QueryId = Guid.NewGuid().ToString(),
                    DateFrom = dateFrom.ToString("yyyy-MM-dd"),
                    DateTo = dateTo.ToString("yyyy-MM-dd"),
                    TotalResults = requests.Count,
                    FilteredResults = topAreas.Count,
                    ExecutedAt = DateTime.UtcNow,
                    ExecutedBy = "TopAreas",
                    CacheKey = $"top-areas-{months}"
                };
                await _queryHistoryService.LogQueryAsync(queryLog);
                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        [HttpGet("inactive-technicians/{months}")]
        public async Task<IActionResult> GetInactiveTechnicians(int months)
        {
            if (months < 1 || months > 3) return BadRequest("Months must be 1, 2, or 3.");
            var cacheKey = $"inactive-techs-{months}";
            if (_cache.TryGetValue(cacheKey, out object? cachedResult))
            {
                return Ok(cachedResult);
            }
            try
            {
                var allTechnicians = await FetchAllTechnicians();
                var allTechNames = allTechnicians
                    .Where(t => t.TryGetValue("name", out var nameObj) && nameObj != null && !string.IsNullOrEmpty(nameObj.ToString()))
                    .Select(t => t["name"].ToString().ToLowerInvariant())
                    .ToHashSet();
                var dateTo = DateTimeOffset.UtcNow;
                var temp = dateTo.AddMonths(-months);
                var dateFrom = new DateTimeOffset(temp.Year, temp.Month, temp.Day, 0, 0, 0, TimeSpan.Zero);
                var requests = await FetchRequestsForDateRange(dateFrom, dateTo);
                var assignedTechs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var req in requests)
                {
                    if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
                    {
                        if (techElem.TryGetProperty("name", out var nameProp) && nameProp.ValueKind != JsonValueKind.Null && !string.IsNullOrEmpty(nameProp.GetString()))
                        {
                            assignedTechs.Add(nameProp.GetString().ToLowerInvariant());
                        }
                    }
                }
                var inactive = allTechNames.Except(assignedTechs).Select(name => new { Name = name }).ToList();
                var result = new { InactiveTechnicians = inactive, Period = $"{months} month(s)", TotalTechnicians = allTechNames.Count, Timestamp = DateTime.UtcNow };
                _cache.Set(cacheKey, result, TimeSpan.FromDays(10));
                // Log query to DB
                var queryLog = new ManageEngineQuery
                {
                    QueryId = Guid.NewGuid().ToString(),
                    DateFrom = dateFrom.ToString("yyyy-MM-dd"),
                    DateTo = dateTo.ToString("yyyy-MM-dd"),
                    TotalResults = allTechnicians.Count,
                    FilteredResults = inactive.Count,
                    ExecutedAt = DateTime.UtcNow,
                    ExecutedBy = "InactiveTechs",
                    CacheKey = $"inactive-techs-{months}"
                };
                await _queryHistoryService.LogQueryAsync(queryLog);
                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        [HttpGet("query-history")]
        public async Task<IActionResult> GetQueryHistory([FromQuery] int? top = 50)
        {
            try
            {
                var queries = await _queryHistoryService.GetRecentQueries(top ?? 50);
                return Ok(new
                {
                    Queries = queries.Select(q => new
                    {
                        q.QueryId,
                        q.Subject,
                        q.DateFrom,
                        q.DateTo,
                        q.Technician,
                        q.TotalResults,
                        q.FilteredResults,
                        q.ExecutedAt,
                        q.ExecutedBy,
                        q.CacheKey
                    }),
                    TotalCount = queries.Count
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // New endpoint to test maximum rows per page (assuming API max is 100, but can test higher if needed)
        [HttpGet("max-rows-test")]
        public async Task<IActionResult> TestMaxRows([FromQuery] int testRowCount = 100)
        {
            try
            {
                var requests = await FetchRequests(testRowCount, 1, "created_time", "desc");
                return Ok(new { RowsFetched = requests.Count, TestRowCount = testRowCount });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        [HttpGet("oldest-date")]
        public async Task<IActionResult> GetOldestDate()
        {
            try
            {
                var requests = await FetchRequests(10, 1, "created_time", "asc"); // Fetch 10 oldest
                if (requests.Count == 0)
                {
                    return NotFound("No records found.");
                }

                var oldestRequest = requests.OrderBy(r => {
                    if (r.TryGetValue("created_time", out var ctObj) && ctObj is JsonElement ctElem && ctElem.ValueKind == JsonValueKind.Object)
                    {
                        if (ctElem.TryGetProperty("value", out var valElem) && valElem.ValueKind == JsonValueKind.String && long.TryParse(valElem.GetString(), out var ms))
                        {
                            return ms;
                        }
                    }
                    return long.MaxValue; // Fallback if missing
                }).FirstOrDefault();

                if (oldestRequest == null || !oldestRequest.TryGetValue("created_time", out var createdTimeObj) || createdTimeObj is not JsonElement createdTimeElem || createdTimeElem.ValueKind != JsonValueKind.Object || !createdTimeElem.TryGetProperty("value", out var valElem) || valElem.ValueKind != JsonValueKind.String)
                {
                    return BadRequest("Invalid created_time format in oldest request.");
                }

                if (!long.TryParse(valElem.GetString(), out var oldestDateMs))
                {
                    return BadRequest("Invalid value in created_time.");
                }
                var oldestDate = DateTimeOffset.FromUnixTimeMilliseconds(oldestDateMs).UtcDateTime;

                return Ok(new { OldestDate = oldestDate.ToString("yyyy-MM-dd") });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // Updated MigrateHistoricalData method in MainController
        [HttpPost("migrate-historical")]
        public async Task<IActionResult> MigrateHistoricalData()
        {
            try
            {
                // Get the last stored date from DB (if any)
                var lastStoredDate = await _requestStorageService.GetLastStoredDateAsync();

                DateTime startFromDate;
                if (lastStoredDate.HasValue)
                {
                    // Resume from the day after the last stored date
                    startFromDate = lastStoredDate.Value.UtcDateTime.AddDays(1);
                    Console.WriteLine($"Resuming migration from {startFromDate:yyyy-MM-dd}");
                }
                else
                {
                    // No records: Start from Jan 1, 2025
                    startFromDate = new DateTime(2025, 1, 1);
                    Console.WriteLine("Starting new migration from Jan 1, 2025");
                }

                var currentDate = DateTime.UtcNow;

                if (startFromDate > currentDate)
                {
                    return Ok(new { Message = "Migration already up to date." });
                }

                // Loop month by month from startFromDate onward
                var startDate = startFromDate;
                while (startDate < currentDate)
                {
                    var endDate = startDate.AddMonths(1).AddDays(-1); // End of month
                    if (endDate > currentDate) endDate = currentDate;

                    var dateFromMs = new DateTimeOffset(startDate).ToUnixTimeMilliseconds();
                    var dateToMs = new DateTimeOffset(endDate).ToUnixTimeMilliseconds();

                    // Log the current batch for monitoring
                    Console.WriteLine($"Processing batch: {startDate:yyyy-MM-dd} to {endDate:yyyy-MM-dd}");

                    var requests = await FetchRequestsForDateRange(new DateTimeOffset(startDate), new DateTimeOffset(endDate));
                    foreach (var req in requests)
                    {
                        var requestId = req["id"].ToString();
                        if (!await _requestStorageService.RequestExistsAsync(requestId))
                        {
                            await _requestStorageService.StoreRequestAsync(req);
                        }
                    }

                    startDate = endDate.AddDays(1); // Next month
                }

                return Ok(new { Message = "Historical migration completed or updated." });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // New endpoint to fetch and store new records (from last stored date to now)
        [HttpPost("update-recent")]
        public async Task<IActionResult> UpdateRecentData()
        {
            try
            {
                var lastStoredDate = await _requestStorageService.GetLastStoredDateAsync();
                if (!lastStoredDate.HasValue)
                {
                    return BadRequest("No stored data found. Run historical migration first.");
                }

                var dateFrom = lastStoredDate.Value.AddDays(1); // Start from day after last
                var dateTo = DateTimeOffset.UtcNow;

                var requests = await FetchRequestsForDateRange(dateFrom, dateTo);
                foreach (var req in requests)
                {
                    var requestId = req["id"].ToString();
                    if (!await _requestStorageService.RequestExistsAsync(requestId))
                    {
                        await _requestStorageService.StoreRequestAsync(req);
                    }
                }

                return Ok(new { Message = "Recent data updated.", NewRecords = requests.Count });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        [HttpGet("distinct-technicians")]
        public async Task<IActionResult> GetDistinctTechnicians()
        {
            try
            {
                var technicians = await _dbContext.ManageEngineRequests
                    .Where(r => !string.IsNullOrEmpty(r.TechnicianName))
                    .Select(r => r.TechnicianName)
                    .Distinct()
                    .ToListAsync();

                return Ok(technicians);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // 2. Inactive technicians over a period (custom dateFrom and dateTo)
        [HttpGet("inactive-technicians")]
        public async Task<IActionResult> GetInactiveTechnicians([FromQuery] DateTime dateFrom, [FromQuery] DateTime dateTo)
        {
            try
            {
                // Get all distinct technicians (ever)
                var allTechnicians = await _dbContext.ManageEngineRequests
                    .Where(r => !string.IsNullOrEmpty(r.TechnicianName))
                    .Select(r => r.TechnicianName)
                    .Distinct()
                    .ToListAsync();

                // Get active technicians in the date range
                var activeTechnicians = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.TechnicianName))
                    .Select(r => r.TechnicianName)
                    .Distinct()
                    .ToListAsync();

                // Inactive = all - active
                var inactive = allTechnicians.Except(activeTechnicians).ToList();

                return Ok(new { InactiveTechnicians = inactive, Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}" });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // 3. Influx of requests per hour in a date range (counts per hour)
        [HttpGet("requests-per-hour")]
        public async Task<IActionResult> GetRequestsPerHour([FromQuery] DateTime dateFrom, [FromQuery] DateTime dateTo)
        {
            try
            {
                var hourlyCounts = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo)
                    .GroupBy(r => new { r.CreatedTime.Date, r.CreatedTime.Hour })
                    .Select(g => new { Date = g.Key.Date, Hour = g.Key.Hour, Count = g.Count() })
                    .OrderBy(x => x.Date).ThenBy(x => x.Hour)
                    .ToListAsync();

                return Ok(hourlyCounts);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // 4. Top request areas (subjects) in a date range
        [HttpGet("top-request-areas")]
        public async Task<IActionResult> GetTopRequestAreas([FromQuery] DateTime dateFrom, [FromQuery] DateTime dateTo, [FromQuery] int topN = 10)
        {
            try
            {
                var topAreas = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.Subject))
                    .GroupBy(r => r.Subject)
                    .Select(g => new { Subject = g.Key, Count = g.Count() })
                    .OrderByDescending(x => x.Count)
                    .Take(topN)
                    .ToListAsync();

                return Ok(new { TopAreas = topAreas, Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}" });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        private async Task<List<Dictionary<string, object>>> FetchRequestsForDateRange(DateTimeOffset dateFrom, DateTimeOffset dateTo)
        {
            var apiClient = _httpClientFactory.CreateClient();
            apiClient.Timeout = TimeSpan.FromSeconds(60);
            string accessToken = await GetAccessTokenAsync();
            apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                "Zoho-oauthtoken", accessToken);
            long dateFromMs = dateFrom.ToUnixTimeMilliseconds();
            long dateToMs = dateTo.ToUnixTimeMilliseconds();
            var searchCriteriaList = new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>
                {
                    ["field"] = "created_time",
                    ["condition"] = "between",
                    ["values"] = new[] { dateFromMs.ToString(), dateToMs.ToString() }
                }
            };
            object searchCriteria = searchCriteriaList;
            var allRequests = new List<Dictionary<string, object>>();
            var seenIds = new HashSet<string>();
            bool hasMoreRows = true;
            int pageNumber = 1;
            const int rowCount = 25;
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
            while (hasMoreRows)
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
                        _cache.Remove("ZohoTokenExpiration");
                        accessToken = await GetAccessTokenAsync();
                        apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                            "Zoho-oauthtoken", accessToken);
                        response = await apiClient.GetAsync(url, cts.Token);
                    }
                    if (!response.IsSuccessStatusCode)
                    {
                        url = "https://sdpondemand.manageengine.com/api/v3/requests";
                        response = await apiClient.GetAsync(url, cts.Token);
                        if (!response.IsSuccessStatusCode)
                        {
                            throw new Exception($"Failed to fetch requests: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                        }
                    }
                }
                string jsonResponse = await response.Content.ReadAsStringAsync(cts.Token);
                var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse) ?? new Dictionary<string, object>();
                var requestsElem = data.ContainsKey("requests") ? (JsonElement)data["requests"] : JsonDocument.Parse("[]").RootElement;
                var currentRequests = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(requestsElem.GetRawText(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new List<Dictionary<string, object>>();
                foreach (var req in currentRequests)
                {
                    if (req.TryGetValue("id", out var idObj) && seenIds.Add(idObj.ToString()))
                    {
                        allRequests.Add(req);
                    }
                }
                hasMoreRows = false;
                if (data.TryGetValue("list_info", out var listInfoObj) && listInfoObj is JsonElement listInfoElem)
                {
                    if (listInfoElem.TryGetProperty("has_more_rows", out var hasMoreProp))
                    {
                        hasMoreRows = hasMoreProp.GetBoolean();
                    }
                }
                pageNumber++;
            }
            return allRequests;
        }

        private async Task<List<Dictionary<string, object>>> FetchAllTechnicians()
        {
            var cacheKey = "all-technicians";
            if (_cache.TryGetValue(cacheKey, out List<Dictionary<string, object>> cachedTechs))
            {
                return cachedTechs;
            }
            var dateTo = DateTimeOffset.UtcNow;
            var dateFrom = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var requests = await FetchRequestsForDateRange(dateFrom, dateTo);
            var allTechnicians = new List<Dictionary<string, object>>();
            var seenIds = new HashSet<string>();
            foreach (var req in requests)
            {
                if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
                {
                    var techDict = JsonSerializer.Deserialize<Dictionary<string, object>>(techElem.GetRawText());
                    if (techDict.TryGetValue("id", out var idObj) && seenIds.Add(idObj.ToString()))
                    {
                        allTechnicians.Add(techDict);
                    }
                }
            }
            _cache.Set(cacheKey, allTechnicians, TimeSpan.FromDays(30));
            return allTechnicians;
        }

        private async Task<string> GetAccessTokenAsync()
        {
            const string tokenCacheKey = "ZohoAccessToken";
            const string expirationCacheKey = "ZohoTokenExpiration";
            if (_cache.TryGetValue(tokenCacheKey, out string cachedToken) &&
                _cache.TryGetValue(expirationCacheKey, out DateTime cachedExpiration) &&
                DateTime.UtcNow < cachedExpiration)
            {
                return cachedToken;
            }
            try
            {
                var client = _httpClientFactory.CreateClient();
                var formContent = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("refresh_token", _refreshToken),
                    new KeyValuePair<string, string>("grant_type", "refresh_token"),
                    new KeyValuePair<string, string>("client_id", _clientId),
                    new KeyValuePair<string, string>("client_secret", _clientSecret),
                    new KeyValuePair<string, string>("redirect_uri", _redirectUri)
                });
                var response = await client.PostAsync("https://accounts.zoho.com/oauth/v2/token", formContent);
                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception($"Failed to refresh Zoho access token: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                }
                string jsonResponse = await response.Content.ReadAsStringAsync();
                var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse) ?? throw new Exception("Invalid response from Zoho token endpoint.");
                if (data.TryGetValue("error", out var errorObj))
                {
                    throw new Exception($"Zoho token error: {errorObj}");
                }
                if (!data.TryGetValue("access_token", out var accessTokenObj) || accessTokenObj == null)
                {
                    throw new Exception("Access token not found in Zoho response.");
                }
                string accessToken = accessTokenObj.ToString()!;
                int expiresIn = 3600;
                if (data.TryGetValue("expires_in", out var expiresInObj) && int.TryParse(expiresInObj.ToString(), out int parsedExpiresIn))
                {
                    expiresIn = parsedExpiresIn;
                }
                var expiration = DateTime.UtcNow.AddSeconds(expiresIn - 60);
                _cache.Set(tokenCacheKey, accessToken, new MemoryCacheEntryOptions { AbsoluteExpiration = expiration });
                _cache.Set(expirationCacheKey, expiration, new MemoryCacheEntryOptions { AbsoluteExpiration = expiration });
                return accessToken;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error refreshing Zoho token: {ex.Message}");
                throw;
            }
        }

        private async Task LogDetails(HttpClient logger, string eventType, object details)
        {
            try
            {
                var logEntry = new { EventType = eventType, Details = details, Timestamp = DateTime.UtcNow };
                var json = JsonSerializer.Serialize(logEntry);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                await logger.PostAsync("https://webhook.site/29d2999b-8b11-4560-aa10-4fd164e18b8e", content);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Logging failed: {ex.Message}");
            }
        }

        private async Task<List<Dictionary<string, object>>> FetchRequests(int rowCount, int startIndex, string sortField, string sortOrder, object? searchCriteria = null)
        {
            var apiClient = _httpClientFactory.CreateClient();
            apiClient.Timeout = TimeSpan.FromSeconds(60);
            string accessToken = await GetAccessTokenAsync();
            apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Zoho-oauthtoken", accessToken);

            var listInfo = new
            {
                row_count = rowCount,
                start_index = startIndex,
                sort_field = sortField,
                sort_order = sortOrder,
                get_total_count = true,
                search_criteria = searchCriteria
            };
            var inputData = new { list_info = listInfo };
            var inputDataJson = JsonSerializer.Serialize(inputData, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
            var encodedInputData = HttpUtility.UrlEncode(inputDataJson);
            string url = $"https://sdpondemand.manageengine.com/api/v3/requests?input_data={encodedInputData}";

            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            var response = await apiClient.GetAsync(url, cts.Token);
            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Failed to fetch requests: {response.StatusCode}");
            }

            string jsonResponse = await response.Content.ReadAsStringAsync(cts.Token);
            var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse) ?? new Dictionary<string, object>();
            var requestsElem = data.ContainsKey("requests") ? (JsonElement)data["requests"] : JsonDocument.Parse("[]").RootElement;
            return JsonSerializer.Deserialize<List<Dictionary<string, object>>>(requestsElem.GetRawText(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new List<Dictionary<string, object>>();
        }
    }

    // Helper classes
    public class AiQueryRequest
    {
        public string Query { get; set; }
    }

    public record Filters(string? Subject, string? Technician, string? DateFrom, string? DateTo);

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