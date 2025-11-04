using Azure;
using Azure.AI.Agents.Persistent;
using Azure.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web; // For URL encoding
using System.Text.Json.Nodes;

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

        public MainController(IConfiguration configuration, IHttpClientFactory httpClientFactory, IMemoryCache cache)
        {
            _projectEndpoint = configuration["AzureAIFoundry:ProjectEndpoint"] ?? throw new InvalidOperationException("ProjectEndpoint not configured.");
            _agentId = configuration["AzureAIFoundry:AgentId"] ?? "asst_MqwY6PBQdS9uxha6Hl1RQCJk";
            _httpClientFactory = httpClientFactory;
            _cache = cache;
            _clientId = configuration["Zoho:ClientId"] ?? throw new InvalidOperationException("Zoho:ClientId not configured.");
            _clientSecret = configuration["Zoho:ClientSecret"] ?? throw new InvalidOperationException("Zoho:ClientSecret not configured.");
            _refreshToken = configuration["Zoho:RefreshToken"] ?? throw new InvalidOperationException("Zoho:RefreshToken not configured.");
            _redirectUri = configuration["Zoho:RedirectUri"] ?? throw new InvalidOperationException("Zoho:RedirectUri not configured.");
        }

        [HttpPost("agent-chat")]
        public async Task<IActionResult> AgentChat([FromBody] AgentChatRequest request)
        {
            if (string.IsNullOrEmpty(request.Message))
            {
                return BadRequest("Message is required.");
            }

            try
            {
                var client = new PersistentAgentsClient(_projectEndpoint, new DefaultAzureCredential());

                PersistentAgent agent = await client.Administration.GetAgentAsync(_agentId);
                PersistentAgentThread thread = await client.Threads.CreateThreadAsync();

                await client.Messages.CreateMessageAsync(thread.Id, MessageRole.User, request.Message);

                ThreadRun run = await client.Runs.CreateRunAsync(
                    thread.Id,
                    _agentId,
                    additionalInstructions: request.AdditionalInstructions
                );

                do
                {
                    await Task.Delay(500);
                    run = await client.Runs.GetRunAsync(thread.Id, run.Id);
                }
                while (run.Status == RunStatus.Queued || run.Status == RunStatus.InProgress || run.Status == RunStatus.RequiresAction);

                if (run.Status == RunStatus.Failed)
                {
                    return StatusCode(500, new { Error = $"Run failed: {run.LastError?.Message}" });
                }

                var messagesAsync = client.Messages.GetMessagesAsync(thread.Id, order: ListSortOrder.Ascending);
                var agentResponses = new List<string>();
                await foreach (var message in messagesAsync)
                {
                    if (message.Role == MessageRole.Agent)
                    {
                        foreach (var content in message.ContentItems)
                        {
                            if (content is MessageTextContent textContent)
                            {
                                agentResponses.Add(textContent.Text);
                            }
                        }
                    }
                }

                return Ok(new { Responses = agentResponses });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
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
                return Ok(result);
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
            const int rowCount = 100;

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
    }

    public class AgentChatRequest
    {
        public string Message { get; set; } = string.Empty;
        public string? AdditionalInstructions { get; set; }
    }
}