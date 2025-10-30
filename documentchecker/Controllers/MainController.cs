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

namespace documentchecker.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MainController : ControllerBase
    {
        private readonly string _projectEndpoint;
        private readonly string _agentId;
        private readonly string _zohoToken;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IMemoryCache _cache;

        public MainController(IConfiguration configuration, IHttpClientFactory httpClientFactory, IMemoryCache cache)
        {
            _projectEndpoint = configuration["AzureAIFoundry:ProjectEndpoint"] ?? throw new InvalidOperationException("ProjectEndpoint not configured.");
            _agentId = configuration["AzureAIFoundry:AgentId"] ?? "asst_MqwY6PBQdS9uxha6Hl1RQCJk";
            _zohoToken = configuration["ZohoOAuthToken"] ?? throw new InvalidOperationException("ZohoOAuthToken not configured.");
            _httpClientFactory = httpClientFactory;
            _cache = cache;
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

            int pageNumber = 1; // Declare here for scope across try/catch
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15)); // 15-minute total timeout
                await LogDetails(logger, "Start", new { Timestamp = DateTime.UtcNow, Details = "Fetching requests from ManageEngine API" });

                apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                    "Zoho-oauthtoken", _zohoToken);

                // Use List for ordered collection (newest-first) + HashSet for fast dedup by ID
                var allRequests = new List<Dictionary<string, object>>();
                var seenIds = new HashSet<string>();
                int totalRows = 0;
                bool hasMoreRows = true;
                int consecutiveEmptyPages = 0;
                const int emptyPageThreshold = 5;
                const int rowCount = 100; // Max allowed per page
                // OPTIONAL: Add max total records limit to stop early (e.g., only fetch 500 latest)
                const int maxTotalRecords = 0; // Set to >0 for limit

                long? dateFromMs = null, dateToMs = null;
                if (!string.IsNullOrEmpty(dateFrom) || !string.IsNullOrEmpty(dateTo))
                {
                    if (DateTime.TryParse(dateFrom, out var fromDate))
                        dateFromMs = new DateTimeOffset(fromDate).ToUnixTimeMilliseconds();
                    if (DateTime.TryParse(dateTo, out var toDate))
                        dateToMs = new DateTimeOffset(toDate).ToUnixTimeMilliseconds();
                    if (dateFromMs == null && dateToMs != null || dateFromMs != null && dateToMs == null)
                        return BadRequest("Both dateFrom and dateTo must be provided for date range filtering.");
                }

                // Build search_criteria dynamically based on filters (AND logic by default)
                var searchCriteriaList = new List<object>();
                if (!string.IsNullOrEmpty(subject))
                {
                    searchCriteriaList.Add(new
                    {
                        field = "subject",
                        condition = "contains",
                        values = new[] { subject }
                    });
                }
                if (!string.IsNullOrEmpty(technician))
                {
                    searchCriteriaList.Add(new
                    {
                        field = "technician.name",
                        condition = "contains",
                        values = new[] { technician }
                    });
                }
                if (dateFromMs != null && dateToMs != null)
                {
                    searchCriteriaList.Add(new
                    {
                        field = "created_time",
                        condition = "between",
                        values = new[] { dateFromMs.ToString(), dateToMs.ToString() }
                    });
                }
                object? searchCriteria = null;
                if (searchCriteriaList.Count > 0)
                {
                    searchCriteria = searchCriteriaList.Count == 1 ? searchCriteriaList[0] : searchCriteriaList;
                }

                // Collect all unique requests first (in desc order, filtered server-side)
                while (hasMoreRows && pageNumber <= 10 && (maxTotalRecords == 0 || totalRows < maxTotalRecords))
                {
                    var startTime = DateTime.UtcNow;
                    // FIXED: Build input_data as anonymous object and serialize once (avoids nested interpolation errors)
                    // Renamed to payloadListInfo to avoid variable name conflict with parsing
                    var payloadListInfo = new
                    {
                        row_count = rowCount,
                        start_index = (pageNumber - 1) * rowCount,
                        sort_field = "created_time",
                        sort_order = "desc",
                        get_total_count = true,
                        search_criteria = searchCriteria
                    };
                    var inputData = new { list_info = payloadListInfo };
                    var inputDataJson = JsonSerializer.Serialize(inputData, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
                    var encodedInputData = HttpUtility.UrlEncode(inputDataJson);
                    string url = $"https://sdpondemand.manageengine.com/api/v3/requests?input_data={encodedInputData}";
                    var response = await apiClient.GetAsync(url, cts.Token);

                    if (!response.IsSuccessStatusCode)
                    {
                        var errorDetails = new { StatusCode = (int)response.StatusCode, Reason = response.ReasonPhrase, Page = pageNumber };
                        await LogDetails(logger, "Failure", errorDetails);
                        // FIXED: Retry with reduced row_count (same structure)
                        var reducedRowCount = Math.Min(rowCount / 2, 50);
                        var retryPayloadListInfo = new
                        {
                            row_count = reducedRowCount,
                            start_index = (pageNumber - 1) * rowCount,
                            sort_field = "created_time",
                            sort_order = "desc",
                            get_total_count = true,
                            search_criteria = searchCriteria
                        };
                        var retryInputData = new { list_info = retryPayloadListInfo };
                        var retryInputDataJson = JsonSerializer.Serialize(retryInputData, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
                        var retryEncodedInputData = HttpUtility.UrlEncode(retryInputDataJson);
                        url = $"https://sdpondemand.manageengine.com/api/v3/requests?input_data={retryEncodedInputData}";
                        response = await apiClient.GetAsync(url, cts.Token);

                        if (!response.IsSuccessStatusCode)
                        {
                            // Final fallback (no sorting or filtering guaranteed)
                            url = "https://sdpondemand.manageengine.com/api/v3/requests";
                            response = await apiClient.GetAsync(url, cts.Token);
                            if (!response.IsSuccessStatusCode)
                                return StatusCode((int)response.StatusCode, new { Error = $"API request failed on page {pageNumber}: {response.ReasonPhrase}" });
                        }
                    }

                    string jsonResponse = await response.Content.ReadAsStringAsync(cts.Token);
                    var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse) ?? new Dictionary<string, object>();
                    var requests = data.ContainsKey("requests") ? (JsonElement)data["requests"] : JsonDocument.Parse("[]").RootElement;
                    var currentRequests = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(requests.GetRawText(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new List<Dictionary<string, object>>(rowCount);

                    // Add to ordered List if ID not seen (preserves newest-first insertion order)
                    int rowsThisPage = 0;
                    foreach (var req in currentRequests)
                    {
                        if (req.TryGetValue("id", out var idObj) && seenIds.Add(idObj.ToString()))
                        {
                            allRequests.Add(req);
                            totalRows++;
                            rowsThisPage++;
                        }
                    }

                    // FIXED: Extract hasMoreRows check to separate statements to avoid pattern matching issues and variable conflicts
                    hasMoreRows = false;
                    if (data.TryGetValue("list_info", out var listInfoObjObj) &&
                        listInfoObjObj is JsonElement listInfoElem &&
                        listInfoElem.TryGetProperty("has_more_rows", out var hasMoreProp) &&
                        hasMoreProp.GetBoolean())
                    {
                        hasMoreRows = true;
                    }

                    // Track consecutive empty pages
                    if (rowsThisPage == 0) consecutiveEmptyPages++;
                    else consecutiveEmptyPages = 0;

                    var elapsedMs = (int)(DateTime.UtcNow - startTime).TotalMilliseconds;
                    await LogDetails(logger, "PageFetched", new { PageNumber = pageNumber, RowsThisPage = rowsThisPage, HasMoreRows = hasMoreRows, TotalRowsSoFar = totalRows, ElapsedMs = elapsedMs });

                    pageNumber++;
                    if (hasMoreRows && consecutiveEmptyPages < emptyPageThreshold) await Task.Delay(50, cts.Token);
                    else if (consecutiveEmptyPages >= emptyPageThreshold) hasMoreRows = false;
                }

                // No client-side filtering needed - allRequests are already filtered server-side
                var filteredRequests = allRequests; // Already ordered newest-first

                var resultDetails = new
                {
                    TotalPages = pageNumber - 1,
                    TotalRows = totalRows,
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
                var dateTo = DateTime.UtcNow;
                var dateFrom = dateTo.AddMonths(-months).Date;
                var requests = await FetchRequestsForDateRange(dateFrom, dateTo);
                var topAreas = requests
                    .Where(req => req.TryGetValue("subject", out var subjObj) && !string.IsNullOrEmpty(subjObj?.ToString()))
                    .GroupBy(req =>
                    {
                        req.TryGetValue("subject", out var subjectObj);
                        return subjectObj?.ToString()?.ToLowerInvariant() ?? string.Empty;
                    })
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
                    .Where(t => t.TryGetValue("name", out var nameObj) && !string.IsNullOrEmpty(nameObj?.ToString()))
                    .Select(t =>
                    {
                        t.TryGetValue("name", out var techNameObj);
                        return techNameObj?.ToString()?.ToLowerInvariant() ?? string.Empty;
                    })
                    .ToHashSet();

                var dateTo = DateTime.UtcNow;
                var dateFrom = dateTo.AddMonths(-months).Date;
                var requests = await FetchRequestsForDateRange(dateFrom, dateTo);

                var assignedTechs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var req in requests)
                {
                    if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
                    {
                        if (techElem.TryGetProperty("name", out var nameProp) && nameProp.ValueKind != JsonValueKind.Null)
                        {
                            assignedTechs.Add(nameProp.GetString()?.ToLowerInvariant() ?? string.Empty);
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

        private async Task<List<Dictionary<string, object>>> FetchRequestsForDateRange(DateTime dateFrom, DateTime dateTo)
        {
            var apiClient = _httpClientFactory.CreateClient();
            apiClient.Timeout = TimeSpan.FromSeconds(60);
            apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                "Zoho-oauthtoken", _zohoToken);

            long dateFromMs = new DateTimeOffset(dateFrom).ToUnixTimeMilliseconds();
            long dateToMs = new DateTimeOffset(dateTo).ToUnixTimeMilliseconds();

            var searchCriteria = new
            {
                field = "created_time",
                condition = "between",
                values = new[] { dateFromMs.ToString(), dateToMs.ToString() }
            };

            var allRequests = new List<Dictionary<string, object>>();
            var seenIds = new HashSet<string>();
            bool hasMoreRows = true;
            int pageNumber = 1;
            const int rowCount = 100;

            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));

            while (hasMoreRows && pageNumber <= 10)
            {
                var payloadListInfo = new
                {
                    row_count = rowCount,
                    start_index = (pageNumber - 1) * rowCount,
                    sort_field = "created_time",
                    sort_order = "desc",
                    get_total_count = true,
                    search_criteria = searchCriteria
                };
                var inputData = new { list_info = payloadListInfo };
                var inputDataJson = JsonSerializer.Serialize(inputData, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
                var encodedInputData = HttpUtility.UrlEncode(inputDataJson);
                string url = $"https://sdpondemand.manageengine.com/api/v3/requests?input_data={encodedInputData}";
                var response = await apiClient.GetAsync(url, cts.Token);

                if (!response.IsSuccessStatusCode)
                {
                    // Fallback without input_data
                    url = "https://sdpondemand.manageengine.com/api/v3/requests";
                    response = await apiClient.GetAsync(url, cts.Token);
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

                hasMoreRows = data.TryGetValue("list_info", out var listInfoObjObj) &&
                              listInfoObjObj is JsonElement listInfoElem &&
                              listInfoElem.TryGetProperty("has_more_rows", out var hasMoreProp) &&
                              hasMoreProp.GetBoolean();

                pageNumber++;
            }

            return allRequests;
        }

        private async Task<List<Dictionary<string, object>>> FetchAllTechnicians()
        {
            var cacheKey = "all-technicians";
            if (_cache.TryGetValue(cacheKey, out List<Dictionary<string, object>>? cachedTechs))
            {
                return cachedTechs;
            }

            var apiClient = _httpClientFactory.CreateClient();
            apiClient.Timeout = TimeSpan.FromSeconds(60);
            apiClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                "Zoho-oauthtoken", _zohoToken);

            // FIXED: Use search_fields: {"is_technician": "true"} for filtering technicians
            var searchFields = new { is_technician = "true" };

            var allTechnicians = new List<Dictionary<string, object>>();
            var seenIds = new HashSet<string>();
            bool hasMoreRows = true;
            int pageNumber = 1;
            const int rowCount = 100;

            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

            while (hasMoreRows && pageNumber <= 5)
            {
                var payloadListInfo = new
                {
                    row_count = rowCount,
                    start_index = (pageNumber - 1) * rowCount + 1, // 1-based index
                    sort_field = "name",
                    sort_order = "asc",
                    get_total_count = true,
                    search_fields = searchFields
                };
                var inputData = new { list_info = payloadListInfo };
                var inputDataJson = JsonSerializer.Serialize(inputData, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
                var encodedInputData = HttpUtility.UrlEncode(inputDataJson);
                string url = $"https://sdpondemand.manageengine.com/api/v3/users?input_data={encodedInputData}";
                var response = await apiClient.GetAsync(url, cts.Token);

                if (!response.IsSuccessStatusCode)
                {
                    // Fallback: Simple query param if supported, or without filter and client-side
                    url = "https://sdpondemand.manageengine.com/api/v3/users";
                    response = await apiClient.GetAsync(url, cts.Token);
                    if (!response.IsSuccessStatusCode)
                    {
                        throw new Exception($"Failed to fetch users: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                    }
                }

                string jsonResponse = await response.Content.ReadAsStringAsync(cts.Token);
                var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonResponse) ?? new Dictionary<string, object>();
                // FIXED: Use "users" key for the array
                var techniciansElem = data.ContainsKey("users") ? (JsonElement)data["users"] : JsonDocument.Parse("[]").RootElement;
                var currentTechnicians = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(techniciansElem.GetRawText(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new List<Dictionary<string, object>>();

                // Client-side filter if fallback (no search_fields)
                var filteredTechs = currentTechnicians.Where(tech => tech.TryGetValue("is_technician", out var isTechObj) && bool.TryParse(isTechObj?.ToString(), out bool isTech) && isTech).ToList();

                foreach (var tech in filteredTechs)
                {
                    if (tech.TryGetValue("id", out var idObj) && seenIds.Add(idObj.ToString()))
                    {
                        allTechnicians.Add(tech);
                    }
                }

                // FIXED: Use list_info for has_more_rows or total_count for pagination
                hasMoreRows = false;
                if (data.TryGetValue("list_info", out var listInfoObjObj) &&
                    listInfoObjObj is JsonElement listInfoElem &&
                    listInfoElem.TryGetProperty("has_more_rows", out var hasMoreProp) &&
                    hasMoreProp.GetBoolean())
                {
                    hasMoreRows = true;
                }
                else if (data.TryGetValue("list_info", out var liObj) &&
                         liObj is JsonElement liElem &&
                         liElem.TryGetProperty("total_count", out var totalProp) &&
                         totalProp.TryGetInt32(out int total) &&
                         (pageNumber - 1) * rowCount + filteredTechs.Count < total)
                {
                    hasMoreRows = true;
                }

                pageNumber++;
            }

            _cache.Set(cacheKey, allTechnicians, TimeSpan.FromDays(30)); // Cache technicians longer, as they change less frequently
            return allTechnicians;
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