using Azure;
using Azure.AI.Agents.Persistent;
using Azure.Identity;
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
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading;
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
        private readonly string _projectEndpoint;
        private readonly string _agentId;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IMemoryCache _cache;
        private readonly ChatService _chatService;
        private readonly QueryHistoryService _queryHistoryService;
        private readonly string _meAiEndpoint;
        private readonly string _meAiDeploymentName;
        private readonly string _meAiApiVersion;
        private readonly string _meAiApiKey;
        private readonly RequestStorageService _requestStorageService;
        private readonly AppDbContext _dbContext;

        public MainController(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            IMemoryCache cache,
            ChatService chatService,
            QueryHistoryService queryHistoryService,
            RequestStorageService requestStorageService,
            AppDbContext dbContext)
        {
            _projectEndpoint = configuration["AzureAIFoundry:ProjectEndpoint"] ?? throw new InvalidOperationException("ProjectEndpoint not configured.");
            _agentId = configuration["AzureAIFoundry:AgentId"] ?? "asst_MqwY6PBQdS9uxha6Hl1RQCJk";
            _httpClientFactory = httpClientFactory;
            _cache = cache;
            _chatService = chatService;
            _queryHistoryService = queryHistoryService;
            _meAiEndpoint = configuration["ManageEngineAI:Endpoint"] ?? throw new InvalidOperationException("ManageEngineAI:Endpoint not configured.");
            _meAiDeploymentName = configuration["ManageEngineAI:DeploymentName"] ?? throw new InvalidOperationException("ManageEngineAI:DeploymentName not configured.");
            _meAiApiVersion = configuration["ManageEngineAI:ApiVersion"] ?? throw new InvalidOperationException("ManageEngineAI:ApiVersion not configured.");
            _meAiApiKey = configuration["ManageEngineAI:ApiKey"] ?? throw new InvalidOperationException("ManageEngineAI:ApiKey not configured.");
            _requestStorageService = requestStorageService;
            _dbContext = dbContext;
        }

        // Sync endpoint to fetch and store recent records from ManageEngine
        [HttpPost("sync-recent-records")]
        public async Task<IActionResult> SyncRecentRecords()
        {
            try
            {
                var lastStoredDate = await _dbContext.ManageEngineRequests
                    .OrderByDescending(r => r.CreatedTime)
                    .Select(r => r.CreatedTime)
                    .FirstOrDefaultAsync();

                var dateFrom = lastStoredDate != default ? lastStoredDate : new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
                var dateTo = DateTimeOffset.UtcNow;

                Console.WriteLine($"Syncing records from {dateFrom:yyyy-MM-dd HH:mm:ss} to {dateTo:yyyy-MM-dd HH:mm:ss}");

                var requests = await FetchRequestsFromManageEngine(dateFrom, dateTo);

                int newCount = 0;
                var existingIds = await _dbContext.ManageEngineRequests
                    .Select(r => r.Id)
                    .ToHashSetAsync();

                foreach (var req in requests)
                {
                    var requestId = req["id"].ToString();
                    if (!existingIds.Contains(requestId))
                    {
                        var managedRequest = new ManageEngineRequest
                        {
                            Id = requestId,
                            Subject = req.TryGetValue("subject", out var subj) ? subj?.ToString() : null,
                            TechnicianName = ExtractTechnicianName(req),
                            CreatedTime = ExtractCreatedTime(req),
                            JsonData = JsonSerializer.Serialize(req)
                        };

                        _dbContext.ManageEngineRequests.Add(managedRequest);
                        newCount++;
                    }
                }

                await _dbContext.SaveChangesAsync();

                return Ok(new { Message = "Sync completed", NewRecords = newCount, TotalFetched = requests.Count });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // Main AI query endpoint - queries database based on AI-interpreted filters
        [HttpPost("ai-query")]
        public async Task<IActionResult> AiQuery([FromBody] AiQueryRequest request)
        {
            if (string.IsNullOrEmpty(request?.Query))
                return BadRequest("Query is required.");

            try
            {
                // First, try to detect specialized query types
                var queryType = await DetectQueryTypeFromAI(request.Query);

                if (queryType.Type == "top-request-areas" && queryType.Months.HasValue)
                {
                    var dateFrom = DateTime.UtcNow.AddMonths(-queryType.Months.Value);
                    var dateTo = DateTime.UtcNow;
                    return await GetTopRequestAreasDetailed(dateFrom, dateTo);
                }

                if (queryType.Type == "inactive-technicians" && queryType.Months.HasValue)
                    return await GetTechniciansWithoutRequests(queryType.Months.Value);

                if (queryType.Type == "requests-influx" && queryType.Months.HasValue)
                {
                    var dateFrom = DateTime.UtcNow.AddMonths(-queryType.Months.Value);
                    var dateTo = DateTime.UtcNow;
                    var hour = (queryType as dynamic)?.Hour;
                    return await GetRequestsInfluxByHour(dateFrom, dateTo, hour);
                }

                // For standard queries, extract filters and query database
                var filters = await GetFiltersFromAI(request.Query);
                return await QueryDatabase(filters);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = $"AI query processing failed: {ex.Message}" });
            }
        }

        // Query database with filters
        [HttpPost("filter-query")]
        public async Task<IActionResult> QueryDatabase([FromBody] Filters filters)
        {
            try
            {
                var query = _dbContext.ManageEngineRequests.AsQueryable();

                if (!string.IsNullOrEmpty(filters.Subject))
                    query = query.Where(r => r.Subject.Contains(filters.Subject));

                if (!string.IsNullOrEmpty(filters.Technician))
                    query = query.Where(r => r.TechnicianName.Contains(filters.Technician));

                if (DateTime.TryParse(filters.DateFrom, out var dateFromParsed) &&
                    DateTime.TryParse(filters.DateTo, out var dateToParsed))
                {
                    var dateFrom = new DateTimeOffset(dateFromParsed);
                    var dateTo = new DateTimeOffset(dateToParsed);
                    query = query.Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo);
                }

                var results = await query
                    .OrderByDescending(r => r.CreatedTime)
                    .Select(r => new
                    {
                        r.Id,
                        r.Subject,
                        r.TechnicianName,
                        r.CreatedTime
                    })
                    .ToListAsync();

                return Ok(new
                {
                    FilteredRequests = results,
                    Summary = new
                    {
                        TotalResults = results.Count,
                        AppliedFilters = new { filters.Subject, filters.Technician, filters.DateFrom, filters.DateTo },
                        Timestamp = DateTime.UtcNow
                    }
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // Advanced query endpoint for custom AI-generated queries
        [HttpPost("advanced-query")]
        public async Task<IActionResult> AdvancedQuery([FromBody] AdvancedQueryRequest request)
        {
            if (string.IsNullOrEmpty(request?.Query))
                return BadRequest("Query is required.");

            try
            {
                // Let AI generate a dynamic LINQ-like query description
                var queryDescription = await GenerateCustomQuery(request.Query);

                // Execute predefined query types based on AI description
                return queryDescription.Type switch
                {
                    "requests-by-technician" => await GetRequestsByTechnician(queryDescription.Technician),
                    "requests-by-subject" => await GetRequestsBySubject(queryDescription.Subject),
                    "requests-in-date-range" => await GetRequestsInDateRange(queryDescription.DateFrom, queryDescription.DateTo),
                    "technician-workload" => await GetTechnicianWorkload(queryDescription.DateFrom, queryDescription.DateTo),
                    _ => await QueryDatabase(new Filters(queryDescription.Subject, queryDescription.Technician, queryDescription.DateFrom, queryDescription.DateTo))
                };
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // Specialized endpoint: Get top request areas
        [HttpGet("top-request-areas")]
        public async Task<IActionResult> GetTopRequestAreas([FromQuery] int months = 1)
        {
            if (months < 1 || months > 12)
                return BadRequest("Months must be between 1 and 12.");

            try
            {
                var dateTo = DateTime.UtcNow;
                var dateFrom = dateTo.AddMonths(-months);

                var topAreas = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.Subject))
                    .GroupBy(r => r.Subject)
                    .Select(g => new { Subject = g.Key, Count = g.Count() })
                    .OrderByDescending(x => x.Count)
                    .Take(10)
                    .ToListAsync();

                return Ok(new { TopAreas = topAreas, Period = $"{months} month(s)", Timestamp = DateTime.UtcNow });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // Specialized endpoint: Get inactive technicians
        [HttpGet("inactive-technicians")]
        public async Task<IActionResult> GetInactiveTechnicians([FromQuery] int months = 1)
        {
            if (months < 1 || months > 12)
                return BadRequest("Months must be between 1 and 12.");

            try
            {
                var dateFrom = DateTime.UtcNow.AddMonths(-months);
                var dateTo = DateTime.UtcNow;

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

                var inactive = allTechnicians.Except(activeTechnicians).ToList();

                return Ok(new { InactiveTechnicians = inactive, Period = $"{months} month(s)", TotalTechnicians = allTechnicians.Count });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // Get distinct technicians
        [HttpGet("technicians")]
        public async Task<IActionResult> GetDistinctTechnicians()
        {
            try
            {
                var technicians = await _dbContext.ManageEngineRequests
                    .Where(r => !string.IsNullOrEmpty(r.TechnicianName))
                    .Select(r => r.TechnicianName)
                    .Distinct()
                    .OrderBy(t => t)
                    .ToListAsync();

                return Ok(technicians);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // 1. Influx of requests within a certain hour
        [HttpGet("requests-influx-by-hour")]
        public async Task<IActionResult> GetRequestsInfluxByHour([FromQuery] DateTime dateFrom, [FromQuery] DateTime dateTo, [FromQuery] int? hour = null)
        {
            try
            {
                var query = _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo);

                if (hour.HasValue && hour.Value >= 0 && hour.Value < 24)
                    query = query.Where(r => r.CreatedTime.Hour == hour.Value);

                var hourlyCounts = await query
                    .GroupBy(r => new { r.CreatedTime.Date, r.CreatedTime.Hour })
                    .Select(g => new
                    {
                        Date = g.Key.Date,
                        Hour = g.Key.Hour,
                        HourRange = $"{g.Key.Hour:00}:00-{g.Key.Hour:00}:59",
                        Count = g.Count(),
                        RequestIds = g.Select(r => r.Id).ToList()
                    })
                    .OrderBy(x => x.Date)
                    .ThenBy(x => x.Hour)
                    .ToListAsync();

                var peakHour = hourlyCounts.OrderByDescending(h => h.Count).FirstOrDefault();

                return Ok(new
                {
                    HourlyBreakdown = hourlyCounts,
                    Period = $"{dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                    SpecificHour = hour.HasValue ? $"{hour:00}:00" : "All hours",
                    TotalRequests = hourlyCounts.Sum(h => h.Count),
                    PeakHour = peakHour?.HourRange,
                    PeakHourCount = peakHour?.Count ?? 0,
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // 2. Top Request Areas (Subject Areas)
        [HttpGet("top-request-areas-detailed")]
        public async Task<IActionResult> GetTopRequestAreasDetailed([FromQuery] DateTime? dateFrom = null, [FromQuery] DateTime? dateTo = null, [FromQuery] int topN = 10)
        {
            try
            {
                var from = dateFrom ?? DateTime.UtcNow.AddMonths(-1);
                var to = dateTo ?? DateTime.UtcNow;

                var topAreas = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= from && r.CreatedTime <= to && !string.IsNullOrEmpty(r.Subject))
                    .GroupBy(r => r.Subject)
                    .Select(g => new
                    {
                        Subject = g.Key,
                        Count = g.Count(),
                        Percentage = ((double)g.Count() / _dbContext.ManageEngineRequests.Count(r => r.CreatedTime >= from && r.CreatedTime <= to)) * 100,
                        RequestIds = g.Select(r => r.Id).ToList(),
                        LatestRequest = g.OrderByDescending(r => r.CreatedTime).FirstOrDefault().CreatedTime,
                        EarliestRequest = g.OrderBy(r => r.CreatedTime).FirstOrDefault().CreatedTime
                    })
                    .OrderByDescending(x => x.Count)
                    .Take(topN)
                    .ToListAsync();

                var totalRequests = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= from && r.CreatedTime <= to)
                    .CountAsync();

                var topAreasWithDetails = topAreas.Select(area => new
                {
                    area.Subject,
                    area.Count,
                    Percentage = Math.Round(area.Percentage, 2),
                    area.RequestIds,
                    area.LatestRequest,
                    area.EarliestRequest
                }).ToList();

                return Ok(new
                {
                    TopAreas = topAreasWithDetails,
                    Period = $"{from:yyyy-MM-dd} to {to:yyyy-MM-dd}",
                    TotalRequests = totalRequests,
                    UniqueSubjects = topAreasWithDetails.Count,
                    TopArea = topAreasWithDetails.FirstOrDefault()?.Subject,
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // 3. Technicians without any request for a specified duration
        [HttpGet("technicians-without-requests")]
        public async Task<IActionResult> GetTechniciansWithoutRequests([FromQuery] int monthsDuration = 1)
        {
            if (monthsDuration < 1 || monthsDuration > 12)
                return BadRequest("Month duration must be between 1 and 12.");

            try
            {
                var cutoffDate = DateTime.UtcNow.AddMonths(-monthsDuration);

                // Get all technicians that ever existed
                var allTechnicians = await _dbContext.ManageEngineRequests
                    .Where(r => !string.IsNullOrEmpty(r.TechnicianName))
                    .Select(r => r.TechnicianName)
                    .Distinct()
                    .ToListAsync();

                // Get technicians active in the specified duration
                var activeTechnicians = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= cutoffDate && r.CreatedTime <= DateTime.UtcNow && !string.IsNullOrEmpty(r.TechnicianName))
                    .Select(r => r.TechnicianName)
                    .Distinct()
                    .ToListAsync();

                // Inactive = all - active in duration
                var inactiveTechnicians = allTechnicians.Except(activeTechnicians).ToList();

                // Get details for inactive technicians
                var inactiveDetails = new List<object>();

                foreach (var techName in inactiveTechnicians)
                {
                    var lastRequestDate = await _dbContext.ManageEngineRequests
                        .Where(r => r.TechnicianName == techName)
                        .OrderByDescending(r => r.CreatedTime)
                        .Select(r => r.CreatedTime)
                        .FirstOrDefaultAsync();

                    var totalRequests = await _dbContext.ManageEngineRequests
                        .Where(r => r.TechnicianName == techName)
                        .CountAsync();

                    inactiveDetails.Add(new
                    {
                        TechnicianName = techName,
                        LastRequestDate = lastRequestDate,
                        DaysSinceLastRequest = (DateTime.UtcNow - lastRequestDate.DateTime).Days,
                        TotalRequestsAllTime = totalRequests
                    });
                }

                var sortedInactive = inactiveDetails
                    .Cast<dynamic>()
                    .OrderByDescending(t => t.DaysSinceLastRequest)
                    .ToList();

                return Ok(new
                {
                    InactiveTechnicians = sortedInactive,
                    DurationMonths = monthsDuration,
                    CutoffDate = cutoffDate.ToString("yyyy-MM-dd"),
                    TotalTechnicians = allTechnicians.Count,
                    InactiveCount = inactiveTechnicians.Count,
                    ActiveCount = activeTechnicians.Count,
                    InactivityPercentage = Math.Round(((double)inactiveTechnicians.Count / allTechnicians.Count) * 100, 2),
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // Get requests with full JSON data
        [HttpGet("requests-with-data")]
        public async Task<IActionResult> GetRequestsWithData([FromQuery] string? subject = null, [FromQuery] string? technician = null, [FromQuery] DateTime? dateFrom = null, [FromQuery] DateTime? dateTo = null, [FromQuery] int take = 100)
        {
            try
            {
                var query = _dbContext.ManageEngineRequests.AsQueryable();

                if (!string.IsNullOrEmpty(subject))
                    query = query.Where(r => r.Subject.Contains(subject));

                if (!string.IsNullOrEmpty(technician))
                    query = query.Where(r => r.TechnicianName.Contains(technician));

                if (dateFrom.HasValue)
                    query = query.Where(r => r.CreatedTime >= dateFrom.Value);

                if (dateTo.HasValue)
                    query = query.Where(r => r.CreatedTime <= dateTo.Value);

                var requests = await query
                    .OrderByDescending(r => r.CreatedTime)
                    .Take(take)
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
                    Requests = requests,
                    FilterApplied = new { subject, technician, dateFrom, dateTo },
                    Count = requests.Count,
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        // Helper endpoints for advanced queries
        private async Task<IActionResult> GetRequestsByTechnician(string technician)
        {
            var requests = await _dbContext.ManageEngineRequests
                .Where(r => r.TechnicianName.Contains(technician))
                .OrderByDescending(r => r.CreatedTime)
                .Take(100)
                .ToListAsync();

            return Ok(new { Requests = requests, Technician = technician, Count = requests.Count });
        }

        private async Task<IActionResult> GetRequestsBySubject(string subject)
        {
            var requests = await _dbContext.ManageEngineRequests
                .Where(r => r.Subject.Contains(subject))
                .OrderByDescending(r => r.CreatedTime)
                .Take(100)
                .ToListAsync();

            return Ok(new { Requests = requests, Subject = subject, Count = requests.Count });
        }

        private async Task<IActionResult> GetRequestsInDateRange(string dateFrom, string dateTo)
        {
            if (!DateTime.TryParse(dateFrom, out var from) || !DateTime.TryParse(dateTo, out var to))
                return BadRequest("Invalid date format.");

            var requests = await _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= from && r.CreatedTime <= to)
                .OrderByDescending(r => r.CreatedTime)
                .Take(500)
                .ToListAsync();

            return Ok(new { Requests = requests, DateRange = $"{dateFrom} to {dateTo}", Count = requests.Count });
        }

        private async Task<IActionResult> GetTechnicianWorkload(string dateFrom, string dateTo)
        {
            if (!DateTime.TryParse(dateFrom, out var from) || !DateTime.TryParse(dateTo, out var to))
                return BadRequest("Invalid date format.");

            var workload = await _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= from && r.CreatedTime <= to && !string.IsNullOrEmpty(r.TechnicianName))
                .GroupBy(r => r.TechnicianName)
                .Select(g => new { Technician = g.Key, RequestCount = g.Count() })
                .OrderByDescending(x => x.RequestCount)
                .ToListAsync();

            return Ok(new { Workload = workload, Period = $"{dateFrom} to {dateTo}" });
        }

        // AI detection for query type
        private async Task<QueryType> DetectQueryTypeFromAI(string userQuery)
        {
            var apiClient = _httpClientFactory.CreateClient();
            var fullUrl = $"{_meAiEndpoint}openai/deployments/{_meAiDeploymentName}/chat/completions?api-version={_meAiApiVersion}";

            var detectionPrompt = $@"Analyze this query and determine its type. Return ONLY a JSON object.

If the query is asking for:
1. Top/most common request areas/categories/subjects - return {{""type"": ""top-request-areas"", ""months"": <number>}}
2. Inactive technicians / technicians without requests - return {{""type"": ""inactive-technicians"", ""months"": <number>}}
3. Influx of requests within a certain hour / requests per hour - return {{""type"": ""requests-influx"", ""hour"": <number or null>, ""months"": <number>}}
4. Otherwise - return {{""type"": ""standard"", ""months"": null}}

Extract the number of months from relative time expressions (default = 1).
Extract the hour (0-23) if a specific hour is mentioned, otherwise null.
Output ONLY the JSON object, no other text.

Examples:
- ""Show me top request areas for last 3 months"" -> {{""type"": ""top-request-areas"", ""months"": 3}}
- ""Which technicians have been inactive for 6 months?"" -> {{""type"": ""inactive-technicians"", ""months"": 6}}
- ""How many requests came in during hour 14 last month?"" -> {{""type"": ""requests-influx"", ""hour"": 14, ""months"": 1}}
- ""Show password reset tickets from January"" -> {{""type"": ""standard"", ""months"": null}}

User query: {userQuery}";

            var requestBody = new
            {
                messages = new[] { new { role = "user", content = detectionPrompt } },
                max_tokens = 100,
                temperature = 0.1
            };

            var json = JsonSerializer.Serialize(requestBody);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            apiClient.DefaultRequestHeaders.Add("api-key", _meAiApiKey);

            var response = await apiClient.PostAsync(fullUrl, content);

            if (!response.IsSuccessStatusCode)
                return new QueryType { Type = "standard", Months = null };

            var responseJson = await response.Content.ReadAsStringAsync();
            var aiResponse = JsonSerializer.Deserialize<AiResponse>(responseJson);

            if (aiResponse?.Choices == null || aiResponse.Choices.Count == 0)
                return new QueryType { Type = "standard", Months = null };

            var outputContent = aiResponse.Choices[0].Message.Content;
            var queryType = JsonSerializer.Deserialize<QueryType>(outputContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? new QueryType { Type = "standard", Months = null };

            return queryType;
        }

        // Extract filters from natural language
        private async Task<Filters> GetFiltersFromAI(string userQuery)
        {
            var apiClient = _httpClientFactory.CreateClient();
            var fullUrl = $"{_meAiEndpoint}openai/deployments/{_meAiDeploymentName}/chat/completions?api-version={_meAiApiVersion}";
            string currentDate = DateTime.UtcNow.ToString("yyyy-MM-dd");

            var systemPrompt = $@"Convert natural language queries into structured filters.
Current date: {currentDate}

Output JSON with keys: subject, technician, dateFrom, dateTo (all nullable strings in yyyy-MM-dd format).

Example:
Query: password reset tickets from John last month
Output: {{""subject"": ""password reset"", ""technician"": ""John"", ""dateFrom"": ""{DateTime.Parse(currentDate).AddMonths(-1):yyyy-MM-dd}"", ""dateTo"": ""{currentDate}""}}";

            var userPrompt = $"Query: {userQuery}";

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
                throw new Exception($"AI API request failed: {response.StatusCode}");

            var responseJson = await response.Content.ReadAsStringAsync();
            var aiResponse = JsonSerializer.Deserialize<AiResponse>(responseJson);

            if (aiResponse?.Choices == null || aiResponse.Choices.Count == 0)
                throw new Exception("Invalid AI response format.");

            var outputContent = aiResponse.Choices[0].Message.Content;
            var filters = JsonSerializer.Deserialize<Filters>(outputContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? new Filters(null, null, null, null);

            return filters;
        }

        // Generate custom query description
        private async Task<QueryDescription> GenerateCustomQuery(string userQuery)
        {
            var apiClient = _httpClientFactory.CreateClient();
            var fullUrl = $"{_meAiEndpoint}openai/deployments/{_meAiDeploymentName}/chat/completions?api-version={_meAiApiVersion}";

            var prompt = $@"Analyze this query and return a JSON description.
Return type as one of: requests-by-technician, requests-by-subject, requests-in-date-range, technician-workload, or standard.
Include subject, technician, dateFrom, dateTo fields.

Query: {userQuery}";

            var requestBody = new
            {
                messages = new[] { new { role = "user", content = prompt } },
                max_tokens = 150,
                temperature = 0.2
            };

            var json = JsonSerializer.Serialize(requestBody);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            apiClient.DefaultRequestHeaders.Add("api-key", _meAiApiKey);

            var response = await apiClient.PostAsync(fullUrl, content);
            var responseJson = await response.Content.ReadAsStringAsync();
            var aiResponse = JsonSerializer.Deserialize<AiResponse>(responseJson);

            if (aiResponse?.Choices == null || aiResponse.Choices.Count == 0)
                return new QueryDescription { Type = "standard" };

            var outputContent = aiResponse.Choices[0].Message.Content;
            return JsonSerializer.Deserialize<QueryDescription>(outputContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? new QueryDescription { Type = "standard" };
        }

        // Fetch requests from ManageEngine API
        private async Task<List<Dictionary<string, object>>> FetchRequestsFromManageEngine(DateTimeOffset dateFrom, DateTimeOffset dateTo)
        {
            var apiClient = _httpClientFactory.CreateClient();
            apiClient.Timeout = TimeSpan.FromSeconds(60);

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

            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));

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
                        throw new Exception("Unauthorized access to ManageEngine API.");

                    throw new Exception($"Failed to fetch requests: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
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
                {
                    if (listInfoElem.TryGetProperty("has_more_rows", out var hasMoreProp))
                        hasMoreRows = hasMoreProp.GetBoolean();
                }

                pageNumber++;
                if (hasMoreRows) await Task.Delay(50, cts.Token);
            }

            return allRequests;
        }

        // Import requests from CSV/XLSX
        [HttpPost("import-requests")]
        public async Task<IActionResult> ImportRequests(IFormFile file)
        {
            if (file == null || file.Length == 0)
                return BadRequest("No file uploaded.");

            var requests = new List<ManageEngineRequest>();

            using var stream = file.OpenReadStream();

            if (file.FileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            {
                using var reader = new StreamReader(stream);
                using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
                var records = csv.GetRecords<dynamic>().ToList();

                foreach (var record in records)
                {
                    var dict = (IDictionary<string, object>)record;
                    var request = new ManageEngineRequest
                    {
                        Id = dict.ContainsKey("Request ID") ? dict["Request ID"]?.ToString() : Guid.NewGuid().ToString(),
                        Subject = dict.ContainsKey("Subject") ? dict["Subject"]?.ToString() : null,
                        TechnicianName = dict.ContainsKey("Technician") ? dict["Technician"]?.ToString() : null,
                        CreatedTime = dict.ContainsKey("Created Date")
                            ? DateTimeOffset.Parse(dict["Created Date"].ToString())
                            : DateTimeOffset.UtcNow,
                        JsonData = JsonSerializer.Serialize(dict)
                    };
                    requests.Add(request);
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

                    var request = new ManageEngineRequest
                    {
                        Id = dict.ContainsKey("id") ? dict["id"]?.ToString() : Guid.NewGuid().ToString(),
                        Subject = dict.ContainsKey("subject") ? dict["subject"]?.ToString() : null,
                        TechnicianName = dict.ContainsKey("technician") ? dict["technician"]?.ToString() : null,
                        CreatedTime = dict.ContainsKey("created_time")
                            ? DateTimeOffset.Parse(dict["created_time"].ToString())
                            : DateTimeOffset.UtcNow,
                        JsonData = JsonSerializer.Serialize(dict)
                    };
                    requests.Add(request);
                }
            }
            else
            {
                return BadRequest("Unsupported file format. Please upload CSV or XLSX.");
            }

            var existingIds = await _dbContext.ManageEngineRequests
                .Select(r => r.Id)
                .ToHashSetAsync();

            var newRequests = requests
                .Where(r => !existingIds.Contains(r.Id))
                .ToList();

            await _dbContext.ManageEngineRequests.AddRangeAsync(newRequests);
            await _dbContext.SaveChangesAsync();

            return Ok(new { Imported = newRequests.Count, Skipped = requests.Count - newRequests.Count });
        }

        // Helper methods
        private string ExtractTechnicianName(Dictionary<string, object> req)
        {
            if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
            {
                if (techElem.TryGetProperty("name", out var nameProp) && !string.IsNullOrEmpty(nameProp.GetString()))
                    return nameProp.GetString();
            }
            return null;
        }

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

    // Helper classes
    public class AiQueryRequest { public string Query { get; set; } }
    public class AdvancedQueryRequest { public string Query { get; set; } }
    public record Filters(string? Subject, string? Technician, string? DateFrom, string? DateTo);

    public class QueryType
    {
        public string Type { get; set; }
        public int? Months { get; set; }
        public int? Hour { get; set; }
    }

    public class QueryDescription
    {
        public string Type { get; set; }
        public string Subject { get; set; }
        public string Technician { get; set; }
        public string DateFrom { get; set; }
        public string DateTo { get; set; }
    }

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