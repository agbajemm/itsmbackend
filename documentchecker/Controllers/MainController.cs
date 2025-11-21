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
using System.Dynamic;
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
using System.Xml.Linq;
using CsvHelper;
using OfficeOpenXml;
using System.IO;

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
        private readonly RequestStorageService _requestStorageService;
        private readonly AppDbContext _dbContext;
        private readonly IServiceProvider _serviceProvider;

        public MainController(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            IMemoryCache cache,
            ChatService chatService,
            QueryHistoryService queryHistoryService,
            RequestStorageService requestStorageService,
            AppDbContext dbContext,
            IServiceProvider serviceProvider)
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
            _meAiEndpoint = configuration["ManageEngineAI:Endpoint"] ?? throw new InvalidOperationException("ManageEngineAI:Endpoint not configured.");
            _meAiDeploymentName = configuration["ManageEngineAI:DeploymentName"] ?? throw new InvalidOperationException("ManageEngineAI:DeploymentName not configured.");
            _meAiApiVersion = configuration["ManageEngineAI:ApiVersion"] ?? throw new InvalidOperationException("ManageEngineAI:ApiVersion not configured.");
            _meAiApiKey = configuration["ManageEngineAI:ApiKey"] ?? throw new InvalidOperationException("ManageEngineAI:ApiKey not configured.");
            _requestStorageService = requestStorageService;
            _dbContext = dbContext;
            _serviceProvider = serviceProvider;
        }

        [HttpPost("sync-requests")]
        public async Task<IActionResult> SyncRequests()
        {
            try
            {
                var lastStoredDate = await _requestStorageService.GetLastStoredDateAsync();
                DateTimeOffset dateFrom;
                if (lastStoredDate.HasValue)
                {
                    dateFrom = lastStoredDate.Value.AddMinutes(-5);
                }
                else
                {
                    dateFrom = DateTimeOffset.UtcNow.AddMonths(-1);
                }
                var dateTo = DateTimeOffset.UtcNow;
                var requests = await FetchRequestsForDateRange(dateFrom, dateTo);
                int stored = 0;
                foreach (var req in requests)
                {
                    var requestId = req["id"].ToString();
                    if (!await _requestStorageService.RequestExistsAsync(requestId))
                    {
                        await _requestStorageService.StoreRequestAsync(req);
                        stored++;
                    }
                }
                return Ok(new { Message = "Sync completed", NewRecords = stored, TotalFetched = requests.Count });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        [HttpPost("natural-query")]
        public async Task<IActionResult> NaturalQuery([FromBody] NaturalQueryRequest request)
        {
            if (string.IsNullOrEmpty(request?.Query))
            {
                return BadRequest("Query is required.");
            }

            string sessionId = string.IsNullOrEmpty(request.SessionId) ? Guid.NewGuid().ToString() : request.SessionId;

            var conversation = await _dbContext.ChatConversations
                .Include(c => c.Messages)
                .FirstOrDefaultAsync(c => c.SessionId == sessionId);

            if (conversation == null)
            {
                conversation = new ChatConversation { SessionId = sessionId };
                _dbContext.ChatConversations.Add(conversation);
            }

            if (string.IsNullOrEmpty(conversation.UserEmail) && !string.IsNullOrEmpty(request.UserEmail))
            {
                conversation.UserEmail = request.UserEmail;
            }

            var userMessage = new ChatMessage { Role = "user", Content = request.Query };
            conversation.Messages.Add(userMessage);
            await _dbContext.SaveChangesAsync();

            try
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        using var scope = _serviceProvider.CreateScope();
                        var backgroundDbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                        var backgroundRequestStorage = scope.ServiceProvider.GetRequiredService<RequestStorageService>();
                        await SyncRequestsInBackgroundSafe(backgroundDbContext, backgroundRequestStorage);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Background sync failed: {ex.Message}");
                    }
                });

                // Build conversation context from previous messages
                var conversationContext = BuildConversationContext(conversation);

                // Analyze query with AI and include conversation context
                var queryAnalysis = await AnalyzeQueryWithAI(request.Query, conversationContext, request.UserEmail);
                Console.WriteLine($"Query Analysis: {JsonSerializer.Serialize(queryAnalysis)}");

                IActionResult queryResult;
                switch (queryAnalysis.QueryType)
                {
                    case "inactive_technicians":
                        queryResult = await HandleInactiveTechniciansQuery(queryAnalysis);
                        break;
                    case "influx_requests":
                        queryResult = await HandleInfluxRequestsQuery(queryAnalysis);
                        break;
                    case "top_request_areas":
                        queryResult = await HandleTopRequestAreasQuery(queryAnalysis);
                        break;
                    case "top_technicians":
                        queryResult = await HandleTopTechniciansQuery(queryAnalysis);
                        break;
                    case "request_search":
                        queryResult = await HandleRequestSearchQuery(queryAnalysis, request.UserEmail, request.Query);
                        break;
                    default:
                        queryResult = BadRequest("Unable to determine query type.");
                        break;
                }

                if (queryResult is not OkObjectResult okResult || okResult.Value == null)
                {
                    return queryResult;
                }

                var rawData = okResult.Value;
                var conversationalText = await GenerateConversationalResponseAsync(rawData, queryAnalysis, request.Query, conversationContext);
                var fileName = $"queryresult{DateTime.UtcNow.ToString("yyyyMMddHHmmss")}.csv";
                var url = $"/api/Main/download-result/{sessionId}/{fileName}";

                var finalResponseFull = new
                {
                    SessionId = sessionId,
                    ConversationalResponse = conversationalText,
                    ExcelFile = new
                    {
                        FileName = fileName,
                        Data = "",
                        Url = url
                    },
                    Data = rawData
                };

                var agentMessage = new ChatMessage { Role = "agent", Content = JsonSerializer.Serialize(finalResponseFull) };
                conversation.Messages.Add(agentMessage);
                await _dbContext.SaveChangesAsync();

                return Ok(new
                {
                    SessionId = sessionId,
                    ConversationalResponse = finalResponseFull.ConversationalResponse,
                    ExcelFile = finalResponseFull.ExcelFile
                });
            }
            catch (Exception ex)
            {
                var errorContent = JsonSerializer.Serialize(new { Error = $"Query processing failed: {ex.Message}", Details = ex.StackTrace });
                var agentMessage = new ChatMessage { Role = "agent", Content = errorContent };
                conversation.Messages.Add(agentMessage);
                await _dbContext.SaveChangesAsync();
                return Ok(new { SessionId = sessionId, Data = new { Error = $"Query processing failed: {ex.Message}" } });
            }
        }

        [HttpGet("download-result/{sessionId}/{fileName}")]
        public async Task<IActionResult> DownloadResult(string sessionId, string fileName)
        {
            if (fileName.Contains("..") || fileName.Contains("/") || fileName.Contains("\\"))
                return BadRequest("Invalid file name.");

            var conversation = await _dbContext.ChatConversations
                .Include(c => c.Messages)
                .FirstOrDefaultAsync(c => c.SessionId == sessionId);

            if (conversation == null)
                return NotFound("Conversation not found.");

            var lastAgentMessage = conversation.Messages
                .LastOrDefault(m => m.Role == "agent");

            if (lastAgentMessage == null)
                return NotFound("No result found for this session.");

            using var jsonDoc = JsonDocument.Parse(lastAgentMessage.Content);
            var root = jsonDoc.RootElement;

            if (!root.TryGetProperty("Data", out var dataElement))
                return NotFound("Data not found.");

            var csvFileName = Path.GetFileNameWithoutExtension(fileName) + ".csv";
            var csvBytes = GenerateCsvFromData(dataElement);
            return File(csvBytes, "text/csv", csvFileName);
        }

        [HttpGet("chat-history/{sessionId}")]
        public async Task<IActionResult> GetChatHistory(string sessionId)
        {
            var conversation = await _dbContext.ChatConversations
                .Include(c => c.Messages)
                .FirstOrDefaultAsync(c => c.SessionId == sessionId);

            if (conversation == null)
            {
                return NotFound("Conversation not found.");
            }

            var history = conversation.Messages
                .OrderBy(m => m.SentAt)
                .Select(m => new { m.Role, m.Content, m.SentAt })
                .ToList();

            return Ok(new { SessionId = sessionId, StartedAt = conversation.StartedAt, Messages = history });
        }

        private string BuildConversationContext(ChatConversation conversation)
        {
            if (conversation?.Messages == null || conversation.Messages.Count <= 1)
                return "";

            var recentMessages = conversation.Messages
                .OrderByDescending(m => m.SentAt)
                .Take(10)
                .OrderBy(m => m.SentAt)
                .ToList();

            if (recentMessages.Count <= 1)
                return "";

            var sb = new StringBuilder();
            sb.AppendLine("Previous conversation context:");
            foreach (var msg in recentMessages.Where(m => m.Role != "agent" || !m.Content.StartsWith("{")))
            {
                var role = msg.Role == "user" ? "User" : "Assistant";
                var content = msg.Content.Length > 200 ? msg.Content.Substring(0, 200) + "..." : msg.Content;
                sb.AppendLine($"  {role}: {content}");
            }

            return sb.ToString();
        }

        private async Task SyncRequestsInBackgroundSafe(AppDbContext dbContext, RequestStorageService requestStorageService)
        {
            try
            {
                var lastStoredDate = await requestStorageService.GetLastStoredDateAsync();
                DateTimeOffset dateFrom;
                if (lastStoredDate.HasValue)
                {
                    dateFrom = lastStoredDate.Value.AddMinutes(-5);
                }
                else
                {
                    dateFrom = DateTimeOffset.UtcNow.AddMonths(-1);
                }
                var dateTo = DateTimeOffset.UtcNow;
                var requests = await FetchRequestsForDateRange(dateFrom, dateTo);
                foreach (var req in requests)
                {
                    var requestId = req["id"].ToString();
                    if (!await requestStorageService.RequestExistsAsync(requestId))
                    {
                        await requestStorageService.StoreRequestAsync(req);
                    }
                }
                Console.WriteLine($"Background sync completed: Fetched {requests.Count} requests");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Background sync error (non-blocking): {ex.Message}");
            }
        }

        private async Task<QueryAnalysis> AnalyzeQueryWithAI(string userQuery, string conversationContext = "", string userEmail = "")
        {
            var apiClient = _httpClientFactory.CreateClient();
            var fullUrl = $"{_meAiEndpoint}openai/deployments/{_meAiDeploymentName}/chat/completions?api-version={_meAiApiVersion}";

            string currentDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
            string currentTime = DateTime.UtcNow.ToString("HH:mm");

            var analysisPrompt = $@"You are an AI query analyzer. Analyze the user query and extract structured information.

Current date: {currentDate}
Current time: {currentTime}
User Email: {(string.IsNullOrEmpty(userEmail) ? "Unknown" : userEmail)}

{(!string.IsNullOrEmpty(conversationContext) ? $"CONVERSATION CONTEXT:\n{conversationContext}\n\nUse context to understand follow-up questions and maintain continuity.\n" : "")}

Determine the query type and extract parameters. Return ONLY a JSON object with NO explanations or additional text.

Query types:
1. 'inactive_technicians' - asking for technicians with no activity
2. 'influx_requests' - asking for request counts by hour/day
3. 'top_request_areas' - asking for top request subjects/categories
4. 'top_technicians' - asking for ranking of technicians by requests handled
5. 'request_search' - searching for specific requests with filters

IMPORTANT - Default Date Ranges by Query Type (when user doesn't specify):
- inactive_technicians: 30 days (unless user specifies inactivityPeriod)
- influx_requests: 7 days (recent trends)
- top_request_areas: 30 days
- top_technicians: 30 days
- request_search: 30 days (unless user says ""today"", ""this week"", etc)

For time periods, convert to absolute dates:
- 'today' = today's date
- 'yesterday' = yesterday's date
- 'this week' = last 7 days
- 'past X hours' = from now minus X hours to now
- 'past X days' = from now minus X days to now
- 'past X weeks' = from now minus X*7 days to now
- 'past X months' = from now minus X months to now
- 'this month' = from 1st of current month to today
- 'last month' = from 1st of last month to last day of last month

PERSONALIZATION RULES:
- If query mentions ""me"", ""my"", ""mine"" or is about the current user's requests, set a flag in the response
- If query mentions a specific technician name, extract it to 'technician' field
- If query is about the requester (user asking about their own requests), leave 'technician' empty

Response JSON structure:
{{
  ""queryType"": ""one of the types above"",
  ""dateFrom"": ""yyyy-MM-dd HH:mm or null"",
  ""dateTo"": ""yyyy-MM-dd HH:mm or null"",
  ""timeUnit"": ""hour|day or null"",
  ""topN"": number or null,
  ""subject"": ""search subject or null"",
  ""technician"": ""technician name or null"",
  ""inactivityPeriod"": ""X days/weeks/months or null"",
  ""isUserRequest"": true/false (true if asking about user's own requests)
}}

Examples:
Query: show me my requests
Response: {{""queryType"": ""request_search"", ""dateFrom"": ""{DateTime.Parse(currentDate).AddDays(-30):yyyy-MM-dd} 00:00"", ""dateTo"": ""{currentDate} 23:59"", ""timeUnit"": null, ""topN"": null, ""subject"": null, ""technician"": null, ""inactivityPeriod"": null, ""isUserRequest"": true}}

Query: technicians inactive for 2 weeks
Response: {{""queryType"": ""inactive_technicians"", ""dateFrom"": null, ""dateTo"": null, ""inactivityPeriod"": ""14 days"", ""topN"": null, ""subject"": null, ""technician"": null, ""timeUnit"": null, ""isUserRequest"": false}}

Query: password reset requests from John
Response: {{""queryType"": ""request_search"", ""dateFrom"": ""{DateTime.Parse(currentDate).AddDays(-30):yyyy-MM-dd} 00:00"", ""dateTo"": ""{currentDate} 23:59"", ""subject"": ""password reset"", ""technician"": ""John"", ""topN"": null, ""timeUnit"": null, ""inactivityPeriod"": null, ""isUserRequest"": false}}

Query: top 10 technicians this month
Response: {{""queryType"": ""top_technicians"", ""dateFrom"": ""{currentDate.Substring(0, 7)}-01 00:00"", ""dateTo"": ""{currentDate} 23:59"", ""topN"": 10, ""subject"": null, ""technician"": null, ""timeUnit"": null, ""inactivityPeriod"": null, ""isUserRequest"": false}}

Query: show influx of requests today by hour
Response: {{""queryType"": ""influx_requests"", ""dateFrom"": ""{currentDate} 00:00"", ""dateTo"": ""{currentDate} 23:59"", ""timeUnit"": ""hour"", ""topN"": null, ""subject"": null, ""technician"": null, ""inactivityPeriod"": null, ""isUserRequest"": false}}

User query: {userQuery}";

            var requestBody = new
            {
                messages = new[]
                {
                    new { role = "user", content = analysisPrompt }
                },
                max_tokens = 500,
                temperature = 0.1
            };

            var json = JsonSerializer.Serialize(requestBody);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            apiClient.DefaultRequestHeaders.Add("api-key", _meAiApiKey);

            var response = await apiClient.PostAsync(fullUrl, content);

            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                throw new Exception($"AI analysis failed: {response.StatusCode} - {errorContent}");
            }

            var responseJson = await response.Content.ReadAsStringAsync();
            var aiResponse = JsonSerializer.Deserialize<AiResponse>(responseJson);

            if (aiResponse?.Choices == null || aiResponse.Choices.Count == 0)
            {
                throw new Exception("No response from AI analysis.");
            }

            var outputContent = aiResponse.Choices[0].Message.Content;
            Console.WriteLine($"Raw AI Output: {outputContent}");

            var cleanedContent = outputContent.Trim();
            if (cleanedContent.StartsWith("```json"))
                cleanedContent = cleanedContent.Substring(7);
            else if (cleanedContent.StartsWith("```"))
                cleanedContent = cleanedContent.Substring(3);

            if (cleanedContent.EndsWith("```"))
                cleanedContent = cleanedContent.Substring(0, cleanedContent.Length - 3);

            cleanedContent = cleanedContent.Trim();
            Console.WriteLine($"Cleaned AI Output: {cleanedContent}");

            var analysis = JsonSerializer.Deserialize<QueryAnalysis>(cleanedContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? throw new Exception("Failed to parse query analysis.");

            return analysis;
        }

        private async Task<IActionResult> HandleInactiveTechniciansQuery(QueryAnalysis analysis)
        {
            try
            {
                var (daysInactive, _) = ParseInactivityPeriod(analysis.InactivityPeriod);
                var dateTo = DateTimeOffset.UtcNow;
                var dateFrom = dateTo.AddDays(-daysInactive);

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

                var inactive = allTechnicians.Except(activeTechnicians, StringComparer.OrdinalIgnoreCase).ToList();

                return Ok(new
                {
                    QueryType = "InactiveTechnicians",
                    InactivityPeriod = analysis.InactivityPeriod,
                    Period = $"From {dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                    InactiveTechnicians = inactive,
                    TotalInactive = inactive.Count,
                    TotalTechnicians = allTechnicians.Count,
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        private async Task<IActionResult> HandleInfluxRequestsQuery(QueryAnalysis analysis)
        {
            try
            {
                var dateFrom = ParseDateTime(analysis.DateFrom) ?? DateTimeOffset.UtcNow.AddDays(-7).DateTime;
                var dateTo = ParseDateTime(analysis.DateTo) ?? DateTimeOffset.UtcNow.DateTime;

                var timeUnit = analysis.TimeUnit?.ToLower() ?? "hour";

                if (timeUnit == "hour")
                {
                    var allRequests = await _dbContext.ManageEngineRequests
                        .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo)
                        .Select(r => new { r.CreatedTime })
                        .ToListAsync();

                    var hourlyCounts = allRequests
                        .GroupBy(r => new { Date = r.CreatedTime.Date, Hour = r.CreatedTime.Hour })
                        .Select(g => new
                        {
                            DateTime = new DateTime(g.Key.Date.Year, g.Key.Date.Month, g.Key.Date.Day, g.Key.Hour, 0, 0),
                            Count = g.Count()
                        })
                        .OrderBy(x => x.DateTime)
                        .ToList();

                    var peakHour = hourlyCounts.OrderByDescending(x => x.Count).FirstOrDefault();

                    return Ok(new
                    {
                        QueryType = "InfluxRequests",
                        TimeUnit = "Hour",
                        Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                        HourlyData = hourlyCounts,
                        PeakHour = peakHour,
                        TotalRequests = hourlyCounts.Sum(x => x.Count),
                        Timestamp = DateTime.UtcNow
                    });
                }
                else if (timeUnit == "day")
                {
                    var allRequests = await _dbContext.ManageEngineRequests
                        .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo)
                        .Select(r => new { r.CreatedTime })
                        .ToListAsync();

                    var dailyCounts = allRequests
                        .GroupBy(r => r.CreatedTime.Date)
                        .Select(g => new
                        {
                            Date = g.Key,
                            Count = g.Count()
                        })
                        .OrderBy(x => x.Date)
                        .ToList();

                    var peakDay = dailyCounts.OrderByDescending(x => x.Count).FirstOrDefault();

                    return Ok(new
                    {
                        QueryType = "InfluxRequests",
                        TimeUnit = "Day",
                        Period = $"From {dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                        DailyData = dailyCounts,
                        PeakDay = peakDay,
                        TotalRequests = dailyCounts.Sum(x => x.Count),
                        Timestamp = DateTime.UtcNow
                    });
                }

                return BadRequest("Invalid time unit for influx query.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        private async Task<IActionResult> HandleTopRequestAreasQuery(QueryAnalysis analysis)
        {
            try
            {
                var dateFrom = ParseDateTime(analysis.DateFrom) ?? DateTimeOffset.UtcNow.AddDays(-30).DateTime;
                var dateTo = ParseDateTime(analysis.DateTo) ?? DateTimeOffset.UtcNow.DateTime;
                var topN = analysis.TopN ?? 10;

                var topAreas = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.Subject))
                    .GroupBy(r => r.Subject)
                    .Select(g => new
                    {
                        Subject = g.Key,
                        Count = g.Count()
                    })
                    .OrderByDescending(x => x.Count)
                    .Take(topN)
                    .ToListAsync();

                return Ok(new
                {
                    QueryType = "TopRequestAreas",
                    Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                    TopN = topN,
                    TopAreas = topAreas,
                    TotalAreas = topAreas.Count,
                    TotalRequests = topAreas.Sum(x => x.Count),
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        private async Task<IActionResult> HandleTopTechniciansQuery(QueryAnalysis analysis)
        {
            try
            {
                var dateFrom = ParseDateTime(analysis.DateFrom) ?? DateTimeOffset.UtcNow.AddDays(-30).DateTime;
                var dateTo = ParseDateTime(analysis.DateTo) ?? DateTimeOffset.UtcNow.DateTime;
                var topN = analysis.TopN ?? 10;

                var topTechs = await _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.TechnicianName))
                    .GroupBy(r => r.TechnicianName)
                    .Select(g => new
                    {
                        Technician = g.Key,
                        RequestsHandled = g.Count()
                    })
                    .OrderByDescending(x => x.RequestsHandled)
                    .Take(topN)
                    .ToListAsync();

                return Ok(new
                {
                    QueryType = "TopTechnicians",
                    Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                    TopN = topN,
                    TopTechnicians = topTechs,
                    TotalTechnicians = topTechs.Count,
                    TotalRequests = topTechs.Sum(x => x.RequestsHandled),
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        private async Task<IActionResult> HandleRequestSearchQuery(QueryAnalysis analysis, string userEmail = "", string userQuery = "")
        {
            try
            {
                var dateFrom = ParseDateTime(analysis.DateFrom) ?? DateTimeOffset.UtcNow.AddDays(-30).DateTime;
                var dateTo = ParseDateTime(analysis.DateTo) ?? DateTimeOffset.UtcNow.DateTime;

                var query = _dbContext.ManageEngineRequests
                    .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo);

                // If isUserRequest is true, filter by user's email in JSON data
                if (analysis.IsUserRequest && !string.IsNullOrEmpty(userEmail))
                {
                    query = query.Where(r => r.JsonData.Contains(userEmail));
                }
                // If specific technician requested, filter by technician name
                else if (!string.IsNullOrEmpty(analysis.Technician))
                {
                    query = query.Where(r => r.TechnicianName.ToLower().Contains(analysis.Technician.ToLower()));
                }

                // Filter by subject if specified
                if (!string.IsNullOrEmpty(analysis.Subject))
                {
                    query = query.Where(r => r.Subject.ToLower().Contains(analysis.Subject.ToLower()));
                }

                var requests = await query
                    .OrderByDescending(r => r.CreatedTime)
                    .Take(analysis.TopN ?? 50)
                    .ToListAsync();

                return Ok(new
                {
                    QueryType = "RequestSearch",
                    Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                    Subject = analysis.Subject,
                    Technician = analysis.Technician,
                    IsUserRequest = analysis.IsUserRequest,
                    RequestsFound = requests.Count,
                    Requests = requests.Select(r => new
                    {
                        r.Id,
                        r.Subject,
                        r.TechnicianName,
                        r.CreatedTime
                    }),
                    Timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = ex.Message });
            }
        }

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
                .ToListAsync();

            var newRequests = requests
                .Where(r => !existingIds.Contains(r.Id))
                .ToList();

            await _dbContext.ManageEngineRequests.AddRangeAsync(newRequests);
            await _dbContext.SaveChangesAsync();

            return Ok(new { Imported = newRequests.Count, Skipped = requests.Count - newRequests.Count });
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

        private (int days, int hours) ParseInactivityPeriod(string period)
        {
            if (string.IsNullOrEmpty(period))
                return (30, 0);

            var parts = period.ToLower().Split(' ');

            if (parts.Length < 2)
                return (30, 0);

            if (!int.TryParse(parts[0], out int value))
                return (30, 0);

            return parts[1] switch
            {
                "hours" or "hour" => (0, value),
                "days" or "day" => (value, 0),
                "weeks" or "week" => (value * 7, 0),
                "months" or "month" => (value * 30, 0),
                _ => (30, 0)
            };
        }

        private DateTime? ParseDateTime(string dateTimeStr)
        {
            if (string.IsNullOrEmpty(dateTimeStr))
                return null;

            if (DateTime.TryParse(dateTimeStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out var result))
                return result;

            return null;
        }

        private async Task<string> GenerateConversationalResponseAsync(object rawData, QueryAnalysis analysis, string userQuery, string conversationContext = "")
        {
            var dataJson = JsonSerializer.Serialize(rawData);
            var data = JsonSerializer.Deserialize<JsonElement>(dataJson);

            string previewText = analysis.QueryType switch
            {
                "inactive_technicians" => string.Join("\n", data.GetProperty("InactiveTechnicians").EnumerateArray().Take(10).Select(x => "• " + x.GetString())),

                "top_request_areas" => string.Join("\n", data.GetProperty("TopAreas").EnumerateArray().Take(10).Select(x => $"• {x.GetProperty("Subject").GetString()}: {x.GetProperty("Count").GetInt32()}")),

                "top_technicians" => string.Join("\n", data.GetProperty("TopTechnicians").EnumerateArray().Take(10).Select(x => $"• {x.GetProperty("Technician").GetString()}: {x.GetProperty("RequestsHandled").GetInt32()} requests")),

                "influx_requests" when data.TryGetProperty("HourlyData", out var h) => string.Join("\n", h.EnumerateArray().Take(10).Select(x => $"• {x.GetProperty("DateTime").GetDateTime():yyyy-MM-dd HH:00} → {x.GetProperty("Count").GetInt32()} requests")),

                "influx_requests" => string.Join("\n", data.GetProperty("DailyData").EnumerateArray().Take(10).Select(x => $"• {x.GetProperty("Date").GetDateTime():yyyy-MM-dd} → {x.GetProperty("Count").GetInt32()} requests")),

                "request_search" => string.Join("\n", data.GetProperty("Requests").EnumerateArray().Take(10).Select(r =>
                    $"• #{r.GetProperty("Id").GetString()} | {r.GetProperty("Subject").GetString()} | {r.GetProperty("TechnicianName").GetString() ?? "Unassigned"} | {r.GetProperty("CreatedTime").GetDateTime():yyyy-MM-dd HH:mm}")),

                _ => ""
            };

            string contextInfo = !string.IsNullOrEmpty(conversationContext) ? $"Based on our conversation:\n{conversationContext}\n" : "";

            string userRequestNote = analysis.IsUserRequest ? "Note: This is filtering for the user's own requests.\n" : "";

            string prompt = analysis.QueryType switch
            {
                "inactive_technicians" => $"""
                You are a friendly IT service desk assistant helping manage technician performance.
                {contextInfo}
                {userRequestNote}
                User asked: "{userQuery}"

                Inactive technicians (first 10 shown):
                {previewText}

                Total inactive: {data.GetProperty("TotalInactive").GetInt32()} out of {data.GetProperty("TotalTechnicians").GetInt32()}
                Period: {data.GetProperty("Period").GetString()}

                Respond in 2–4 warm, natural sentences. Be helpful and professional. Always explicitly list the first 10 inactive technicians in your response using bullet points.
                """,

                "influx_requests" => $"""
                You are a helpful IT analytics assistant tracking request patterns.
                {contextInfo}
                {userRequestNote}
                User asked: "{userQuery}"

                Request influx ({(data.TryGetProperty("TimeUnit", out var tu) ? tu.GetString() : "period")} view):
                {previewText}

                Peak: {(data.TryGetProperty("PeakHour", out var ph) ? $"{ph.GetProperty("DateTime").GetDateTime():yyyy-MM-dd HH:00} ({ph.GetProperty("Count").GetInt32()} requests)" :
                          data.TryGetProperty("PeakDay", out var pd) ? $"{pd.GetProperty("Date").GetDateTime():yyyy-MM-dd} ({pd.GetProperty("Count").GetInt32()} requests)" : "N/A")}

                Total requests: {data.GetProperty("TotalRequests").GetInt32()}

                Give a short, conversational summary highlighting the busiest time and patterns. Always explicitly list the first 10 items in your response using bullet points.
                """,

                "top_request_areas" => $"""
                You are a helpful IT service desk analyst.
                {contextInfo}
                {userRequestNote}
                User asked: "{userQuery}"

                Top request categories (top 10 shown):
                {previewText}

                Respond naturally: mention the #1 area and maybe #2–3. Sound helpful and insightful about what we're dealing with. Always explicitly list the first 10 categories in your response using bullet points.
                """,

                "top_technicians" => $"""
                You are a supportive IT team manager recognizing top performers.
                {contextInfo}
                {userRequestNote}
                User asked: "{userQuery}"

                Top performing technicians (top 10 shown):
                {previewText}

                Respond in a congratulatory, friendly tone. Shout out the top 1–3 performers and thank the team for their hard work. Always explicitly list the first 10 technicians in your response using bullet points.
                """,

                "request_search" => $"""
                You are a helpful IT service desk assistant.
                {contextInfo}
                {userRequestNote}
                User searched: "{userQuery}"

                Found {data.GetProperty("RequestsFound").GetInt32()} matching requests. First 10:
                {previewText}

                Respond naturally: confirm the search, highlight anything interesting, and mention the full list is available in Excel. Always explicitly list the first 10 requests in your response using bullet points.
                """,

                _ => "Here's the information you requested!"
            };

            return await CallMeAiForTextAsync(prompt);
        }

        private async Task<string> CallMeAiForTextAsync(string prompt)
        {
            var client = _httpClientFactory.CreateClient();
            var url = $"{_meAiEndpoint}openai/deployments/{_meAiDeploymentName}/chat/completions?api-version={_meAiApiVersion}";

            var body = new
            {
                messages = new[] { new { role = "user", content = prompt } },
                max_tokens = 500,
                temperature = 0.7
            };

            var content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
            client.DefaultRequestHeaders.Add("api-key", _meAiApiKey);

            var response = await client.PostAsync(url, content);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<AiResponse>(json);

            return result?.Choices?[0]?.Message?.Content?.Trim() ?? "Here's your data!";
        }

        private byte[] GenerateCsvFromData(JsonElement data)
        {
            var sb = new StringBuilder();

            string Safe(object? value) => value?.ToString()?.Replace("\"", "\"\"") ?? "";

            if (data.TryGetProperty("QueryType", out var qt))
            {
                var type = qt.GetString();

                switch (type)
                {
                    case "InactiveTechnicians":
                        sb.AppendLine("Inactive Technician");
                        foreach (var tech in data.GetProperty("InactiveTechnicians").EnumerateArray())
                            sb.AppendLine($"\"{Safe(tech.GetString())}\"");
                        break;

                    case "TopRequestAreas":
                        sb.AppendLine("Subject,Count");
                        foreach (var item in data.GetProperty("TopAreas").EnumerateArray())
                            sb.AppendLine($"\"{Safe(item.GetProperty("Subject").GetString())}\",\"{item.GetProperty("Count").GetInt32()}\"");
                        break;

                    case "TopTechnicians":
                        sb.AppendLine("Technician,Requests Handled");
                        foreach (var item in data.GetProperty("TopTechnicians").EnumerateArray())
                            sb.AppendLine($"\"{Safe(item.GetProperty("Technician").GetString())}\",\"{item.GetProperty("RequestsHandled").GetInt32()}\"");
                        break;

                    case "InfluxRequests":
                        if (data.TryGetProperty("HourlyData", out var hourly))
                        {
                            sb.AppendLine("DateTime,Count");
                            foreach (var item in hourly.EnumerateArray())
                                sb.AppendLine($"\"{item.GetProperty("DateTime").GetDateTime():yyyy-MM-dd HH:00}\",\"{item.GetProperty("Count").GetInt32()}\"");
                        }
                        else
                        {
                            sb.AppendLine("Date,Count");
                            foreach (var item in data.GetProperty("DailyData").EnumerateArray())
                                sb.AppendLine($"\"{item.GetProperty("Date").GetDateTime():yyyy-MM-dd}\",\"{item.GetProperty("Count").GetInt32()}\"");
                        }
                        break;

                    case "RequestSearch":
                        sb.AppendLine("ID,Subject,Technician,Created Time");
                        foreach (var r in data.GetProperty("Requests").EnumerateArray())
                        {
                            var tech = r.TryGetProperty("TechnicianName", out var t) ? t.GetString() : "";
                            sb.AppendLine($"\"{r.GetProperty("Id").GetString()}\",\"{Safe(r.GetProperty("Subject").GetString())}\",\"{Safe(tech)}\",\"{r.GetProperty("CreatedTime").GetDateTime():yyyy-MM-dd HH:mm}\"");
                        }
                        break;

                    default:
                        sb.AppendLine("No data available");
                        break;
                }
            }

            return Encoding.UTF8.GetBytes(sb.ToString());
        }
    }

    // Helper Classes

    public class NaturalQueryRequest
    {
        public string Query { get; set; }
        public string? SessionId { get; set; }
        public string? UserEmail { get; set; }
    }

    public class QueryAnalysis
    {
        [JsonPropertyName("queryType")]
        public string QueryType { get; set; }

        [JsonPropertyName("dateFrom")]
        public string DateFrom { get; set; }

        [JsonPropertyName("dateTo")]
        public string DateTo { get; set; }

        [JsonPropertyName("timeUnit")]
        public string TimeUnit { get; set; }

        [JsonPropertyName("topN")]
        public int? TopN { get; set; }

        [JsonPropertyName("subject")]
        public string Subject { get; set; }

        [JsonPropertyName("technician")]
        public string Technician { get; set; }

        [JsonPropertyName("inactivityPeriod")]
        public string InactivityPeriod { get; set; }

        [JsonPropertyName("isUserRequest")]
        public bool IsUserRequest { get; set; }
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