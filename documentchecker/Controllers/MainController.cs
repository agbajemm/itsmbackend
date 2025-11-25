using Azure;
using Azure.AI.Agents.Persistent;
using Azure.Identity;
using CsvHelper;
using documentchecker.Models;
using documentchecker.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace documentchecker.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MainController : ControllerBase
    {
        private readonly string _projectEndpoint;
        private readonly string _queryAnalysisAgentId;
        private readonly string _conversationAgentId;
        private readonly string _dataRetrievalAgentId;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IMemoryCache _cache;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly string _refreshToken;
        private readonly string _redirectUri;
        private readonly RequestStorageService _requestStorageService;
        private readonly AppDbContext _dbContext;
        private readonly IServiceProvider _serviceProvider;

        public MainController(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            IMemoryCache cache,
            RequestStorageService requestStorageService,
            AppDbContext dbContext,
            IServiceProvider serviceProvider)
        {
            _projectEndpoint = configuration["AzureAIFoundry:ProjectEndpoint"] ?? throw new InvalidOperationException("ProjectEndpoint not configured.");
            _queryAnalysisAgentId = configuration["AzureAIFoundry:QueryAnalysisAgentId"] ?? throw new InvalidOperationException("QueryAnalysisAgentId not configured.");
            _conversationAgentId = configuration["AzureAIFoundry:ConversationAgentId"] ?? throw new InvalidOperationException("ConversationAgentId not configured.");
            _dataRetrievalAgentId = configuration["AzureAIFoundry:DataRetrievalAgentId"] ?? throw new InvalidOperationException("DataRetrievalAgentId not configured.");

            _httpClientFactory = httpClientFactory;
            _cache = cache;
            _clientId = configuration["Zoho:ClientId"] ?? throw new InvalidOperationException("Zoho:ClientId not configured.");
            _clientSecret = configuration["Zoho:ClientSecret"] ?? throw new InvalidOperationException("Zoho:ClientSecret not configured.");
            _refreshToken = configuration["Zoho:RefreshToken"] ?? throw new InvalidOperationException("Zoho:RefreshToken not configured.");
            _redirectUri = configuration["Zoho:RedirectUri"] ?? throw new InvalidOperationException("Zoho:RedirectUri not configured.");
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
                DateTimeOffset dateFrom = lastStoredDate.HasValue
                    ? lastStoredDate.Value.AddMinutes(-5)
                    : DateTimeOffset.UtcNow.AddMonths(-1);

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

            try
            {
                // Background sync
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

                // Initialize Azure AI Agents Client
                var agentsClient = new PersistentAgentsClient(_projectEndpoint, new DefaultAzureCredential());

                // Get or create conversation with thread management
                var conversation = await _dbContext.ChatConversations
                    .Include(c => c.Messages)
                    .FirstOrDefaultAsync(c => c.SessionId == sessionId);

                if (conversation == null)
                {
                    conversation = new ChatConversation
                    {
                        SessionId = sessionId,
                        UserEmail = request.UserEmail
                    };
                    _dbContext.ChatConversations.Add(conversation);
                    await _dbContext.SaveChangesAsync();
                }
                else if (string.IsNullOrEmpty(conversation.UserEmail) && !string.IsNullOrEmpty(request.UserEmail))
                {
                    conversation.UserEmail = request.UserEmail;
                }

                // Thread ID = Session ID for conversation continuity
                string threadId;
                if (string.IsNullOrEmpty(conversation.ThreadId))
                {
                    // Create new thread in Azure AI Foundry
                    var threadResponse = await agentsClient.Threads.CreateThreadAsync();
                    threadId = threadResponse.Value.Id;
                    conversation.ThreadId = threadId;
                    await _dbContext.SaveChangesAsync();
                }
                else
                {
                    threadId = conversation.ThreadId;
                }

                // Add user message to conversation history
                var userMessageEntity = new ChatMessage { Role = "user", Content = request.Query };
                conversation.Messages.Add(userMessageEntity);
                await _dbContext.SaveChangesAsync();

                // Build conversation context from previous messages
                var conversationContext = BuildConversationContext(conversation);

                // STEP 1: Query Analysis Agent - Extract structured parameters
                var queryAnalysis = await AnalyzeQueryWithAgent(agentsClient, threadId, request.Query, conversationContext, request.UserEmail);
                Console.WriteLine($"Query Analysis: {JsonSerializer.Serialize(queryAnalysis)}");

                // STEP 2: Retrieve data from database based on analysis
                var retrievedData = await RetrieveDataBasedOnAnalysis(queryAnalysis, request.UserEmail);

                // STEP 3: Data Retrieval Agent - Optional refinement
                var dataSummary = GenerateDataSummary(retrievedData, queryAnalysis);
                var refinedAnalysis = await RefineQueryWithDataRetrievalAgent(agentsClient, threadId, request.Query, queryAnalysis, dataSummary, conversationContext);

                // If refinement produced a better analysis, use it and re-query data
                if (refinedAnalysis != null && refinedAnalysis.QueryType != queryAnalysis.QueryType)
                {
                    queryAnalysis = refinedAnalysis;
                    retrievedData = await RetrieveDataBasedOnAnalysis(queryAnalysis, request.UserEmail);
                    dataSummary = GenerateDataSummary(retrievedData, queryAnalysis);
                }

                // STEP 4: Conversation Agent - Generate friendly response
                var conversationalResponse = await GenerateConversationalResponseWithAgent(
                    agentsClient,
                    threadId,
                    request.Query,
                    retrievedData,
                    queryAnalysis,
                    conversationContext
                );

                // Generate CSV file reference
                var fileName = $"queryresult_{DateTime.UtcNow:yyyyMMddHHmmss}.csv";
                var url = $"/api/Main/download-result/{sessionId}/{fileName}";

                var finalResponseFull = new
                {
                    SessionId = sessionId,
                    ThreadId = threadId,
                    ConversationalResponse = conversationalResponse,
                    ExcelFile = new
                    {
                        FileName = fileName,
                        Url = url
                    },
                    Data = retrievedData,
                    QueryAnalysis = queryAnalysis
                };

                var agentMessage = new ChatMessage
                {
                    Role = "agent",
                    Content = JsonSerializer.Serialize(finalResponseFull)
                };
                conversation.Messages.Add(agentMessage);
                await _dbContext.SaveChangesAsync();

                return Ok(new
                {
                    SessionId = sessionId,
                    ThreadId = threadId,
                    ConversationalResponse = conversationalResponse,
                    ExcelFile = finalResponseFull.ExcelFile,
                    Summary = ExtractSummaryFromData(retrievedData, queryAnalysis)
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}\n{ex.StackTrace}");
                return StatusCode(500, new
                {
                    Error = ex.Message,
                    ConversationalResponse = "I apologize, but I encountered an issue processing your request. Could you please rephrase your question?"
                });
            }
        }

        private string BuildConversationContext(ChatConversation conversation)
        {
            if (conversation?.Messages == null || conversation.Messages.Count < 2)
                return string.Empty;

            // Take more messages for better context (up to 10)
            var recentMessages = conversation.Messages
                .OrderByDescending(m => m.SentAt)
                .Take(10)
                .OrderBy(m => m.SentAt)
                .ToList();

            if (recentMessages.Count == 0)
                return string.Empty;

            var sb = new StringBuilder();
            sb.AppendLine("Recent conversation history:");

            foreach (var msg in recentMessages)
            {
                var role = msg.Role == "user" ? "User" : "Assistant";
                var content = msg.Content;

                // For agent messages, extract conversational response from JSON
                if (role == "Assistant" && content.StartsWith("{"))
                {
                    try
                    {
                        var doc = JsonDocument.Parse(content);
                        if (doc.RootElement.TryGetProperty("ConversationalResponse", out var resp))
                        {
                            content = resp.GetString() ?? content;
                        }
                        // Also try to extract key data points for better context
                        if (doc.RootElement.TryGetProperty("Data", out var dataElem))
                        {
                            if (dataElem.TryGetProperty("InactiveTechnicians", out var inactiveTechs))
                            {
                                var names = inactiveTechs.EnumerateArray()
                                    .Take(5)
                                    .Select(t => t.GetProperty("Technician").GetString())
                                    .Where(n => !string.IsNullOrEmpty(n));
                                content += $"\n[Mentioned technicians: {string.Join(", ", names)}]";
                            }
                            else if (dataElem.TryGetProperty("TopTechnicians", out var topTechs))
                            {
                                var names = topTechs.EnumerateArray()
                                    .Take(5)
                                    .Select(t => t.GetProperty("Technician").GetString())
                                    .Where(n => !string.IsNullOrEmpty(n));
                                content += $"\n[Mentioned technicians: {string.Join(", ", names)}]";
                            }
                        }
                    }
                    catch { }
                }

                if (content.Length > 400)
                    content = content.Substring(0, 400) + "...";

                sb.AppendLine($"{role}: {content}");
            }

            sb.AppendLine("\nUse this context to understand references like 'them', 'those', 'the technicians', previous queries, and maintain conversational continuity.");
            return sb.ToString();
        }

        private async Task<QueryAnalysis> AnalyzeQueryWithAgent(
            PersistentAgentsClient client,
            string threadId,
            string userQuery,
            string conversationContext,
            string userEmail = "")
        {
            var currentDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
            var currentTime = DateTime.UtcNow.ToString("HH:mm");

            // Enhanced agent instructions with comprehensive examples
            var instructions = @"You are a query analysis agent for an IT service desk system. 

Your task: Analyze the user query and return ONLY a valid JSON object with NO explanations or markdown.

Schema:
{
  ""queryType"": ""inactive_technicians|influx_requests|top_request_areas|top_technicians|request_search"",
  ""dateFrom"": ""yyyy-MM-dd HH:mm or null"",
  ""dateTo"": ""yyyy-MM-dd HH:mm or null"",
  ""timeUnit"": ""hour|day or null"",
  ""topN"": number or null,
  ""subject"": ""string or null"",
  ""technician"": ""string or null"",
  ""technicians"": [""array or null""],
  ""requester"": ""string or null"",
  ""inactivityPeriod"": ""string or null (e.g., '14 days', '2 weeks')"",
  ""isUserRequest"": boolean,
  ""isUserTechnician"": boolean,
  ""status"": ""open|closed|null""
}

Query Types:
1. inactive_technicians - technicians with no activity in period
2. influx_requests - request volume by hour/day
3. top_request_areas - most common request subjects
4. top_technicians - technician performance ranking
5. request_search - search with filters

Time Parsing Rules:
- 'today' → current date 00:00 to 23:59
- 'yesterday' → previous date 00:00 to 23:59
- 'past 2 weeks' or '2 weeks' → 14 days back to now
- 'past X days' → X days back to now
- 'this week' → 7 days back
- 'this month' → 1st of month to today
- 'last month' → previous month full range

Default Ranges (when not specified):
- inactive_technicians: 30 days
- influx_requests: 7 days
- top_request_areas: 30 days
- top_technicians: 30 days
- request_search: 30 days

Status Filtering:
- 'open' includes: open, in progress, pending
- 'closed' includes: closed, resolved, completed
- Extract from keywords: 'open requests', 'closed tickets', 'pending', 'resolved'

Personalization Rules:
- 'my requests' / 'tickets I opened' / 'requests from me' → isUserRequest: true
- 'assigned to me' / 'my assigned tickets' / 'tickets treated by me' → isUserTechnician: true
- Specific names in query → extract to 'technician' or 'requester' fields
- List from context (e.g., 'them', 'those') → use 'technicians' array

Conversation Context:
- If user refers to 'them', 'those', 'the technicians', extract names from context into technicians array
- Maintain continuity with previous queries and filters
- For follow-up questions, preserve previous parameters

Examples:
Query: show me my open requests
Response: {""queryType"": ""request_search"", ""dateFrom"": """ + DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd") + @" 00:00"", ""dateTo"": """ + currentDate + @" 23:59"", ""isUserRequest"": true, ""status"": ""open"", ""timeUnit"": null, ""topN"": null, ""subject"": null, ""technician"": null, ""technicians"": null, ""requester"": null, ""inactivityPeriod"": null, ""isUserTechnician"": false}

Query: technicians inactive for 2 weeks
Response: {""queryType"": ""inactive_technicians"", ""inactivityPeriod"": ""14 days"", ""dateFrom"": null, ""dateTo"": null, ""timeUnit"": null, ""topN"": null, ""subject"": null, ""technician"": null, ""technicians"": null, ""requester"": null, ""isUserRequest"": false, ""isUserTechnician"": false, ""status"": null}

Query: top 10 technicians this month
Response: {""queryType"": ""top_technicians"", ""dateFrom"": """ + new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, 1).ToString("yyyy-MM-dd") + @" 00:00"", ""dateTo"": """ + currentDate + @" 23:59"", ""topN"": 10, ""timeUnit"": null, ""subject"": null, ""technician"": null, ""technicians"": null, ""requester"": null, ""inactivityPeriod"": null, ""isUserRequest"": false, ""isUserTechnician"": false, ""status"": null}

Query: show influx of requests today by hour
Response: {""queryType"": ""influx_requests"", ""dateFrom"": """ + currentDate + @" 00:00"", ""dateTo"": """ + currentDate + @" 23:59"", ""timeUnit"": ""hour"", ""topN"": null, ""subject"": null, ""technician"": null, ""technicians"": null, ""requester"": null, ""inactivityPeriod"": null, ""isUserRequest"": false, ""isUserTechnician"": false, ""status"": null}

Query: password reset requests from John
Response: {""queryType"": ""request_search"", ""dateFrom"": """ + DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd") + @" 00:00"", ""dateTo"": """ + currentDate + @" 23:59"", ""subject"": ""password reset"", ""requester"": ""John"", ""timeUnit"": null, ""topN"": null, ""technician"": null, ""technicians"": null, ""inactivityPeriod"": null, ""isUserRequest"": false, ""isUserTechnician"": false, ""status"": null}

Output ONLY the JSON object. No markdown code blocks, no explanations.";

            var prompt = $@"Current Date: {currentDate}
Current Time: {currentTime}
User Email: {(string.IsNullOrEmpty(userEmail) ? "Unknown" : userEmail)}

{(!string.IsNullOrEmpty(conversationContext) ? $"CONVERSATION CONTEXT:\n{conversationContext}\n" : "")}

USER QUERY: {userQuery}";

            // Call agent via API
            var responses = await RunAgentAsync(client, threadId, _queryAnalysisAgentId, prompt, instructions);

            // Parse response
            for (int i = responses.Count - 1; i >= 0; i--)
            {
                var response = responses[i].Trim();

                // Remove markdown code blocks if present
                if (response.StartsWith("```json"))
                    response = response.Substring(7);
                else if (response.StartsWith("```"))
                    response = response.Substring(3);
                if (response.EndsWith("```"))
                    response = response.Substring(0, response.Length - 3);
                response = response.Trim();

                if (response.StartsWith("{") && response.EndsWith("}"))
                {
                    try
                    {
                        var analysis = JsonSerializer.Deserialize<QueryAnalysis>(response, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                        if (analysis != null && !string.IsNullOrEmpty(analysis.QueryType))
                        {
                            // Apply defaults if dates are missing
                            if (string.IsNullOrEmpty(analysis.DateFrom) || string.IsNullOrEmpty(analysis.DateTo))
                            {
                                var defaultDays = analysis.QueryType switch
                                {
                                    "inactive_technicians" => 30,
                                    "influx_requests" => 7,
                                    "top_request_areas" => 30,
                                    "top_technicians" => 30,
                                    _ => 30
                                };
                                var now = DateTime.UtcNow;
                                analysis.DateTo = now.ToString("yyyy-MM-dd HH:mm");
                                analysis.DateFrom = now.AddDays(-defaultDays).ToString("yyyy-MM-dd HH:mm");
                            }

                            if (analysis.QueryType == "influx_requests" && string.IsNullOrEmpty(analysis.TimeUnit))
                                analysis.TimeUnit = "hour";

                            return analysis;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to parse analysis: {ex.Message}");
                    }
                }
            }

            // Fallback
            return await FallbackHeuristicAnalysis(userQuery, userEmail);
        }

        private async Task<QueryAnalysis?> RefineQueryWithDataRetrievalAgent(
            PersistentAgentsClient client,
            string threadId,
            string userQuery,
            QueryAnalysis currentAnalysis,
            string dataSummary,
            string conversationContext)
        {
            var instructions = @"You are a data retrieval refinement agent.

Review the query analysis and data summary. If the analysis seems correct, respond with: {""status"":""ok""}

If you detect issues (wrong query type, missing filters, incorrect dates), respond with an IMPROVED JSON analysis using the same schema as the original.

Only respond with JSON. No explanations.";

            var prompt = $@"Original Query: {userQuery}

Current Analysis:
{JsonSerializer.Serialize(currentAnalysis)}

Data Summary Retrieved:
{dataSummary}

{(!string.IsNullOrEmpty(conversationContext) ? $"Context:\n{conversationContext}" : "")}

Is this analysis correct? If not, provide improved JSON.";

            var responses = await RunAgentAsync(client, threadId, _dataRetrievalAgentId, prompt, instructions);

            foreach (var response in responses)
            {
                var cleaned = response.Trim();
                if (cleaned.StartsWith("```json"))
                    cleaned = cleaned.Substring(7);
                else if (cleaned.StartsWith("```"))
                    cleaned = cleaned.Substring(3);
                if (cleaned.EndsWith("```"))
                    cleaned = cleaned.Substring(0, cleaned.Length - 3);
                cleaned = cleaned.Trim();

                if (cleaned.Contains("\"status\"") && cleaned.Contains("\"ok\""))
                    return null; // Analysis is fine

                if (cleaned.StartsWith("{") && cleaned.EndsWith("}"))
                {
                    try
                    {
                        var refined = JsonSerializer.Deserialize<QueryAnalysis>(cleaned, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                        if (refined != null && !string.IsNullOrEmpty(refined.QueryType))
                            return refined;
                    }
                    catch { }
                }
            }

            return null;
        }

        private async Task<string> GenerateConversationalResponseWithAgent(
            PersistentAgentsClient client,
            string threadId,
            string userQuery,
            object data,
            QueryAnalysis analysis,
            string conversationContext)
        {
            var dataSummary = GenerateDataSummary(data, analysis);

            var instructions = @"You are a friendly IT service desk assistant.

Generate a warm, professional conversational response (2-5 paragraphs) that:
1. Acknowledges the user's query naturally
2. Highlights key findings with bullet points (top 3-10 items)
3. Provides insights and patterns in the data
4. Mentions the downloadable CSV file for full details
5. If this is a follow-up (based on context), acknowledge continuity
6. Uses encouraging and positive tone

IMPORTANT:
- Always explicitly list top items using bullet points (•)
- Be specific with numbers and names
- Sound conversational, not robotic
- Celebrate successes when appropriate
- Provide actionable insights

NO markdown code blocks. Plain text with bullet points only.";

            var userRequestNote = analysis.IsUserRequest ? "\n[Note: Results filtered to show only user's submitted requests]" :
                analysis.IsUserTechnician ? "\n[Note: Results filtered to show only requests assigned to user as technician]" : "";

            var statusNote = !string.IsNullOrEmpty(analysis.Status) ? 
                $"\n[Status filter: {analysis.Status} requests only]" : "";

            var prompt = $"User Query: \"{userQuery}\"\n" +
                         $"Query Type: {analysis.QueryType}\n" +
                         $"Period: {analysis.DateFrom ?? "N/A"} to {analysis.DateTo ?? "N/A"}\n" +
                         statusNote +
                         userRequestNote +
                         (string.IsNullOrEmpty(conversationContext) ? string.Empty : $"\nConversation Context:\n{conversationContext}\n") +
                         $"\nDATA SUMMARY:\n{dataSummary}\n\nGenerate friendly, conversational response now:";

            var responses = await RunAgentAsync(client, threadId, _conversationAgentId, prompt, instructions);
            var last = responses.LastOrDefault() ?? string.Empty;

            // Enhanced fallback if agent run failed
            if (string.IsNullOrEmpty(last) || last.StartsWith("Agent failed:") || last.StartsWith("Error:"))
            {
                var failureInfo = last;
                var bullets = BuildFallbackBulletsFromSummary(dataSummary);
                var fallback = new StringBuilder();
                
                fallback.AppendLine($"I processed your query successfully! Here's what I found:");
                if (!string.IsNullOrWhiteSpace(failureInfo) && !failureInfo.StartsWith("Agent failed:"))
                {
                    fallback.AppendLine($"(Note: {failureInfo.Replace("Error:", string.Empty).Trim()})");
                }
                fallback.AppendLine();
                fallback.AppendLine("Key findings:");
                foreach (var b in bullets)
                {
                    fallback.AppendLine($"• {b}");
                }
                fallback.AppendLine();
                fallback.AppendLine("You can download the full CSV file for detailed analysis. Let me know if you'd like to filter or refine these results further!");
                return fallback.ToString();
            }

            return last;
        }

        private List<string> BuildFallbackBulletsFromSummary(string summary)
        {
            var lines = summary.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var bullets = new List<string>();
            foreach (var line in lines)
            {
                var trimmed = line.Trim();
                if (trimmed.StartsWith("•"))
                {
                    bullets.Add(trimmed.TrimStart('•', ' '));
                }
                else if (char.IsDigit(trimmed.FirstOrDefault()) && trimmed.Contains('.'))
                {
                    // ranked line like "1. Technician X - 5 requests"
                    bullets.Add(trimmed); 
                }
                else if (trimmed.StartsWith("Found "))
                {
                    bullets.Add(trimmed); 
                }
            }
            // Ensure at least one bullet
            if (bullets.Count == 0 && !string.IsNullOrWhiteSpace(summary))
            {
                bullets.Add(summary.Length > 180 ? summary[..180] + "..." : summary);
            }
            return bullets.Take(10).ToList();
        }

        private async Task<QueryAnalysis> FallbackHeuristicAnalysis(string userQuery, string userEmail = "")
        {
            await Task.Yield();

            var query = userQuery.ToLowerInvariant();
            var analysis = new QueryAnalysis
            {
                QueryType = "request_search",
                IsUserRequest = false,
                IsUserTechnician = false,
                Technicians = new List<string>()
            };

            // Determine query type
            if (query.Contains("inactive") || query.Contains("no activity"))
                analysis.QueryType = "inactive_technicians";
            else if (query.Contains("influx") || query.Contains("volume") || query.Contains("per hour"))
                analysis.QueryType = "influx_requests";
            else if (query.Contains("top tech") || query.Contains("ranking"))
                analysis.QueryType = "top_technicians";
            else if (query.Contains("top request") || query.Contains("common request") || query.Contains("top subject"))
                analysis.QueryType = "top_request_areas";

            // Status
            if (query.Contains("open"))
                analysis.Status = "open";
            else if (query.Contains("closed") || query.Contains("resolved"))
                analysis.Status = "closed";

            // Personalization
            if (query.Contains("my requests"))
                analysis.IsUserRequest = true;
            if (query.Contains("assigned to me"))
                analysis.IsUserTechnician = true;

            // Dates
            var now = DateTime.UtcNow;
            var defaultDays = analysis.QueryType switch
            {
                "inactive_technicians" => 30,
                "influx_requests" => 7,
                "top_request_areas" => 30,
                "top_technicians" => 30,
                _ => 30
            };
            analysis.DateTo = now.ToString("yyyy-MM-dd HH:mm");
            analysis.DateFrom = now.AddDays(-defaultDays).ToString("yyyy-MM-dd HH:mm");

            if (analysis.QueryType == "influx_requests")
                analysis.TimeUnit = query.Contains("hour") ? "hour" : "day";

            return analysis;
        }

        private async Task<object> RetrieveDataBasedOnAnalysis(QueryAnalysis analysis, string userEmail = "")
        {
            return analysis.QueryType switch
            {
                "inactive_technicians" => await GetInactiveTechniciansData(analysis),
                "influx_requests" => await GetInfluxRequestsData(analysis),
                "top_request_areas" => await GetTopRequestAreasData(analysis),
                "top_technicians" => await GetTopTechniciansData(analysis),
                "request_search" => await GetRequestSearchData(analysis, userEmail),
                _ => throw new Exception($"Unknown query type: {analysis.QueryType}")
            };
        }

        // Data retrieval methods remain the same as in your document 1
        // I'll include the key ones here with enhanced JSON parsing

        private async Task<object> GetInactiveTechniciansData(QueryAnalysis analysis)
        {
            var (daysInactive, _) = ParseInactivityPeriod(analysis.InactivityPeriod);
            var dateTo = DateTimeOffset.UtcNow;
            var dateFrom = dateTo.AddDays(-daysInactive);

            var query = _dbContext.ManageEngineRequests
                .Where(r => !string.IsNullOrEmpty(r.TechnicianName));

            if (analysis.Technicians?.Any() ?? false)
            {
                query = query.Where(r => analysis.Technicians.Contains(r.TechnicianName, StringComparer.OrdinalIgnoreCase));
            }

            if (!string.IsNullOrEmpty(analysis.Technician))
            {
                query = query.Where(r => r.TechnicianName.ToLower().Contains(analysis.Technician.ToLower()));
            }

            var allTechActivity = await query
                .GroupBy(r => r.TechnicianName)
                .Select(g => new
                {
                    Technician = g.Key,
                    TechnicianEmail = g.Select(r => r.TechnicianEmail).FirstOrDefault(e => !string.IsNullOrEmpty(e)),
                    LastActivity = g.Max(r => r.CreatedTime),
                    TotalRequests = g.Count(),
                    AllRequests = g.OrderByDescending(r => r.CreatedTime).Select(r => new
                    {
                        r.Id,
                        r.DisplayId,
                        r.Subject,
                        r.Status,
                        r.CreatedTime,
                        r.RequesterName,
                        r.RequesterEmail,
                        r.JsonData
                    }).ToList()
                })
                .ToListAsync();

            var inactive = allTechActivity
                .Where(a => a.LastActivity < dateFrom)
                .Select(a => new
                {
                    a.Technician,
                    a.TechnicianEmail,
                    a.LastActivity,
                    a.TotalRequests,
                    DaysInactive = (int)(dateTo - a.LastActivity).TotalDays,
                    Requests = a.AllRequests.Select(r => ParseRequestDetails(r.JsonData, r)).ToList()
                })
                .ToList();

            return new
            {
                QueryType = "InactiveTechnicians",
                InactivityPeriod = analysis.InactivityPeriod,
                Period = $"From {dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                InactiveTechnicians = inactive,
                TotalInactive = inactive.Count,
                TotalTechnicians = allTechActivity.Count,
                Timestamp = DateTime.UtcNow
            };
        }

        private async Task<object> GetInfluxRequestsData(QueryAnalysis analysis)
        {
            var dateFrom = ParseDateTime(analysis.DateFrom) ?? DateTimeOffset.UtcNow.AddDays(-7).DateTime;
            var dateTo = ParseDateTime(analysis.DateTo) ?? DateTimeOffset.UtcNow.DateTime;
            var timeUnit = analysis.TimeUnit?.ToLower() ?? "hour";

            var query = _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo);

            if (analysis.Technicians?.Any() ?? false)
            {
                query = query.Where(r => analysis.Technicians.Contains(r.TechnicianName, StringComparer.OrdinalIgnoreCase));
            }

            if (!string.IsNullOrEmpty(analysis.Technician))
            {
                query = query.Where(r => r.TechnicianName.ToLower().Contains(analysis.Technician.ToLower()));
            }

            if (!string.IsNullOrEmpty(analysis.Requester))
            {
                query = query.Where(r => r.RequesterName.ToLower().Contains(analysis.Requester.ToLower()));
            }

            var allRequests = await query
                .Select(r => new { r.CreatedTime, r.Id, r.DisplayId, r.Subject, r.Status, r.TechnicianName, r.RequesterName, r.JsonData })
                .ToListAsync();

            if (timeUnit == "hour")
            {
                var hourlyCounts = allRequests
                    .GroupBy(r => new { Date = r.CreatedTime.Date, Hour = r.CreatedTime.Hour })
                    .Select(g => new
                    {
                        // Fixed: properly construct DateTime from Date and Hour
                        DateTime = new DateTime(g.Key.Date.Year, g.Key.Date.Month, g.Key.Date.Day, g.Key.Hour, 0, 0),
                        Count = g.Count(),
                        Requests = g.Select(r => ParseRequestDetails(r.JsonData, r)).ToList()
                    })
                    .OrderBy(x => x.DateTime)
                    .ToList();

                var peakHour = hourlyCounts.OrderByDescending(x => x.Count).FirstOrDefault();

                return new
                {
                    QueryType = "InfluxRequests",
                    TimeUnit = "Hour",
                    Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                    HourlyData = hourlyCounts,
                    PeakHour = peakHour,
                    TotalRequests = hourlyCounts.Sum(x => x.Count),
                    Timestamp = DateTime.UtcNow
                };
            }
            else
            {
                var dailyCounts = allRequests
                    .GroupBy(r => r.CreatedTime.Date)
                    .Select(g => new
                    {
                        Date = g.Key,
                        Count = g.Count(),
                        Requests = g.Select(r => ParseRequestDetails(r.JsonData, r)).ToList()
                    })
                    .OrderBy(x => x.Date)
                    .ToList();

                var peakDay = dailyCounts.OrderByDescending(x => x.Count).FirstOrDefault();

                return new
                {
                    QueryType = "InfluxRequests",
                    TimeUnit = "Day",
                    Period = $"From {dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                    DailyData = dailyCounts,
                    PeakDay = peakDay,
                    TotalRequests = dailyCounts.Sum(x => x.Count),
                    Timestamp = DateTime.UtcNow
                };
            }
        }

        private async Task<object> GetTopRequestAreasData(QueryAnalysis analysis)
        {
            var dateFrom = ParseDateTime(analysis.DateFrom) ?? DateTimeOffset.UtcNow.AddDays(-30).DateTime;
            var dateTo = ParseDateTime(analysis.DateTo) ?? DateTimeOffset.UtcNow.DateTime;
            var topN = analysis.TopN ?? 10;

            var query = _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.Subject));

            if (analysis.Technicians?.Any() ?? false)
            {
                query = query.Where(r => analysis.Technicians.Contains(r.TechnicianName, StringComparer.OrdinalIgnoreCase));
            }

            if (!string.IsNullOrEmpty(analysis.Technician))
            {
                query = query.Where(r => r.TechnicianName.ToLower().Contains(analysis.Technician.ToLower()));
            }

            if (!string.IsNullOrEmpty(analysis.Requester))
            {
                query = query.Where(r => r.RequesterName.ToLower().Contains(analysis.Requester.ToLower()));
            }

            var topAreas = await query
                .GroupBy(r => r.Subject)
                .Select(g => new
                {
                    Subject = g.Key,
                    Count = g.Count(),
                    Requests = g.OrderByDescending(r => r.CreatedTime).Select(r => new
                    {
                        r.Id,
                        r.DisplayId,
                        r.Subject,
                        r.Status,
                        r.CreatedTime,
                        r.RequesterName,
                        r.RequesterEmail,
                        r.TechnicianName,
                        r.TechnicianEmail,
                        r.JsonData
                    }).ToList()
                })
                .OrderByDescending(x => x.Count)
                .Take(topN)
                .ToListAsync();

            var detailedAreas = topAreas.Select(area => new
            {
                area.Subject,
                area.Count,
                Requests = area.Requests.Select(r => ParseRequestDetails(r.JsonData, r)).ToList()
            }).ToList();

            return new
            {
                QueryType = "TopRequestAreas",
                Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                TopN = topN,
                TopAreas = detailedAreas,
                TotalAreas = detailedAreas.Count,
                TotalRequests = detailedAreas.Sum(x => x.Count),
                Timestamp = DateTime.UtcNow
            };
        }

        private async Task<object> GetTopTechniciansData(QueryAnalysis analysis)
        {
            var dateFrom = ParseDateTime(analysis.DateFrom) ?? DateTimeOffset.UtcNow.AddDays(-30).DateTime;
            var dateTo = ParseDateTime(analysis.DateTo) ?? DateTimeOffset.UtcNow.DateTime;
            var topN = analysis.TopN ?? 10;

            var query = _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo && !string.IsNullOrEmpty(r.TechnicianName));

            if (analysis.Technicians?.Any() ?? false)
            {
                query = query.Where(r => analysis.Technicians.Contains(r.TechnicianName, StringComparer.OrdinalIgnoreCase));
            }

            if (!string.IsNullOrEmpty(analysis.Technician))
            {
                query = query.Where(r => r.TechnicianName.ToLower().Contains(analysis.Technician.ToLower()));
            }

            if (!string.IsNullOrEmpty(analysis.Requester))
            {
                query = query.Where(r => r.RequesterName.ToLower().Contains(analysis.Requester.ToLower()));
            }

            if (!string.IsNullOrEmpty(analysis.Status))
            {
                query = ApplyStatusFilter(query, analysis.Status);
            }

            var technicianStats = await query
                .GroupBy(r => r.TechnicianName)
                .Select(g => new
                {
                    Technician = g.Key,
                    TechnicianEmail = g.Select(r => r.TechnicianEmail).FirstOrDefault(e => !string.IsNullOrEmpty(e)),
                    RequestsHandled = g.Count(),
                    AllRequests = g.OrderByDescending(r => r.CreatedTime)
                                     .Select(r => new {
                                         r.Id,
                                         r.DisplayId,
                                         r.Subject,
                                         r.Status,
                                         r.CreatedTime,
                                         r.RequesterName,
                                         r.RequesterEmail,
                                         r.JsonData
                                     }).ToList()
                })
                .OrderByDescending(x => x.RequestsHandled)
                .Take(topN)
                .ToListAsync();

            var detailedStats = technicianStats.Select(tech =>
            {
                var parsedRequests = tech.AllRequests.Select(r => ParseRequestDetails(r.JsonData, r)).ToList();
                return new
                {
                    tech.Technician,
                    tech.TechnicianEmail,
                    tech.RequestsHandled,
                    OpenRequests = parsedRequests.Count(r => IsOpenStatus(r.StatusInternal)),
                    ClosedRequests = parsedRequests.Count(r => IsClosedStatus(r.StatusInternal)),
                    Requests = parsedRequests
                };
            }).ToList();

            return new
            {
                QueryType = "TopTechnicians",
                Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                TopN = topN,
                TopTechnicians = detailedStats,
                TotalTechnicians = detailedStats.Count,
                TotalRequests = detailedStats.Sum(x => x.RequestsHandled),
                Timestamp = DateTime.UtcNow
            };
        }

        private async Task<object> GetRequestSearchData(QueryAnalysis analysis, string userEmail = "")
        {
            var dateFrom = ParseDateTime(analysis.DateFrom) ?? DateTimeOffset.UtcNow.AddDays(-30).DateTime;
            var dateTo = ParseDateTime(analysis.DateTo) ?? DateTimeOffset.UtcNow.DateTime;

            var query = _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime >= dateFrom && r.CreatedTime <= dateTo);

            // Enhanced status filtering
            if (!string.IsNullOrEmpty(analysis.Status))
            {
                query = ApplyStatusFilter(query, analysis.Status);
            }

            // Enhanced personalization filtering
            if (analysis.IsUserRequest && !string.IsNullOrEmpty(userEmail))
            {
                query = query.Where(r => r.RequesterEmail == userEmail);
            }
            else if (analysis.IsUserTechnician && !string.IsNullOrEmpty(userEmail))
            {
                query = query.Where(r => r.TechnicianEmail == userEmail);
            }
            else
            {
                // Apply technician filters
                if (analysis.Technicians?.Any() ?? false)
                {
                    query = query.Where(r => analysis.Technicians.Contains(r.TechnicianName, StringComparer.OrdinalIgnoreCase));
                }
                if (!string.IsNullOrEmpty(analysis.Technician))
                {
                    query = query.Where(r => r.TechnicianName.ToLower().Contains(analysis.Technician.ToLower()));
                }
                
                // Apply requester filter
                if (!string.IsNullOrEmpty(analysis.Requester))
                {
                    query = query.Where(r => r.RequesterName.ToLower().Contains(analysis.Requester.ToLower()) || 
                                             r.RequesterEmail.ToLower().Contains(analysis.Requester.ToLower()));
                }
            }

            // Filter by subject if specified
            if (!string.IsNullOrEmpty(analysis.Subject))
            {
                query = query.Where(r => r.Subject.ToLower().Contains(analysis.Subject.ToLower()));
            }

            var requests = await query
                .OrderByDescending(r => r.CreatedTime)
                .Take(analysis.TopN ?? 100)
                .ToListAsync();

            var detailedRequests = requests.Select(r => ParseRequestDetails(r.JsonData, r)).ToList();

            return new
            {
                QueryType = "RequestSearch",
                Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                Subject = analysis.Subject,
                Technician = analysis.Technician,
                Technicians = analysis.Technicians,
                Requester = analysis.Requester,
                Status = analysis.Status,
                IsUserRequest = analysis.IsUserRequest,
                IsUserTechnician = analysis.IsUserTechnician,
                RequestsFound = detailedRequests.Count,
                Requests = detailedRequests,
                Timestamp = DateTime.UtcNow
            };
        }

        private IQueryable<ManageEngineRequest> ApplyStatusFilter(IQueryable<ManageEngineRequest> query, string statusFilter)
        {
            var lowerStatus = statusFilter.ToLower();

            if (lowerStatus == "open")
            {
                return query.Where(r =>
                    r.Status.ToLower() == "open" ||
                    r.Status.ToLower() == "in progress" ||
                    r.Status.ToLower() == "pending" ||
                    r.JsonData.Contains("\"internal_name\":\"Open\"") ||
                    r.JsonData.Contains("\"internal_name\":\"In Progress\"") ||
                    r.JsonData.Contains("\"internal_name\":\"Pending\"")
                );
            }
            else if (lowerStatus == "closed")
            {
                return query.Where(r =>
                    r.Status.ToLower() == "closed" ||
                    r.Status.ToLower() == "resolved" ||
                    r.Status.ToLower() == "completed" ||
                    r.JsonData.Contains("\"internal_name\":\"Closed\"") ||
                    r.JsonData.Contains("\"internal_name\":\"Resolved\"") ||
                    r.JsonData.Contains("\"internal_name\":\"Completed\"")
                );
            }

            return query;
        }

        private bool IsOpenStatus(string internalStatus)
        {
            if (string.IsNullOrEmpty(internalStatus)) return false;
            var lower = internalStatus.ToLower();
            return lower == "open" || lower == "in progress" || lower == "pending";
        }

        private bool IsClosedStatus(string internalStatus)
        {
            if (string.IsNullOrEmpty(internalStatus)) return false;
            var lower = internalStatus.ToLower();
            return lower == "closed" || lower == "resolved" || lower == "completed";
        }

        private dynamic ParseRequestDetails(string jsonData, dynamic basicRequest)
        {
            try
            {
                var data = JsonSerializer.Deserialize<ManageEngineRequestData>(jsonData);

                return new
                {
                    Id = basicRequest.Id?.ToString() ?? "",
                    DisplayId = data?.DisplayId ?? basicRequest.DisplayId?.ToString() ?? "",
                    Subject = data?.Subject ?? basicRequest.Subject?.ToString() ?? "",
                    TechnicianName = data?.Technician?.Name ?? basicRequest.TechnicianName?.ToString() ?? "",
                    TechnicianEmail = data?.Technician?.EmailId ?? "",
                    RequesterName = data?.Requester?.Name ?? basicRequest.RequesterName?.ToString() ?? "",
                    RequesterEmail = data?.Requester?.EmailId ?? basicRequest.RequesterEmail?.ToString() ?? "",
                    RequesterPhone = data?.Requester?.Phone ?? "",
                    RequesterMobile = data?.Requester?.Mobile ?? "",
                    RequesterDepartment = data?.Requester?.Department?.Name ?? "",
                    RequesterSite = data?.Requester?.Site?.Name ?? "",
                    RequesterJobTitle = data?.Requester?.JobTitle ?? "",
                    RequesterEmployeeId = data?.Requester?.EmployeeId ?? "",
                    Status = data?.Status?.Name ?? basicRequest.Status?.ToString() ?? "",
                    StatusInternal = data?.Status?.InternalName ?? "",
                    StatusColor = data?.Status?.Color ?? "",
                    CreatedTime = basicRequest.CreatedTime,
                    CreatedTimeDisplay = data?.CreatedTime?.DisplayValue ?? basicRequest.CreatedTime.ToString(),
                    DueByTime = data?.DueByTime?.DisplayValue ?? "",
                    Template = data?.Template?.Name ?? "",
                    Group = data?.Group?.Name ?? "",
                    IsServiceRequest = data?.IsServiceRequest ?? false,
                    HasNotes = data?.HasNotes ?? false,
                    Priority = data?.Priority?.Name ?? "",
                    Category = data?.Category?.Name ?? "",
                    Subcategory = data?.Subcategory?.Name ?? "",
                    FullData = data
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing request {basicRequest.Id}: {ex.Message}");
                return new
                {
                    Id = basicRequest.Id?.ToString() ?? "",
                    DisplayId = basicRequest.DisplayId?.ToString() ?? "",
                    Subject = basicRequest.Subject?.ToString() ?? "",
                    TechnicianName = basicRequest.TechnicianName?.ToString() ?? "",
                    TechnicianEmail = "",
                    RequesterName = basicRequest.RequesterName?.ToString() ?? "",
                    RequesterEmail = basicRequest.RequesterEmail?.ToString() ?? "",
                    RequesterPhone = "",
                    RequesterMobile = "",
                    RequesterDepartment = "",
                    RequesterSite = "",
                    RequesterJobTitle = "",
                    RequesterEmployeeId = "",
                    Status = basicRequest.Status?.ToString() ?? "",
                    StatusInternal = "",
                    StatusColor = "",
                    CreatedTime = basicRequest.CreatedTime,
                    CreatedTimeDisplay = basicRequest.CreatedTime.ToString(),
                    DueByTime = "",
                    Template = "",
                    Group = "",
                    IsServiceRequest = false,
                    HasNotes = false,
                    Priority = "",
                    Category = "",
                    Subcategory = "",
                    FullData = (object)null
                };
            }
        }

        private string GenerateDataSummary(object data, QueryAnalysis analysis)
        {
            var jsonElement = JsonSerializer.SerializeToElement(data);
            var sb = new StringBuilder();

            switch (analysis.QueryType)
            {
                case "inactive_technicians":
                    var inactive = jsonElement.GetProperty("InactiveTechnicians").EnumerateArray().Take(10).ToList();
                    sb.AppendLine($"Found {jsonElement.GetProperty("TotalInactive").GetInt32()} inactive technicians out of {jsonElement.GetProperty("TotalTechnicians").GetInt32()} total.");
                    sb.AppendLine("Top 10:");
                    foreach (var tech in inactive)
                    {
                        sb.AppendLine($"• {tech.GetProperty("Technician").GetString()} - Last active {tech.GetProperty("DaysInactive").GetInt32()} days ago");
                    }
                    break;

                case "top_technicians":
                    var topTechs = jsonElement.GetProperty("TopTechnicians").EnumerateArray().Take(10).ToList();
                    sb.AppendLine($"Top {topTechs.Count} technicians:");
                    int rank = 1;
                    foreach (var tech in topTechs)
                    {
                        sb.AppendLine($"{rank}. {tech.GetProperty("Technician").GetString()} - {tech.GetProperty("RequestsHandled").GetInt32()} requests (Open: {tech.GetProperty("OpenRequests").GetInt32()}, Closed: {tech.GetProperty("ClosedRequests").GetInt32()})");
                        rank++;
                    }
                    break;

                case "top_request_areas":
                    var topAreas = jsonElement.GetProperty("TopAreas").EnumerateArray().Take(10).ToList();
                    sb.AppendLine($"Top {topAreas.Count} categories:");
                    foreach (var area in topAreas)
                    {
                        sb.AppendLine($"• {area.GetProperty("Subject").GetString()}: {area.GetProperty("Count").GetInt32()} requests");
                    }
                    break;

                case "influx_requests":
                    if (jsonElement.TryGetProperty("HourlyData", out var hourly))
                    {
                        var peak = jsonElement.GetProperty("PeakHour");
                        sb.AppendLine($"Request influx by hour. Total: {jsonElement.GetProperty("TotalRequests").GetInt32()}");
                        sb.AppendLine($"Peak: {peak.GetProperty("DateTime").GetDateTime():MMM dd HH:00} with {peak.GetProperty("Count").GetInt32()} requests");
                    }
                    else
                    {
                        var peak = jsonElement.GetProperty("PeakDay");
                        sb.AppendLine($"Request influx by day. Total: {jsonElement.GetProperty("TotalRequests").GetInt32()}");
                        sb.AppendLine($"Peak: {peak.GetProperty("Date").GetDateTime():MMM dd} with {peak.GetProperty("Count").GetInt32()} requests");
                    }
                    break;

                case "request_search":
                    sb.AppendLine($"Found {jsonElement.GetProperty("RequestsFound").GetInt32()} matching requests");
                    break;
            }

            return sb.ToString();
        }

        private object ExtractSummaryFromData(object data, QueryAnalysis analysis)
        {
            var jsonElement = JsonSerializer.SerializeToElement(data);

            return analysis.QueryType switch
            {
                "inactive_technicians" => new
                {
                    TotalInactive = jsonElement.GetProperty("TotalInactive").GetInt32(),
                    TotalTechnicians = jsonElement.GetProperty("TotalTechnicians").GetInt32()
                },
                "top_technicians" => new
                {
                    TotalTechnicians = jsonElement.GetProperty("TotalTechnicians").GetInt32(),
                    TotalRequests = jsonElement.GetProperty("TotalRequests").GetInt32()
                },
                "top_request_areas" => new
                {
                    TotalAreas = jsonElement.GetProperty("TotalAreas").GetInt32(),
                    TotalRequests = jsonElement.GetProperty("TotalRequests").GetInt32()
                },
                "influx_requests" => new
                {
                    TotalRequests = jsonElement.GetProperty("TotalRequests").GetInt32(),
                    TimeUnit = jsonElement.GetProperty("TimeUnit").GetString()
                },
                "request_search" => new
                {
                    RequestsFound = jsonElement.GetProperty("RequestsFound").GetInt32()
                },
                _ => new { }
            };
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
            var csvBytes = GenerateEnhancedCsvFromData(dataElement);

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

            return Ok(new { SessionId = sessionId, ThreadId = conversation.ThreadId, StartedAt = conversation.StartedAt, Messages = history });
        }

        private async Task SyncRequestsInBackgroundSafe(AppDbContext dbContext, RequestStorageService requestStorageService)
        {
            try
            {
                var lastStoredDate = await requestStorageService.GetLastStoredDateAsync();
                DateTimeOffset dateFrom = lastStoredDate.HasValue
                    ? lastStoredDate.Value.AddMinutes(-5)
                    : DateTimeOffset.UtcNow.AddMonths(-1);

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

        /// <summary>
        /// Helper method to run an agent and get responses (re-added after accidental removal)
        /// </summary>
        private async Task<List<string>> RunAgentAsync(
            PersistentAgentsClient client,
            string threadId,
            string agentId,
            string userMessage,
            string additionalInstructions)
        {
            var responses = new List<string>();
            try
            {
                await client.Messages.CreateMessageAsync(threadId, MessageRole.User, userMessage);

                // Use the overload without CreateRunOptions to avoid unsupported parameters like temperature
                var runResponse = await client.Runs.CreateRunAsync(threadId, agentId, additionalInstructions: additionalInstructions);

                var run = runResponse.Value;

                var start = DateTime.UtcNow;
                var maxDuration = TimeSpan.FromSeconds(75);
                while (run.Status == RunStatus.Queued || run.Status == RunStatus.InProgress || run.Status == RunStatus.RequiresAction)
                {
                    if (DateTime.UtcNow - start > maxDuration)
                    {
                        responses.Add("Agent timeout while waiting for completion.");
                        Console.WriteLine($"Agent run timeout after {(DateTime.UtcNow-start).TotalSeconds:F1}s. Status: {run.Status}");
                        break;
                    }
                    await Task.Delay(750);
                    run = (await client.Runs.GetRunAsync(threadId, run.Id)).Value;
                    if (run.Status == RunStatus.RequiresAction)
                    {
                        responses.Add("Agent requires an action that this server does not implement.");
                        Console.WriteLine("Run requires action; exiting polling loop.");
                        break;
                    }
                }

                if (run.Status == RunStatus.Failed)
                {
                    responses.Add($"Agent failed: {run.LastError?.Message ?? "Unknown error"}");
                    return responses;
                }

                if (responses.Count > 0 && responses.Last().StartsWith("Agent timeout"))
                {
                    return responses; // skip collecting messages on timeout
                }

                var messagesAsync = client.Messages.GetMessagesAsync(threadId, order: ListSortOrder.Descending);
                await foreach (var message in messagesAsync)
                {
                    if (message.Role == MessageRole.Agent)
                    {
                        foreach (var content in message.ContentItems)
                        {
                            if (content is MessageTextContent textContent)
                            {
                                responses.Add(textContent.Text);
                            }
                        }
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Agent execution error: {ex.Message}");
                responses.Add($"Error: {ex.Message}");
            }
            return responses;
        }

        // Continue with helper methods for Zoho API, CSV generation, etc.
        // These remain the same as in your original code

        private async Task<List<Dictionary<string, object>>> FetchRequestsForDateRange(DateTimeOffset dateFrom, DateTimeOffset dateTo)
        {
            // Implementation remains the same as document 1
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

        private byte[] GenerateEnhancedCsvFromData(JsonElement data)
        {
            var sb = new StringBuilder();
            string Safe(object? value) => value?.ToString()?.Replace("\"", "\"\"").Replace("\n", " ").Replace("\r", "") ?? "";

            if (data.TryGetProperty("QueryType", out var qt))
            {
                var type = qt.GetString();

                switch (type)
                {
                    case "InactiveTechnicians":
                        sb.AppendLine("Technician,Email,Last Activity,Days Inactive,Total Requests,Request ID,Display ID,Subject,Status,Created Time,Requester,Requester Email,Department,Priority");
                        foreach (var tech in data.GetProperty("InactiveTechnicians").EnumerateArray())
                        {
                            var techName = Safe(tech.GetProperty("Technician").GetString());
                            var techEmail = Safe(tech.TryGetProperty("TechnicianEmail", out var te) ? te.GetString() : "");
                            var lastActivity = tech.GetProperty("LastActivity").GetDateTime().ToString("yyyy-MM-dd HH:mm");
                            var daysInactive = tech.GetProperty("DaysInactive").GetInt32();
                            var totalReqs = tech.GetProperty("TotalRequests").GetInt32();

                            foreach (var req in tech.GetProperty("Requests").EnumerateArray())
                            {
                                sb.AppendLine($"\"{techName}\",\"{techEmail}\",\"{lastActivity}\",\"{daysInactive}\",\"{totalReqs}\"," +
                                    $"\"{Safe(req.GetProperty("Id").GetString())}\",\"{Safe(req.GetProperty("DisplayId").GetString())}\"," +
                                    $"\"{Safe(req.GetProperty("Subject").GetString())}\",\"{Safe(req.GetProperty("Status").GetString())}\"," +
                                    $"\"{req.GetProperty("CreatedTime").GetDateTime():yyyy-MM-dd HH:mm}\"," +
                                    $"\"{Safe(req.GetProperty("RequesterName").GetString())}\",\"{Safe(req.GetProperty("RequesterEmail").GetString())}\"," +
                                    $"\"{Safe(req.TryGetProperty("RequesterDepartment", out var rd) ? rd.GetString() : "")}\"," +
                                    $"\"{Safe(req.TryGetProperty("Priority", out var pri) ? pri.GetString() : "")}\"");
                            }
                        }
                        break;

                    case "TopTechnicians":
                        sb.AppendLine("Rank,Technician,Email,Total Requests,Open Requests,Closed Requests,Request ID,Display ID,Subject,Status,Created Time,Requester,Priority,Category");
                        int rank = 1;
                        foreach (var tech in data.GetProperty("TopTechnicians").EnumerateArray())
                        {
                            var techName = Safe(tech.GetProperty("Technician").GetString());
                            var techEmail = Safe(tech.TryGetProperty("TechnicianEmail", out var te) ? te.GetString() : "");
                            var totalReqs = tech.GetProperty("RequestsHandled").GetInt32();
                            var openReqs = tech.GetProperty("OpenRequests").GetInt32();
                            var closedReqs = tech.GetProperty("ClosedRequests").GetInt32();

                            foreach (var req in tech.GetProperty("Requests").EnumerateArray())
                            {
                                sb.AppendLine($"\"{rank}\",\"{techName}\",\"{techEmail}\",\"{totalReqs}\",\"{openReqs}\",\"{closedReqs}\"," +
                                    $"\"{Safe(req.GetProperty("Id").GetString())}\",\"{Safe(req.GetProperty("DisplayId").GetString())}\"," +
                                    $"\"{Safe(req.GetProperty("Subject").GetString())}\",\"{Safe(req.GetProperty("Status").GetString())}\"," +
                                    $"\"{req.GetProperty("CreatedTime").GetDateTime():yyyy-MM-dd HH:mm}\"," +
                                    $"\"{Safe(req.GetProperty("RequesterName").GetString())}\"," +
                                    $"\"{Safe(req.TryGetProperty("Priority", out var pri) ? pri.GetString() : "")}\"," +
                                    $"\"{Safe(req.TryGetProperty("Category", out var cat) ? cat.GetString() : "")}\"");
                            }
                            rank++;
                        }
                        break;

                    case "TopRequestAreas":
                        sb.AppendLine("Subject,Total Count,Request ID,Display ID,Status,Created Time,Technician,Technician Email,Requester,Requester Email,Department,Priority");
                        foreach (var area in data.GetProperty("TopAreas").EnumerateArray())
                        {
                            var subject = Safe(area.GetProperty("Subject").GetString());
                            var count = area.GetProperty("Count").GetInt32();

                            foreach (var req in area.GetProperty("Requests").EnumerateArray())
                            {
                                sb.AppendLine($"\"{subject}\",\"{count}\"," +
                                    $"\"{Safe(req.GetProperty("Id").GetString())}\",\"{Safe(req.GetProperty("DisplayId").GetString())}\"," +
                                    $"\"{Safe(req.GetProperty("Status").GetString())}\"," +
                                    $"\"{req.GetProperty("CreatedTime").GetDateTime():yyyy-MM-dd HH:mm}\"," +
                                    $"\"{Safe(req.TryGetProperty("TechnicianName", out var tn) ? tn.GetString() : "")}\"," +
                                    $"\"{Safe(req.TryGetProperty("TechnicianEmail", out var te) ? te.GetString() : "")}\"," +
                                    $"\"{Safe(req.GetProperty("RequesterName").GetString())}\"," +
                                    $"\"{Safe(req.GetProperty("RequesterEmail").GetString())}\"," +
                                    $"\"{Safe(req.TryGetProperty("RequesterDepartment", out var rd) ? rd.GetString() : "")}\"," +
                                    $"\"{Safe(req.TryGetProperty("Priority", out var pri) ? pri.GetString() : "")}\"");
                            }
                        }
                        break;

                    case "InfluxRequests":
                        if (data.TryGetProperty("HourlyData", out var hourly))
                        {
                            sb.AppendLine("DateTime,Hour Count,Request ID,Display ID,Subject,Status,Technician,Requester,Priority");
                            foreach (var item in hourly.EnumerateArray())
                            {
                                var dateTime = item.GetProperty("DateTime").GetDateTime().ToString("yyyy-MM-dd HH:00");
                                var count = item.GetProperty("Count").GetInt32();

                                foreach (var req in item.GetProperty("Requests").EnumerateArray())
                                {
                                    sb.AppendLine($"\"{dateTime}\",\"{count}\"," +
                                        $"\"{Safe(req.GetProperty("Id").GetString())}\",\"{Safe(req.GetProperty("DisplayId").GetString())}\"," +
                                        $"\"{Safe(req.GetProperty("Subject").GetString())}\",\"{Safe(req.GetProperty("Status").GetString())}\"," +
                                        $"\"{Safe(req.TryGetProperty("TechnicianName", out var tn) ? tn.GetString() : "")}\"," +
                                        $"\"{Safe(req.GetProperty("RequesterName").GetString())}\"," +
                                        $"\"{Safe(req.TryGetProperty("Priority", out var pri) ? pri.GetString() : "")}\"");
                                }
                            }
                        }
                        else
                        {
                            sb.AppendLine("Date,Day Count,Request ID,Display ID,Subject,Status,Technician,Requester,Priority");
                            foreach (var item in data.GetProperty("DailyData").EnumerateArray())
                            {
                                var date = item.GetProperty("Date").GetDateTime().ToString("yyyy-MM-dd");
                                var count = item.GetProperty("Count").GetInt32();

                                foreach (var req in item.GetProperty("Requests").EnumerateArray())
                                {
                                    sb.AppendLine($"\"{date}\",\"{count}\"," +
                                        $"\"{Safe(req.GetProperty("Id").GetString())}\",\"{Safe(req.GetProperty("DisplayId").GetString())}\"," +
                                        $"\"{Safe(req.GetProperty("Subject").GetString())}\",\"{Safe(req.GetProperty("Status").GetString())}\"," +
                                        $"\"{Safe(req.TryGetProperty("TechnicianName", out var tn) ? tn.GetString() : "")}\"," +
                                        $"\"{Safe(req.GetProperty("RequesterName").GetString())}\"," +
                                        $"\"{Safe(req.TryGetProperty("Priority", out var pri) ? pri.GetString() : "")}\"");
                                }
                            }
                        }
                        break;

                    case "RequestSearch":
                        sb.AppendLine("ID,Display ID,Subject,Technician,Technician Email,Requester,Requester Email,Status,Created Time,Department,Site,Priority,Category,Subcategory,Phone,Mobile,Employee ID,Job Title");
                        foreach (var r in data.GetProperty("Requests").EnumerateArray())
                        {
                            sb.AppendLine($"\"{Safe(r.GetProperty("Id").GetString())}\",\"{Safe(r.GetProperty("DisplayId").GetString())}\"," +
                                $"\"{Safe(r.GetProperty("Subject").GetString())}\"," +
                                $"\"{Safe(r.GetProperty("TechnicianName").GetString())}\"," +
                                $"\"{Safe(r.GetProperty("TechnicianEmail").GetString())}\"," +
                                $"\"{Safe(r.GetProperty("RequesterName").GetString())}\"," +
                                $"\"{Safe(r.GetProperty("RequesterEmail").GetString())}\"," +
                                $"\"{Safe(r.GetProperty("Status").GetString())}\"," +
                                $"\"{r.GetProperty("CreatedTime").GetDateTime():yyyy-MM-dd HH:mm}\"," +
                                $"\"{Safe(r.TryGetProperty("RequesterDepartment", out var rd) ? rd.GetString() : "")}\"," +
                                $"\"{Safe(r.TryGetProperty("RequesterSite", out var rs) ? rs.GetString() : "")}\"," +
                                $"\"{Safe(r.TryGetProperty("Priority", out var pri) ? pri.GetString() : "")}\"," +
                                $"\"{Safe(r.TryGetProperty("Category", out var cat) ? cat.GetString() : "")}\"," +
                                $"\"{Safe(r.TryGetProperty("Subcategory", out var sub) ? sub.GetString() : "")}\"," +
                                $"\"{Safe(r.TryGetProperty("RequesterPhone", out var rp) ? rp.GetString() : "")}\"," +
                                $"\"{Safe(r.TryGetProperty("RequesterMobile", out var rm) ? rm.GetString() : "")}\"," +
                                $"\"{Safe(r.TryGetProperty("RequesterEmployeeId", out var eid) ? eid.GetString() : "")}\"," +
                                $"\"{Safe(r.TryGetProperty("RequesterJobTitle", out var jt) ? jt.GetString() : "")}\"");
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

    // Helper Classes (same as document 1)
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

        [JsonPropertyName("technicians")]
        public List<string> Technicians { get; set; }

        [JsonPropertyName("requester")]
        public string Requester { get; set; }

        [JsonPropertyName("inactivityPeriod")]
        public string InactivityPeriod { get; set; }

        [JsonPropertyName("isUserRequest")]
        public bool IsUserRequest { get; set; }

        [JsonPropertyName("isUserTechnician")]
        public bool IsUserTechnician { get; set; }

        [JsonPropertyName("status")]
        public string Status { get; set; }
    }

    // Data models (same as document 1)
    public class ManageEngineRequestData
    {
        [JsonPropertyName("status")]
        public StatusInfo Status { get; set; }

        [JsonPropertyName("requester")]
        public RequesterInfo Requester { get; set; }

        [JsonPropertyName("technician")]
        public TechnicianInfo Technician { get; set; }

        [JsonPropertyName("subject")]
        public string Subject { get; set; }

        [JsonPropertyName("created_time")]
        public TimeInfo CreatedTime { get; set; }

        [JsonPropertyName("due_by_time")]
        public TimeInfo DueByTime { get; set; }

        [JsonPropertyName("display_id")]
        public string DisplayId { get; set; }

        [JsonPropertyName("template")]
        public TemplateInfo Template { get; set; }

        [JsonPropertyName("group")]
        public GroupInfo Group { get; set; }

        [JsonPropertyName("is_service_request")]
        public bool IsServiceRequest { get; set; }

        [JsonPropertyName("has_notes")]
        public bool HasNotes { get; set; }

        [JsonPropertyName("priority")]
        public PriorityInfo Priority { get; set; }

        [JsonPropertyName("category")]
        public CategoryInfo Category { get; set; }

        [JsonPropertyName("subcategory")]
        public SubcategoryInfo Subcategory { get; set; }
    }

    public class StatusInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("internal_name")]
        public string InternalName { get; set; }

        [JsonPropertyName("color")]
        public string Color { get; set; }
    }

    public class RequesterInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("email_id")]
        public string EmailId { get; set; }

        [JsonPropertyName("phone")]
        public string Phone { get; set; }

        [JsonPropertyName("mobile")]
        public string Mobile { get; set; }

        [JsonPropertyName("employee_id")]
        public string EmployeeId { get; set; }

        [JsonPropertyName("job_title")]
        public string JobTitle { get; set; }

        [JsonPropertyName("department")]
        public DepartmentInfo Department { get; set; }

        [JsonPropertyName("site")]
        public SiteInfo Site { get; set; }
    }

    public class TechnicianInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("email_id")]
        public string EmailId { get; set; }
    }

    public class TimeInfo
    {
        [JsonPropertyName("value")]
        public string Value { get; set; }

        [JsonPropertyName("display_value")]
        public string DisplayValue { get; set; }
    }

    public class TemplateInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
    }

    public class GroupInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
    }

    public class PriorityInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
    }

    public class CategoryInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
    }

    public class SubcategoryInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
    }

    public class DepartmentInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
    }

    public class SiteInfo
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
    }
}