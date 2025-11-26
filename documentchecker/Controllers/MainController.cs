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

        // Thread lock dictionary to prevent concurrent operations on same thread
        private static readonly Dictionary<string, SemaphoreSlim> _threadLocks = new();
        private static readonly object _lockDictLock = new();

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

        private SemaphoreSlim GetThreadLock(string threadId)
        {
            lock (_lockDictLock)
            {
                if (!_threadLocks.ContainsKey(threadId))
                {
                    _threadLocks[threadId] = new SemaphoreSlim(1, 1);
                }
                return _threadLocks[threadId];
            }
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
                // Background sync (fire and forget)
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

                // Thread ID management - create if not exists
                string threadId;
                if (string.IsNullOrEmpty(conversation.ThreadId))
                {
                    var threadResponse = await agentsClient.Threads.CreateThreadAsync();
                    threadId = threadResponse.Value.Id;
                    conversation.ThreadId = threadId;
                    await _dbContext.SaveChangesAsync();
                }
                else
                {
                    threadId = conversation.ThreadId;
                }

                // Get thread lock to prevent concurrent operations
                var threadLock = GetThreadLock(threadId);

                // Add user message to conversation history
                var userMessageEntity = new ChatMessage { Role = "user", Content = request.Query };
                conversation.Messages.Add(userMessageEntity);
                await _dbContext.SaveChangesAsync();

                // Build conversation context from previous messages
                var conversationContext = BuildConversationContext(conversation);

                // Acquire lock with timeout
                if (!await threadLock.WaitAsync(TimeSpan.FromSeconds(90)))
                {
                    return StatusCode(503, new
                    {
                        Error = "System is busy processing a previous request. Please try again in a moment.",
                        ConversationalResponse = "I'm currently processing your previous request. Please wait a moment and try again."
                    });
                }

                try
                {
                    // Wait for any active runs to complete before proceeding
                    await WaitForActiveRunsToComplete(agentsClient, threadId);

                    // STEP 1: Query Analysis Agent - Extract structured parameters
                    var queryAnalysis = await AnalyzeQueryWithAgent(agentsClient, threadId, request.Query, conversationContext, request.UserEmail);
                    Console.WriteLine($"Query Analysis: {JsonSerializer.Serialize(queryAnalysis)}");

                    // STEP 2: Retrieve data from database based on analysis
                    var retrievedData = await RetrieveDataBasedOnAnalysis(queryAnalysis, request.UserEmail);

                    // STEP 3: Data Retrieval Agent - Optional refinement
                    var dataSummary = GenerateDataSummary(retrievedData, queryAnalysis);

                    // Wait for any active runs before refinement agent
                    await WaitForActiveRunsToComplete(agentsClient, threadId);

                    var refinedAnalysis = await RefineQueryWithDataRetrievalAgent(agentsClient, threadId, request.Query, queryAnalysis, dataSummary, conversationContext);

                    // If refinement produced a better analysis, use it and re-query data
                    if (refinedAnalysis != null && refinedAnalysis.QueryType != queryAnalysis.QueryType)
                    {
                        queryAnalysis = refinedAnalysis;
                        retrievedData = await RetrieveDataBasedOnAnalysis(queryAnalysis, request.UserEmail);
                        dataSummary = GenerateDataSummary(retrievedData, queryAnalysis);
                    }

                    // Wait for any active runs before conversation agent
                    await WaitForActiveRunsToComplete(agentsClient, threadId);

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
                finally
                {
                    threadLock.Release();
                }
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

        /// <summary>
        /// Wait for any active runs on a thread to complete before adding new messages
        /// </summary>
        private async Task WaitForActiveRunsToComplete(PersistentAgentsClient client, string threadId, int maxWaitSeconds = 60)
        {
            var startTime = DateTime.UtcNow;
            while ((DateTime.UtcNow - startTime).TotalSeconds < maxWaitSeconds)
            {
                try
                {
                    // Get all runs for this thread
                    var runsAsync = client.Runs.GetRunsAsync(threadId, limit: 10);
                    var hasActiveRun = false;

                    await foreach (var run in runsAsync)
                    {
                        if (run.Status == RunStatus.InProgress || run.Status == RunStatus.Queued || run.Status == RunStatus.RequiresAction)
                        {
                            hasActiveRun = true;
                            Console.WriteLine($"Waiting for active run {run.Id} with status {run.Status}...");
                            break;
                        }
                    }

                    if (!hasActiveRun)
                    {
                        return; // No active runs, safe to proceed
                    }

                    await Task.Delay(1000); // Wait 1 second before checking again
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error checking run status: {ex.Message}");
                    await Task.Delay(500);
                }
            }

            Console.WriteLine($"Timeout waiting for active runs to complete on thread {threadId}");
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
            sb.AppendLine("=== CONVERSATION HISTORY ===");

            foreach (var msg in recentMessages)
            {
                var role = msg.Role == "user" ? "USER" : "ASSISTANT";
                var content = msg.Content;

                // For agent messages, extract useful context from JSON
                if (role == "ASSISTANT" && content.StartsWith("{"))
                {
                    try
                    {
                        var doc = JsonDocument.Parse(content);
                        var root = doc.RootElement;

                        // Extract the query analysis to understand what was searched
                        if (root.TryGetProperty("QueryAnalysis", out var analysisElem))
                        {
                            var queryType = analysisElem.TryGetProperty("queryType", out var qt) ? qt.GetString() : "";
                            var technician = analysisElem.TryGetProperty("technician", out var tech) ? tech.GetString() : "";
                            var status = analysisElem.TryGetProperty("status", out var st) ? st.GetString() : "";
                            var dateFrom = analysisElem.TryGetProperty("dateFrom", out var df) ? df.GetString() : "";
                            var dateTo = analysisElem.TryGetProperty("dateTo", out var dt) ? dt.GetString() : "";

                            sb.AppendLine($"[Previous Query: type={queryType}, technician={technician}, status={status}, period={dateFrom} to {dateTo}]");
                        }

                        // Extract key data points
                        if (root.TryGetProperty("Data", out var dataElem))
                        {
                            // For technician-related queries, extract technician names
                            if (dataElem.TryGetProperty("InactiveTechnicians", out var inactiveTechs))
                            {
                                var names = inactiveTechs.EnumerateArray()
                                    .Take(20)
                                    .Select(t => t.GetProperty("Technician").GetString())
                                    .Where(n => !string.IsNullOrEmpty(n))
                                    .ToList();
                                sb.AppendLine($"[Technicians mentioned: {string.Join(", ", names)}]");
                            }
                            else if (dataElem.TryGetProperty("TopTechnicians", out var topTechs))
                            {
                                var names = topTechs.EnumerateArray()
                                    .Take(20)
                                    .Select(t => t.GetProperty("Technician").GetString())
                                    .Where(n => !string.IsNullOrEmpty(n))
                                    .ToList();
                                sb.AppendLine($"[Technicians mentioned: {string.Join(", ", names)}]");
                            }

                            // For request searches, note the count
                            if (dataElem.TryGetProperty("RequestsFound", out var reqFound))
                            {
                                sb.AppendLine($"[Found {reqFound.GetInt32()} requests]");
                            }
                        }

                        // Also include the conversational response for context
                        if (root.TryGetProperty("ConversationalResponse", out var resp))
                        {
                            content = resp.GetString() ?? "";
                            if (content.Length > 300)
                                content = content.Substring(0, 300) + "...";
                        }
                    }
                    catch { }
                }

                sb.AppendLine($"{role}: {content}");
            }

            sb.AppendLine("=== END HISTORY ===");
            sb.AppendLine("\nIMPORTANT: Use this context to understand references like 'them', 'those', 'the technicians', 'how many are open', etc.");
            sb.AppendLine("If user asks a follow-up like 'how many of them are open', apply the previous filters PLUS the new 'open' status filter.");

            return sb.ToString();
        }

        private async Task<QueryAnalysis> AnalyzeQueryWithAgent(
            PersistentAgentsClient client,
            string threadId,
            string userQuery,
            string conversationContext,
            string userEmail = "")
        {
            // IMPORTANT: Use local timezone-aware date calculations
            var now = DateTime.UtcNow;
            var today = now.Date;
            var yesterday = today.AddDays(-1);
            var thisMonthStart = new DateTime(now.Year, now.Month, 1);

            var currentDate = today.ToString("yyyy-MM-dd");
            var yesterdayDate = yesterday.ToString("yyyy-MM-dd");
            var currentTime = now.ToString("HH:mm");

            var instructions = $@"You are a query analysis agent for an IT service desk system.

CRITICAL: Today's date is {currentDate}. Yesterday was {yesterdayDate}. Current time is {currentTime} UTC.
This month started on {thisMonthStart:yyyy-MM-dd}.

Your task: Analyze the user query and return ONLY a valid JSON object with NO explanations or markdown.

Schema:
{{
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
}}

Query Types:
1. inactive_technicians - technicians with no activity in period
2. influx_requests - request volume by hour/day
3. top_request_areas - most common request subjects
4. top_technicians - technician performance ranking
5. request_search - search with filters (DEFAULT for most queries)

=== DATE PARSING RULES (CRITICAL) ===
- 'today' → {currentDate} 00:00 to {currentDate} 23:59
- 'yesterday' → {yesterdayDate} 00:00 to {yesterdayDate} 23:59
- 'this week' → {today.AddDays(-7):yyyy-MM-dd} 00:00 to {currentDate} 23:59
- 'this month' → {thisMonthStart:yyyy-MM-dd} 00:00 to {currentDate} 23:59
- 'past X days' or 'last X days' → {today.AddDays(-1):yyyy-MM-dd} adjusted by X days

=== STATUS FILTERING ===
- 'open' includes: open, in progress, pending
- 'closed' includes: closed, resolved, completed
- Keywords: 'open requests', 'closed tickets', 'pending', 'resolved'

=== PERSONALIZATION RULES ===
- 'my requests' / 'tickets I opened' / 'requests from me' → isUserRequest: true
- 'assigned to me' / 'my tickets' / 'tickets I have' → isUserTechnician: true
- Specific names → extract to 'technician' or 'requester'

=== FOLLOW-UP QUERY HANDLING (VERY IMPORTANT) ===
If the conversation context shows a previous query, and the user asks a follow-up:
- 'how many of them are open' → Keep previous technician filter, ADD status: 'open'
- 'show me closed ones' → Keep previous filters, CHANGE status to 'closed'
- 'what about this week' → Keep previous type/filters, CHANGE date range

ALWAYS preserve relevant filters from context for follow-up questions.

=== EXAMPLES ===

Query: ""what tickets do i have assigned to me""
→ {{""queryType"": ""request_search"", ""dateFrom"": ""{today.AddDays(-30):yyyy-MM-dd} 00:00"", ""dateTo"": ""{currentDate} 23:59"", ""isUserTechnician"": true, ""status"": null, ""technician"": null, ""timeUnit"": null, ""topN"": null, ""subject"": null, ""technicians"": null, ""requester"": null, ""inactivityPeriod"": null, ""isUserRequest"": false}}

Query: ""how many tickets assigned to akinola this month""
→ {{""queryType"": ""request_search"", ""dateFrom"": ""{thisMonthStart:yyyy-MM-dd} 00:00"", ""dateTo"": ""{currentDate} 23:59"", ""technician"": ""akinola"", ""status"": null, ""isUserRequest"": false, ""isUserTechnician"": false, ""timeUnit"": null, ""topN"": null, ""subject"": null, ""technicians"": null, ""requester"": null, ""inactivityPeriod"": null}}

Query: ""how many of them are open"" (after asking about akinola)
→ {{""queryType"": ""request_search"", ""dateFrom"": ""{thisMonthStart:yyyy-MM-dd} 00:00"", ""dateTo"": ""{currentDate} 23:59"", ""technician"": ""akinola"", ""status"": ""open"", ""isUserRequest"": false, ""isUserTechnician"": false, ""timeUnit"": null, ""topN"": null, ""subject"": null, ""technicians"": null, ""requester"": null, ""inactivityPeriod"": null}}

Query: ""what hour had an influx of requests yesterday""
→ {{""queryType"": ""influx_requests"", ""dateFrom"": ""{yesterdayDate} 00:00"", ""dateTo"": ""{yesterdayDate} 23:59"", ""timeUnit"": ""hour"", ""topN"": null, ""subject"": null, ""technician"": null, ""technicians"": null, ""requester"": null, ""inactivityPeriod"": null, ""isUserRequest"": false, ""isUserTechnician"": false, ""status"": null}}

Output ONLY the JSON object. No markdown code blocks, no explanations.";

            var prompt = $@"Current Date: {currentDate}
Current Time: {currentTime} UTC
Yesterday: {yesterdayDate}
This Month Start: {thisMonthStart:yyyy-MM-dd}
User Email: {(string.IsNullOrEmpty(userEmail) ? "Unknown" : userEmail)}

{(!string.IsNullOrEmpty(conversationContext) ? conversationContext : "")}

USER QUERY: {userQuery}

Return ONLY the JSON analysis:";

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
            return await FallbackHeuristicAnalysis(userQuery, userEmail, conversationContext);
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

            var instructions = @"You are a friendly, helpful IT service desk assistant named AIDA (AI Desk Assistant).

Generate a warm, conversational response that feels like talking to a helpful colleague. Follow these guidelines:

TONE:
- Be warm and personable, not robotic
- Use natural language, not bullet-heavy lists
- Sound like a knowledgeable colleague sharing insights
- Show enthusiasm when sharing positive news

STRUCTURE (2-4 paragraphs):
1. Start with a brief, natural acknowledgment of what they asked
2. Share the key finding or number prominently
3. Highlight 3-5 notable items naturally in prose (use bullet points sparingly)
4. End with a helpful note about the downloadable file and offer to help further

EXAMPLES OF GOOD RESPONSES:

For ""how many tickets assigned to akinola this month"":
""Looking at Akinola's workload this month, I found 72 tickets assigned to them. That's a solid amount of activity! The tickets cover a range of areas including password resets, hardware requests, and software installations. You can download the full breakdown in the CSV file for more details. Would you like me to filter these by status or category?""

For ""what tickets do I have assigned to me"":
""You currently have 15 tickets assigned to you. Most of them are in 'Open' or 'In Progress' status. The oldest one is from 3 days ago regarding a VPN connection issue. I've included all the details in the CSV file. Let me know if you'd like me to help prioritize or filter these further!""

For influx queries:
""Yesterday saw 47 requests come through, with the busiest period between 9-10 AM when 12 tickets were logged - probably the morning rush! Things quieted down after lunch with only a handful in the afternoon. The full hourly breakdown is in the CSV.""

AVOID:
- Starting with ""I processed your query successfully""
- Excessive bullet points
- Robotic language like ""Key findings:""
- Generic phrases like ""Here's what I found""
- Repeating ""You can download the full CSV"" multiple times

Remember: Sound human, be helpful, share insights naturally.";

            var userRequestNote = analysis.IsUserRequest ? "\n[Filtered to user's submitted requests]" :
                analysis.IsUserTechnician ? "\n[Filtered to requests assigned to user as technician]" : "";

            var statusNote = !string.IsNullOrEmpty(analysis.Status) ?
                $"\n[Status filter: {analysis.Status}]" : "";

            var prompt = $@"User asked: ""{userQuery}""

Query Details:
- Type: {analysis.QueryType}
- Period: {analysis.DateFrom ?? "N/A"} to {analysis.DateTo ?? "N/A"}
- Technician filter: {analysis.Technician ?? "none"}
- Status filter: {analysis.Status ?? "all"}{userRequestNote}{statusNote}

DATA SUMMARY:
{dataSummary}

{(!string.IsNullOrEmpty(conversationContext) ? $"Previous conversation context:\n{conversationContext}\n" : "")}

Generate a friendly, conversational response:";

            var responses = await RunAgentAsync(client, threadId, _conversationAgentId, prompt, instructions);
            var last = responses.LastOrDefault() ?? string.Empty;

            // Enhanced fallback if agent run failed
            if (string.IsNullOrEmpty(last) || last.StartsWith("Agent failed:") || last.StartsWith("Error:") || last.StartsWith("Agent timeout"))
            {
                return GenerateFallbackConversationalResponse(data, analysis, userQuery, dataSummary);
            }

            return last;
        }

        private string GenerateFallbackConversationalResponse(object data, QueryAnalysis analysis, string userQuery, string dataSummary)
        {
            var sb = new StringBuilder();
            var jsonElement = JsonSerializer.SerializeToElement(data);

            switch (analysis.QueryType)
            {
                case "request_search":
                    var reqCount = jsonElement.TryGetProperty("RequestsFound", out var rf) ? rf.GetInt32() : 0;
                    if (analysis.IsUserTechnician)
                    {
                        sb.AppendLine($"You have {reqCount} tickets assigned to you for the selected period.");
                    }
                    else if (!string.IsNullOrEmpty(analysis.Technician))
                    {
                        sb.AppendLine($"I found {reqCount} tickets assigned to {analysis.Technician}.");
                    }
                    else
                    {
                        sb.AppendLine($"I found {reqCount} tickets matching your criteria.");
                    }

                    if (!string.IsNullOrEmpty(analysis.Status))
                    {
                        sb.AppendLine($"These are filtered to show only {analysis.Status} tickets.");
                    }

                    if (jsonElement.TryGetProperty("Requests", out var reqs) && reqs.GetArrayLength() > 0)
                    {
                        sb.AppendLine("\nHere are a few of the most recent ones:");
                        var count = 0;
                        foreach (var req in reqs.EnumerateArray().Take(5))
                        {
                            var subject = req.TryGetProperty("Subject", out var subj) ? subj.GetString() : "No subject";
                            var status = req.TryGetProperty("Status", out var st) ? st.GetString() : "";
                            sb.AppendLine($"• {subject} ({status})");
                            count++;
                        }
                    }
                    break;

                case "top_technicians":
                    var techCount = jsonElement.TryGetProperty("TotalTechnicians", out var tc) ? tc.GetInt32() : 0;
                    var totalReqs = jsonElement.TryGetProperty("TotalRequests", out var tr) ? tr.GetInt32() : 0;
                    sb.AppendLine($"Here's the technician performance breakdown - I found {techCount} technicians who handled {totalReqs} requests total.");

                    if (jsonElement.TryGetProperty("TopTechnicians", out var techs))
                    {
                        sb.AppendLine("\nTop performers:");
                        var rank = 1;
                        foreach (var tech in techs.EnumerateArray().Take(5))
                        {
                            var name = tech.TryGetProperty("Technician", out var n) ? n.GetString() : "Unknown";
                            var handled = tech.TryGetProperty("RequestsHandled", out var h) ? h.GetInt32() : 0;
                            sb.AppendLine($"{rank}. {name} - {handled} requests");
                            rank++;
                        }
                    }
                    break;

                case "influx_requests":
                    var influxTotal = jsonElement.TryGetProperty("TotalRequests", out var it) ? it.GetInt32() : 0;
                    var timeUnit = jsonElement.TryGetProperty("TimeUnit", out var tu) ? tu.GetString() : "hour";

                    if (jsonElement.TryGetProperty("PeakHour", out var peakHour))
                    {
                        var peakTime = peakHour.TryGetProperty("DateTime", out var pt) ? pt.GetDateTime() : DateTime.MinValue;
                        var peakCount = peakHour.TryGetProperty("Count", out var pc) ? pc.GetInt32() : 0;
                        sb.AppendLine($"I analyzed the request flow and found {influxTotal} total requests. The busiest time was {peakTime:MMM dd} at {peakTime:HH:00} with {peakCount} requests coming in.");
                    }
                    else if (jsonElement.TryGetProperty("PeakDay", out var peakDay))
                    {
                        var peakDate = peakDay.TryGetProperty("Date", out var pd) ? pd.GetDateTime() : DateTime.MinValue;
                        var peakCount = peakDay.TryGetProperty("Count", out var pc) ? pc.GetInt32() : 0;
                        sb.AppendLine($"Looking at daily trends, there were {influxTotal} total requests. The busiest day was {peakDate:MMM dd} with {peakCount} requests.");
                    }
                    break;

                case "inactive_technicians":
                    var inactiveCount = jsonElement.TryGetProperty("TotalInactive", out var ic) ? ic.GetInt32() : 0;
                    var totalTechs = jsonElement.TryGetProperty("TotalTechnicians", out var tt) ? tt.GetInt32() : 0;
                    sb.AppendLine($"I found {inactiveCount} technicians who haven't had any activity recently (out of {totalTechs} total).");

                    if (jsonElement.TryGetProperty("InactiveTechnicians", out var inactive))
                    {
                        sb.AppendLine("\nMost inactive:");
                        foreach (var tech in inactive.EnumerateArray().Take(5))
                        {
                            var name = tech.TryGetProperty("Technician", out var n) ? n.GetString() : "Unknown";
                            var days = tech.TryGetProperty("DaysInactive", out var d) ? d.GetInt32() : 0;
                            sb.AppendLine($"• {name} - {days} days inactive");
                        }
                    }
                    break;

                case "top_request_areas":
                    var areasCount = jsonElement.TryGetProperty("TotalAreas", out var ac) ? ac.GetInt32() : 0;
                    sb.AppendLine($"Here are the top request categories:");

                    if (jsonElement.TryGetProperty("TopAreas", out var areas))
                    {
                        foreach (var area in areas.EnumerateArray().Take(5)
                          )
                        {
                            var subject = area.TryGetProperty("Subject", out var s) ? s.GetString() : "Unknown";
                            var count = area.TryGetProperty("Count", out var c) ? c.GetInt32() : 0;
                            sb.AppendLine($"• {subject}: {count} requests");
                        }
                    }
                    break;

                default:
                    sb.AppendLine("I've retrieved the data you requested.");
                    break;
            }

            sb.AppendLine("\nThe full details are available in the CSV file. Let me know if you'd like me to dig deeper into any of these!");
            return sb.ToString();
        }

        private async Task<QueryAnalysis> FallbackHeuristicAnalysis(string userQuery, string userEmail = "", string conversationContext = "")
        {
            await Task.Yield();

            var query = userQuery.ToLowerInvariant();
            var now = DateTime.UtcNow;
            var today = now.Date;
            var yesterday = today.AddDays(-1);

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
            else if (query.Contains("influx") || query.Contains("volume") || query.Contains("per hour") || query.Contains("busiest"))
                analysis.QueryType = "influx_requests";
            else if (query.Contains("top tech") || query.Contains("ranking") || query.Contains("best performing"))
                analysis.QueryType = "top_technicians";
            else if (query.Contains("top request") || query.Contains("common request") || query.Contains("top subject") || query.Contains("categories"))
                analysis.QueryType = "top_request_areas";

            // Status
            if (query.Contains("open"))
                analysis.Status = "open";
            else if (query.Contains("closed") || query.Contains("resolved"))
                analysis.Status = "closed";

            // Personalization
            if (query.Contains("my requests") || query.Contains("i opened") || query.Contains("i submitted"))
                analysis.IsUserRequest = true;
            if (query.Contains("assigned to me") || query.Contains("my tickets") || query.Contains("i have"))
                analysis.IsUserTechnician = true;

            // Date handling
            if (query.Contains("yesterday"))
            {
                analysis.DateFrom = yesterday.ToString("yyyy-MM-dd") + " 00:00";
                analysis.DateTo = yesterday.ToString("yyyy-MM-dd") + " 23:59";
            }
            else if (query.Contains("today"))
            {
                analysis.DateFrom = today.ToString("yyyy-MM-dd") + " 00:00";
                analysis.DateTo = today.ToString("yyyy-MM-dd") + " 23:59";
            }
            else if (query.Contains("this month"))
            {
                var monthStart = new DateTime(now.Year, now.Month, 1);
                analysis.DateFrom = monthStart.ToString("yyyy-MM-dd") + " 00:00";
                analysis.DateTo = today.ToString("yyyy-MM-dd") + " 23:59";
            }
            else if (query.Contains("this week"))
            {
                analysis.DateFrom = today.AddDays(-7).ToString("yyyy-MM-dd") + " 00:00";
                analysis.DateTo = today.ToString("yyyy-MM-dd") + " 23:59";
            }
            else
            {
                // Default date range
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
            }

            if (analysis.QueryType == "influx_requests")
                analysis.TimeUnit = query.Contains("day") ? "day" : "hour";

            // Try to extract technician name from query
            var commonPrefixes = new[] { "assigned to ", "tickets for ", "handled by ", "from " };
            foreach (var prefix in commonPrefixes)
            {
                var idx = query.IndexOf(prefix);
                if (idx >= 0)
                {
                    var remainder = query.Substring(idx + prefix.Length).Trim();
                    var endIdx = remainder.IndexOfAny(new[] { ' ', '.', ',', '?' });
                    var name = endIdx > 0 ? remainder.Substring(0, endIdx) : remainder;
                    if (!string.IsNullOrEmpty(name) && name.Length > 2)
                    {
                        analysis.Technician = name;
                        break;
                    }
                }
            }

            // Parse context for follow-up queries
            if (!string.IsNullOrEmpty(conversationContext) && (query.Contains("them") || query.Contains("those") || query.Contains("of them")))
            {
                // Try to extract previous technician from context
                var techMatch = System.Text.RegularExpressions.Regex.Match(conversationContext, @"technician=([^,\]]+)");
                if (techMatch.Success && !string.IsNullOrEmpty(techMatch.Groups[1].Value) && techMatch.Groups[1].Value != "null")
                {
                    analysis.Technician = techMatch.Groups[1].Value.Trim();
                }

                // Try to extract date range from context
                var dateFromMatch = System.Text.RegularExpressions.Regex.Match(conversationContext, @"period=([^ ]+) to");
                if (dateFromMatch.Success)
                {
                    analysis.DateFrom = dateFromMatch.Groups[1].Value + " 00:00";
                }
            }

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

        private async Task<object> GetInactiveTechniciansData(QueryAnalysis analysis)
        {
            var (daysInactive, _) = ParseInactivityPeriod(analysis.InactivityPeriod);
            var dateTo = DateTimeOffset.UtcNow;
            var dateFrom = dateTo.AddDays(-daysInactive);

            var query = _dbContext.ManageEngineRequests
                .Where(r => !string.IsNullOrEmpty(r.TechnicianName));

            if (analysis.Technicians?.Any() ?? false)
            {
                var techList = analysis.Technicians.Select(t => t.ToLower()).ToList();
                query = query.Where(r => techList.Contains(r.TechnicianName.ToLower()));
            }

            if (!string.IsNullOrEmpty(analysis.Technician))
            {
                var techLower = analysis.Technician.ToLower();
                query = query.Where(r => r.TechnicianName.ToLower().Contains(techLower));
            }

            // Phase 1: Get technician activity summary (this translates to SQL)
            var techActivitySummary = await query
                .GroupBy(r => r.TechnicianName)
                .Select(g => new
                {
                    Technician = g.Key,
                    TechnicianEmail = g.Select(r => r.TechnicianEmail).FirstOrDefault(e => !string.IsNullOrEmpty(e)),
                    LastActivity = g.Max(r => r.CreatedTime),
                    TotalRequests = g.Count()
                })
                .ToListAsync();

            // Phase 2: For inactive technicians, fetch their requests separately
            var inactiveTechnicians = techActivitySummary
                .Where(a => a.LastActivity < dateFrom)
                .ToList();

            var result = new List<object>();

            foreach (var tech in inactiveTechnicians)
            {
                // Fetch requests for this technician
                var techRequests = await _dbContext.ManageEngineRequests
                    .Where(r => r.TechnicianName == tech.Technician)
                    .OrderByDescending(r => r.CreatedTime)
                    .Take(100) // Limit to prevent excessive data
                    .Select(r => new
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
                    })
                    .ToListAsync();

                result.Add(new
                {
                    tech.Technician,
                    tech.TechnicianEmail,
                    tech.LastActivity,
                    tech.TotalRequests,
                    DaysInactive = (int)(dateTo - tech.LastActivity).TotalDays,
                    Requests = techRequests.Select(r => ParseRequestDetails(r.JsonData, r)).ToList()
                });
            }

            return new
            {
                QueryType = "InactiveTechnicians",
                InactivityPeriod = analysis.InactivityPeriod,
                Period = $"From {dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                InactiveTechnicians = result,
                TotalInactive = result.Count,
                TotalTechnicians = techActivitySummary.Count,
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
                var techList = analysis.Technicians.Select(t => t.ToLower()).ToList();
                query = query.Where(r => techList.Contains(r.TechnicianName.ToLower()));
            }

            if (!string.IsNullOrEmpty(analysis.Technician))
            {
                var techLower = analysis.Technician.ToLower();
                query = query.Where(r => r.TechnicianName.ToLower().Contains(techLower));
            }

            if (!string.IsNullOrEmpty(analysis.Requester))
            {
                var reqLower = analysis.Requester.ToLower();
                query = query.Where(r => r.RequesterName.ToLower().Contains(reqLower));
            }

            // Fetch all matching requests first (with lightweight projection)
            var allRequests = await query
                .Select(r => new { r.CreatedTime, r.Id, r.DisplayId, r.Subject, r.Status, r.TechnicianName, r.RequesterName, r.JsonData })
                .ToListAsync();

            if (timeUnit == "hour")
            {
                // Group by hour in-memory
                var hourlyGroups = allRequests
                    .GroupBy(r => new { Date = r.CreatedTime.Date, Hour = r.CreatedTime.Hour })
                    .Select(g => new
                    {
                        DateTime = new DateTime(g.Key.Date.Year, g.Key.Date.Month, g.Key.Date.Day, g.Key.Hour, 0, 0),
                        Count = g.Count(),
                        RequestIds = g.Select(r => (r.Id, r.DisplayId, r.Subject, r.Status, r.TechnicianName, r.RequesterName, r.JsonData)).ToList()
                    })
                    .OrderBy(x => x.DateTime)
                    .ToList();

                // Fetch detailed request data for each hour
                var hourlyCounts = new List<object>();
                foreach (var hourGroup in hourlyGroups)
                {
                    hourlyCounts.Add(new
                    {
                        hourGroup.DateTime,
                        hourGroup.Count,
                        Requests = hourGroup.RequestIds.Select(r => ParseRequestDetails(r.JsonData, new
                        {
                            Id = r.Id,
                            DisplayId = r.DisplayId,
                            Subject = r.Subject,
                            Status = r.Status,
                            TechnicianName = r.TechnicianName,
                            RequesterName = r.RequesterName
                        })).ToList()
                    });
                }

                var peakHour = hourlyCounts.OrderByDescending(x => ((dynamic)x).Count).FirstOrDefault();

                return new
                {
                    QueryType = "InfluxRequests",
                    TimeUnit = "Hour",
                    Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                    HourlyData = hourlyCounts,
                    PeakHour = peakHour,
                    TotalRequests = hourlyCounts.Sum(x => ((dynamic)x).Count),
                    Timestamp = DateTime.UtcNow
                };
            }
            else
            {
                // Group by day in-memory
                var dailyGroups = allRequests
                    .GroupBy(r => r.CreatedTime.Date)
                    .Select(g => new
                    {
                        Date = g.Key,
                        Count = g.Count(),
                        RequestIds = g.Select(r => (r.Id, r.DisplayId, r.Subject, r.Status, r.TechnicianName, r.RequesterName, r.JsonData)).ToList()
                    })
                    .OrderBy(x => x.Date)
                    .ToList();

                // Fetch detailed request data for each day
                var dailyCounts = new List<object>();
                foreach (var dayGroup in dailyGroups)
                {
                    dailyCounts.Add(new
                    {
                        dayGroup.Date,
                        dayGroup.Count,
                        Requests = dayGroup.RequestIds.Select(r => ParseRequestDetails(r.JsonData, new
                        {
                            Id = r.Id,
                            DisplayId = r.DisplayId,
                            Subject = r.Subject,
                            Status = r.Status,
                            TechnicianName = r.TechnicianName,
                            RequesterName = r.RequesterName
                        })).ToList()
                    });
                }

                var peakDay = dailyCounts.OrderByDescending(x => ((dynamic)x).Count).FirstOrDefault();

                return new
                {
                    QueryType = "InfluxRequests",
                    TimeUnit = "Day",
                    Period = $"From {dateFrom:yyyy-MM-dd} to {dateTo:yyyy-MM-dd}",
                    DailyData = dailyCounts,
                    PeakDay = peakDay,
                    TotalRequests = dailyCounts.Sum(x => ((dynamic)x).Count),
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
                var techList = analysis.Technicians.Select(t => t.ToLower()).ToList();
                query = query.Where(r => techList.Contains(r.TechnicianName.ToLower()));
            }

            if (!string.IsNullOrEmpty(analysis.Technician))
            {
                var techLower = analysis.Technician.ToLower();
                query = query.Where(r => r.TechnicianName.ToLower().Contains(techLower));
            }

            if (!string.IsNullOrEmpty(analysis.Requester))
            {
                var reqLower = analysis.Requester.ToLower();
                query = query.Where(r => r.RequesterName.ToLower().Contains(reqLower));
            }

            // Phase 1: Get area statistics (this translates to SQL)
            var areaStats = await query
                .GroupBy(r => r.Subject)
                .Select(g => new
                {
                    Subject = g.Key,
                    Count = g.Count()
                })
                .OrderByDescending(x => x.Count)
                .Take(topN)
                .ToListAsync();

            // Phase 2: Fetch detailed requests for each top area
            var detailedAreas = new List<object>();

            foreach (var area in areaStats)
            {
                var areaRequests = await query
                    .Where(r => r.Subject == area.Subject)
                    .OrderByDescending(r => r.CreatedTime)
                    .Take(100) // Limit per subject area
                    .Select(r => new
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
                    })
                    .ToListAsync();

                detailedAreas.Add(new
                {
                    area.Subject,
                    area.Count,
                    Requests = areaRequests.Select(r => ParseRequestDetails(r.JsonData, r)).ToList()
                });
            }

            return new
            {
                QueryType = "TopRequestAreas",
                Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                TopN = topN,
                TopAreas = detailedAreas,
                TotalAreas = detailedAreas.Count,
                TotalRequests = detailedAreas.Sum(x => ((dynamic)x).Count),
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
                var techList = analysis.Technicians.Select(t => t.ToLower()).ToList();
                query = query.Where(r => techList.Contains(r.TechnicianName.ToLower()));
            }

            if (!string.IsNullOrEmpty(analysis.Technician))
            {
                var techLower = analysis.Technician.ToLower();
                query = query.Where(r => r.TechnicianName.ToLower().Contains(techLower));
            }

            if (!string.IsNullOrEmpty(analysis.Requester))
            {
                var reqLower = analysis.Requester.ToLower();
                query = query.Where(r => r.RequesterName.ToLower().Contains(reqLower));
            }

            if (!string.IsNullOrEmpty(analysis.Status))
            {
                query = ApplyStatusFilter(query, analysis.Status);
            }

            // Phase 1: Get technician statistics (translates to SQL)
            var technicianStats = await query
                .GroupBy(r => r.TechnicianName)
                .Select(g => new
                {
                    Technician = g.Key,
                    TechnicianEmail = g.Select(r => r.TechnicianEmail).FirstOrDefault(e => !string.IsNullOrEmpty(e)),
                    RequestsHandled = g.Count()
                })
                .OrderByDescending(x => x.RequestsHandled)
                .Take(topN)
                .ToListAsync();

            // Phase 2: Fetch detailed requests for each top technician
            var detailedStats = new List<object>();

            foreach (var tech in technicianStats)
            {
                var techRequests = await query
                    .Where(r => r.TechnicianName == tech.Technician)
                    .OrderByDescending(r => r.CreatedTime)
                    .Take(500) // Limit per technician
                    .Select(r => new
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
                    })
                    .ToListAsync();

                var parsedRequests = techRequests.Select(r => ParseRequestDetails(r.JsonData, r)).ToList();

                detailedStats.Add(new
                {
                    tech.Technician,
                    tech.TechnicianEmail,
                    tech.RequestsHandled,
                    OpenRequests = parsedRequests.Count(r => IsOpenStatus(r.StatusInternal)),
                    ClosedRequests = parsedRequests.Count(r => IsClosedStatus(r.StatusInternal)),
                    Requests = parsedRequests
                });
            }

            return new
            {
                QueryType = "TopTechnicians",
                Period = $"From {dateFrom:yyyy-MM-dd HH:mm} to {dateTo:yyyy-MM-dd HH:mm}",
                TopN = topN,
                TopTechnicians = detailedStats,
                TotalTechnicians = detailedStats.Count,
                TotalRequests = detailedStats.Sum(x => ((dynamic)x).RequestsHandled),
                Timestamp = DateTime.UtcNow
            };
        }

        /// <summary>
        /// FIXED: Search requests using JsonData contains for proper technician email matching
        /// </summary>
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

            // FIXED: Enhanced personalization filtering - search in JsonData
            if (analysis.IsUserTechnician && !string.IsNullOrEmpty(userEmail))
            {
                // Search for technician email in JsonData column as well as TechnicianEmail column
                query = query.Where(r =>
                    r.TechnicianEmail == userEmail ||
                    r.JsonData.Contains($"\"email_id\":\"{userEmail}\"") ||
                    r.JsonData.Contains(userEmail)
                );
            }
            else if (analysis.IsUserRequest && !string.IsNullOrEmpty(userEmail))
            {
                // Search for requester email in JsonData column as well as RequesterEmail column
                query = query.Where(r =>
                    r.RequesterEmail == userEmail ||
                    r.JsonData.Contains($"\"email_id\":\"{userEmail}\"") ||
                    r.JsonData.Contains(userEmail)
                );
            }
            else
            {
                // Apply technician filters - also search in JsonData
                if (analysis.Technicians?.Any() ?? false)
                {
                    var techList = analysis.Technicians.Select(t => t.ToLower()).ToList();
                    query = query.Where(r =>
                        techList.Any(t => r.TechnicianName.ToLower().Contains(t)) ||
                        techList.Any(t => r.JsonData.ToLower().Contains(t))
                    );
                }

                if (!string.IsNullOrEmpty(analysis.Technician))
                {
                    var techLower = analysis.Technician.ToLower();
                    query = query.Where(r =>
                        r.TechnicianName.ToLower().Contains(techLower) ||
                        r.JsonData.ToLower().Contains(techLower)
                    );
                }

                // Apply requester filter
                if (!string.IsNullOrEmpty(analysis.Requester))
                {
                    var reqLower = analysis.Requester.ToLower();
                    query = query.Where(r =>
                        r.RequesterName.ToLower().Contains(reqLower) ||
                        r.RequesterEmail.ToLower().Contains(reqLower) ||
                        r.JsonData.ToLower().Contains(reqLower)
                    );
                }
            }

            // Filter by subject if specified
            if (!string.IsNullOrEmpty(analysis.Subject))
            {
                var subjLower = analysis.Subject.ToLower();
                query = query.Where(r => r.Subject.ToLower().Contains(subjLower));
            }

            var requests = await query
                .OrderByDescending(r => r.CreatedTime)
                .Take(analysis.TopN ?? 500) // Increased limit to get more results
                .ToListAsync();

            // Post-process to verify technician match when filtering by user email
            if (analysis.IsUserTechnician && !string.IsNullOrEmpty(userEmail))
            {
                requests = requests.Where(r =>
                {
                    // Check direct column match
                    if (r.TechnicianEmail?.Equals(userEmail, StringComparison.OrdinalIgnoreCase) == true)
                        return true;

                    // Check JsonData for technician email
                    if (!string.IsNullOrEmpty(r.JsonData))
                    {
                        try
                        {
                            var data = JsonSerializer.Deserialize<ManageEngineRequestData>(r.JsonData);
                            if (data?.Technician?.EmailId?.Equals(userEmail, StringComparison.OrdinalIgnoreCase) == true)
                                return true;
                        }
                        catch { }
                    }
                    return false;
                }).ToList();
            }

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
                UserEmail = userEmail,
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
                    r.Status.ToLower().Contains("open") ||
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
                    r.Status.ToLower().Contains("closed") ||
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
            return lower == "open" || lower == "in progress" || lower == "pending" || lower.Contains("open");
        }

        private bool IsClosedStatus(string internalStatus)
        {
            if (string.IsNullOrEmpty(internalStatus)) return false;
            var lower = internalStatus.ToLower();
            return lower == "closed" || lower == "resolved" || lower == "completed" || lower.Contains("closed");
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
                    var inactiveCount = jsonElement.GetProperty("TotalInactive").GetInt32();
                    var totalTechs = jsonElement.GetProperty("TotalTechnicians").GetInt32();
                    sb.AppendLine($"Found {inactiveCount} inactive technicians out of {totalTechs} total.");

                    var inactive = jsonElement.GetProperty("InactiveTechnicians").EnumerateArray().Take(10).ToList();
                    if (inactive.Any())
                    {
                        sb.AppendLine("Top inactive:");
                        foreach (var tech in inactive)
                        {
                            sb.AppendLine($"• {tech.GetProperty("Technician").GetString()} - {tech.GetProperty("DaysInactive").GetInt32()} days inactive");
                        }
                    }
                    break;

                case "top_technicians":
                    var topTechs = jsonElement.GetProperty("TopTechnicians").EnumerateArray().Take(10).ToList();
                    var totalReqs = jsonElement.GetProperty("TotalRequests").GetInt32();
                    sb.AppendLine($"Top {topTechs.Count} technicians handled {totalReqs} total requests:");

                    int rank = 1;
                    foreach (var tech in topTechs)
                    {
                        var name = tech.GetProperty("Technician").GetString();
                        var handled = tech.GetProperty("RequestsHandled").GetInt32();
                        var open = tech.GetProperty("OpenRequests").GetInt32();
                        var closed = tech.GetProperty("ClosedRequests").GetInt32();
                        sb.AppendLine($"{rank}. {name} - {handled} requests (Open: {open}, Closed: {closed})");
                        rank++;
                    }
                    break;

                case "top_request_areas":
                    var topAreas = jsonElement.GetProperty("TopAreas").EnumerateArray().Take(10).ToList();
                    var areasTotal = jsonElement.GetProperty("TotalRequests").GetInt32();
                    sb.AppendLine($"Top {topAreas.Count} categories ({areasTotal} total requests):");

                    foreach (var area in topAreas)
                    {
                        sb.AppendLine($"• {area.GetProperty("Subject").GetString()}: {area.GetProperty("Count").GetInt32()} requests");
                    }
                    break;

                case "influx_requests":
                    var influxTotal = jsonElement.GetProperty("TotalRequests").GetInt32();
                    if (jsonElement.TryGetProperty("HourlyData", out var hourly))
                    {
                        var peak = jsonElement.GetProperty("PeakHour");
                        var peakTime = peak.GetProperty("DateTime").GetDateTime();
                        var peakCount = peak.GetProperty("Count").GetInt32();
                        sb.AppendLine($"Request influx by hour. Total: {influxTotal}");
                        sb.AppendLine($"Peak: {peakTime:MMM dd} at {peakTime:HH:00} with {peakCount} requests");

                        // Show hourly breakdown
                        sb.AppendLine("\nHourly breakdown:");
                        foreach (var h in hourly.EnumerateArray().Take(24))
                        {
                            var dt = h.GetProperty("DateTime").GetDateTime();
                            var cnt = h.GetProperty("Count").GetInt32();
                            sb.AppendLine($"• {dt:MMM dd HH:00}: {cnt} requests");
                        }
                    }
                    else if (jsonElement.TryGetProperty("DailyData", out var daily))
                    {
                        var peak = jsonElement.GetProperty("PeakDay");
                        var peakDate = peak.GetProperty("Date").GetDateTime();
                        var peakCount = peak.GetProperty("Count").GetInt32();
                        sb.AppendLine($"Request influx by day. Total: {influxTotal}");
                        sb.AppendLine($"Peak: {peakDate:MMM dd} with {peakCount} requests.");
                    }
                    break;

                case "request_search":
                    var reqFound = jsonElement.GetProperty("RequestsFound").GetInt32();
                    sb.AppendLine($"Found {reqFound} matching requests.");

                    if (jsonElement.TryGetProperty("Requests", out var reqs) && reqs.GetArrayLength() > 0)
                    {
                        sb.AppendLine("\nSample requests:");
                        foreach (var req in reqs.EnumerateArray().Take(5))
                        {
                            var subj = req.TryGetProperty("Subject", out var s) ? s.GetString() : "";
                            var status = req.TryGetProperty("Status", out var st) ? st.GetString() : "";
                            var tech = req.TryGetProperty("TechnicianName", out var t) ? t.GetString() : "";
                            sb.AppendLine($"• {subj} | Status: {status} | Tech: {tech}");
                        }
                    }
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
        /// Run an agent with proper error handling and retry logic
        /// </summary>
        private async Task<List<string>> RunAgentAsync(
            PersistentAgentsClient client,
            string threadId,
            string agentId,
            string userMessage,
            string additionalInstructions)
        {
            var responses = new List<string>();
            int maxRetries = 3;
            int currentRetry = 0;

            while (currentRetry < maxRetries)
            {
                try
                {
                    // Add message to thread
                    await client.Messages.CreateMessageAsync(threadId, MessageRole.User, userMessage);

                    // Create and run the agent
                    var runResponse = await client.Runs.CreateRunAsync(threadId, agentId, additionalInstructions: additionalInstructions);
                    var run = runResponse.Value;

                    // Poll for completion
                    var start = DateTime.UtcNow;
                    var maxDuration = TimeSpan.FromSeconds(75);

                    while (run.Status == RunStatus.Queued || run.Status == RunStatus.InProgress || run.Status == RunStatus.RequiresAction)
                    {
                        if (DateTime.UtcNow - start > maxDuration)
                        {
                            responses.Add("Agent timeout while waiting for completion.");
                            Console.WriteLine($"Agent run timeout after {(DateTime.UtcNow - start).TotalSeconds:F1}s. Status: {run.Status}");
                            return responses;
                        }

                        await Task.Delay(750);
                        run = (await client.Runs.GetRunAsync(threadId, run.Id)).Value;

                        if (run.Status == RunStatus.RequiresAction)
                        {
                            responses.Add("Agent requires an action that this server does not implement.");
                            Console.WriteLine("Run requires action; exiting polling loop.");
                            return responses;
                        }
                    }

                    if (run.Status == RunStatus.Failed)
                    {
                        responses.Add($"Agent failed: {run.LastError?.Message ?? "Unknown error"}");
                        return responses;
                    }

                    // Get the response messages
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

                    return responses;
                }
                catch (RequestFailedException rfe) when (rfe.Message.Contains("while a run") && rfe.Message.Contains("is active"))
                {
                    // Thread is busy with another run, wait and retry
                    currentRetry++;
                    Console.WriteLine($"Thread busy, waiting before retry {currentRetry}/{maxRetries}...");
                    await Task.Delay(2000 * currentRetry); // Exponential backoff

                    // Wait for active runs to complete
                    await WaitForActiveRunsToComplete(client, threadId);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Agent execution error: {ex.Message}");
                    responses.Add($"Error: {ex.Message}");
                    return responses;
                }
            }

            responses.Add("Failed after maximum retries due to busy thread.");
            return responses;
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
                        sb.AppendLine("Subject,Total Count,Request ID,Display ID,Status,Created Time,Technician,Requester,Priority");
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

    // Helper Cl
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

    // Data models
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