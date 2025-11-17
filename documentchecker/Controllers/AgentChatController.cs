using Azure.AI.Agents.Persistent;
using Azure.Identity;
using documentchecker.Models;
using documentchecker.Services;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace documentchecker.Controllers
{
    [ApiController]
    [Route("api")]
    public class AgentChatController : ControllerBase
    {
        private readonly string _projectEndpoint;
        private readonly string _agentId;
        private readonly ChatService _chatService;

        public AgentChatController(
            IConfiguration configuration,
            ChatService chatService)
        {
            _projectEndpoint = configuration["AzureAIFoundry:ProjectEndpoint"] ?? throw new InvalidOperationException("ProjectEndpoint not configured.");
            _agentId = configuration["AzureAIFoundry:AgentId"] ?? "asst_MqwY6PBQdS9uxha6Hl1RQCJk";
            _chatService = chatService;
        }

        [HttpPost("agent-chat")]
        public async Task<IActionResult> AgentChat([FromBody] AgentChatRequest request)
        {
            if (string.IsNullOrEmpty(request.Message))
            {
                return BadRequest("Message is required.");
            }
            // Start conversation in DB
            var conversationId = await _chatService.StartConversationAsync();
            await _chatService.AddMessageAsync(conversationId, "user", request.Message);
            try
            {
                var client = new PersistentAgentsClient(_projectEndpoint, new DefaultAzureCredential());
                var agentResponse = await client.Administration.GetAgentAsync(_agentId);
                PersistentAgent agent = agentResponse.Value;
                var threadResponse = await client.Threads.CreateThreadAsync();
                PersistentAgentThread thread = threadResponse.Value;
                await client.Messages.CreateMessageAsync(thread.Id, MessageRole.User, request.Message);
                var runResponse = await client.Runs.CreateRunAsync(
                    thread.Id,
                    _agentId,
                    additionalInstructions: request.AdditionalInstructions
                );
                ThreadRun run = runResponse.Value;
                do
                {
                    await Task.Delay(500);
                    var getRunResponse = await client.Runs.GetRunAsync(thread.Id, run.Id);
                    run = getRunResponse.Value;
                }
                while (run.Status == RunStatus.Queued || run.Status == RunStatus.InProgress || run.Status == RunStatus.RequiresAction);
                if (run.Status == RunStatus.Failed)
                {
                    await _chatService.AddMessageAsync(conversationId, "system", $"Run failed: {run.LastError?.Message}");
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
                                await _chatService.AddMessageAsync(conversationId, "agent", textContent.Text);
                            }
                        }
                    }
                }
                return Ok(new { ConversationId = conversationId, Responses = agentResponses });
            }
            catch (Exception ex)
            {
                await _chatService.AddMessageAsync(conversationId, "system", $"Error: {ex.Message}");
                return StatusCode(500, new { Error = ex.Message });
            }
        }

        [HttpGet("chat-history/{conversationId}")]
        public async Task<IActionResult> GetChatHistory(int conversationId)
        {
            var messages = await _chatService.GetConversationAsync(conversationId);
            if (!messages.Any())
            {
                return NotFound("Conversation not found.");
            }
            return Ok(new { Messages = messages });
        }

        [HttpPost("convert-to-base64")]
        public async Task<IActionResult> ConvertToBase64(IFormFile file)
        {
            if (file == null || file.Length == 0)
            {
                return BadRequest("File is required.");
            }

            try
            {
                using var memoryStream = new MemoryStream();
                await file.CopyToAsync(memoryStream);
                var fileBytes = memoryStream.ToArray();
                var base64String = Convert.ToBase64String(fileBytes);
                return Ok(new { Base64 = base64String });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = $"Conversion failed: {ex.Message}" });
            }
        }

        [HttpPost("convert-from-base64")]
        public IActionResult ConvertFromBase64([FromBody] Base64ToFileRequest request)
        {
            if (string.IsNullOrEmpty(request.Base64))
            {
                return BadRequest("Base64 string is required.");
            }

            var fileName = string.IsNullOrEmpty(request.FileName) ? "decoded_file" : request.FileName;
            var fileExtension = string.IsNullOrEmpty(request.FileExtension) ? "" : request.FileExtension.StartsWith(".") ? request.FileExtension : $".{request.FileExtension}";

            try
            {
                var fileBytes = Convert.FromBase64String(request.Base64);
                var contentType = GetContentType(fileExtension); // Helper to get MIME type
                return File(fileBytes, contentType, $"{fileName}{fileExtension}");
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Error = $"Conversion failed: {ex.Message}" });
            }
        }

        private string GetContentType(string fileExtension)
        {
            return fileExtension.ToLower() switch
            {
                ".docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ".pdf" => "application/pdf",
                ".txt" => "text/plain",
                _ => "application/octet-stream" // Default binary
            };
        }
    }

    public class AgentChatRequest
    {
        public string Message { get; set; } = string.Empty;
        public string? AdditionalInstructions { get; set; }
    }

    public class Base64ToFileRequest
    {
        public string Base64 { get; set; } = string.Empty;
        public string? FileName { get; set; }
        public string? FileExtension { get; set; }
    }
}