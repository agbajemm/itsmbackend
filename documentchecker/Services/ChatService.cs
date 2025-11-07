using documentchecker.Models;
using Microsoft.EntityFrameworkCore;

namespace documentchecker.Services

{
    public class ChatService
    {
        private readonly AppDbContext _db;
        public ChatService(AppDbContext db) => _db = db;

        public async Task<int> StartConversationAsync()
        {
            var conv = new ChatConversation();
            _db.ChatConversations.Add(conv);
            await _db.SaveChangesAsync();
            return conv.Id;
        }

        public async Task AddMessageAsync(int conversationId, string role, string content)
        {
            var msg = new ChatMessage
            {
                ConversationId = conversationId,
                Role = role,
                Content = content
            };
            _db.ChatMessages.Add(msg);
            await _db.SaveChangesAsync();
        }

        public async Task<List<ChatMessage>> GetConversationAsync(int conversationId)
        {
            return await _db.ChatMessages
                .Where(m => m.ConversationId == conversationId)
                .OrderBy(m => m.SentAt)
                .ToListAsync();
        }
    }
}