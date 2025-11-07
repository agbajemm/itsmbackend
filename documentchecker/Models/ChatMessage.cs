namespace documentchecker.Models
{
    public class ChatMessage
    {
        public int Id { get; set; }
        public int ConversationId { get; set; }
        public string Role { get; set; } = null!; // "user" or "agent"
        public string Content { get; set; } = null!;
        public DateTime SentAt { get; set; } = DateTime.UtcNow;

        public ChatConversation Conversation { get; set; } = null!;
    }
}
