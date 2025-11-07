using OpenAI.Chat;

namespace documentchecker.Models
{
    public class ChatConversation
    {
        public int Id { get; set; }
        public string SessionId { get; set; } = Guid.NewGuid().ToString();
        public DateTime StartedAt { get; set; } = DateTime.UtcNow;
        public DateTime? EndedAt { get; set; }
        public ICollection<ChatMessage> Messages { get; set; } = new List<ChatMessage>();
    }
}
