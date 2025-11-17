namespace documentchecker.Models
{
    public class ManageEngineRequest
    {
        public string Id { get; set; } // API ID as primary key
        public string Subject { get; set; }
        public DateTimeOffset CreatedTime { get; set; }
        public string TechnicianName { get; set; }
        public string JsonData { get; set; } // Full JSON for additional fields
    }
}
