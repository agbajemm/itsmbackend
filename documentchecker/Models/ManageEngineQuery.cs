namespace documentchecker.Models
{
    public class ManageEngineQuery
    {
        public int Id { get; set; }
        public string QueryId { get; set; } = Guid.NewGuid().ToString();
        public string? Subject { get; set; }
        public string? DateFrom { get; set; }
        public string? DateTo { get; set; }
        public string? Technician { get; set; }
        public int TotalResults { get; set; }
        public int FilteredResults { get; set; }
        public DateTime ExecutedAt { get; set; } = DateTime.UtcNow;
        public string ExecutedBy { get; set; } = "API"; // or User.Identity.Name
        public string? CacheKey { get; set; }
    }
}
