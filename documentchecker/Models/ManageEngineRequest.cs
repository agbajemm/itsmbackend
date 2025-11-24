using documentchecker.Controllers;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json;

namespace documentchecker.Models
{
    public class ManageEngineRequest
    {
        [Key]
        public string Id { get; set; }

        public string Subject { get; set; }

        public string TechnicianName { get; set; }

        public string TechnicianEmail { get; set; }  // New property

        public DateTimeOffset CreatedTime { get; set; }

        public string JsonData { get; set; }

        // New properties for enhanced functionality - make them nullable
        public string? Status { get; set; }

        public string? RequesterName { get; set; }

        public string? RequesterEmail { get; set; }

        public string? DisplayId { get; set; }

        [NotMapped]
        public ManageEngineRequestData? ParsedData =>
            string.IsNullOrEmpty(JsonData) ? null : JsonSerializer.Deserialize<ManageEngineRequestData>(JsonData);
    }
}
