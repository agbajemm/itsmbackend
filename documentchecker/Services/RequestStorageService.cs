using documentchecker.Models;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace documentchecker.Services
{
    public class RequestStorageService
    {
        private readonly AppDbContext _dbContext;

        public RequestStorageService(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        /// <summary>
        /// Check if a request already exists in the database
        /// </summary>
        public async Task<bool> RequestExistsAsync(string requestId)
        {
            return await _dbContext.ManageEngineRequests.AnyAsync(r => r.Id == requestId);
        }

        /// <summary>
        /// Store a request from Zoho API response
        /// </summary>
        public async Task StoreRequestAsync(Dictionary<string, object> req)
        {
            if (req == null)
                throw new ArgumentNullException(nameof(req));

            // Extract created_time
            if (!req.TryGetValue("created_time", out var createdTimeObj) ||
                !(createdTimeObj is JsonElement createdTimeElem) ||
                createdTimeElem.ValueKind != JsonValueKind.Object)
            {
                throw new Exception("Invalid created_time format in request.");
            }

            if (!createdTimeElem.TryGetProperty("value", out var valElem) || valElem.ValueKind != JsonValueKind.String)
                throw new Exception("Invalid created_time value format.");

            if (!long.TryParse(valElem.GetString(), out var createdTimeMs))
                throw new Exception("Could not parse created_time timestamp.");

            var createdTime = DateTimeOffset.FromUnixTimeMilliseconds(createdTimeMs);

            // Extract technician name
            var technicianName = string.Empty;
            if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
            {
                if (techElem.TryGetProperty("name", out var nameElem) && nameElem.ValueKind == JsonValueKind.String)
                {
                    technicianName = nameElem.GetString() ?? string.Empty;
                }
            }

            // Extract subject
            var subject = string.Empty;
            if (req.TryGetValue("subject", out var subjObj) && subjObj is JsonElement subjElem && subjElem.ValueKind == JsonValueKind.String)
            {
                subject = subjElem.GetString() ?? string.Empty;
            }

            // Create and store entity
            var entity = new ManageEngineRequest
            {
                Id = req["id"]?.ToString() ?? Guid.NewGuid().ToString(),
                Subject = subject,
                CreatedTime = createdTime,
                TechnicianName = technicianName,
                JsonData = JsonSerializer.Serialize(req)
            };

            _dbContext.ManageEngineRequests.Add(entity);
            await _dbContext.SaveChangesAsync();
        }

        /// <summary>
        /// Get the most recent request creation date from storage
        /// </summary>
        public async Task<DateTimeOffset?> GetLastStoredDateAsync()
        {
            var lastRequest = await _dbContext.ManageEngineRequests
                .OrderByDescending(r => r.CreatedTime)
                .FirstOrDefaultAsync();

            return lastRequest?.CreatedTime;
        }

        /// <summary>
        /// Get request statistics
        /// </summary>
        public async Task<RequestStorageStats> GetStatsAsync()
        {
            var totalCount = await _dbContext.ManageEngineRequests.CountAsync();
            var oldestDate = await _dbContext.ManageEngineRequests
                .OrderBy(r => r.CreatedTime)
                .Select(r => r.CreatedTime)
                .FirstOrDefaultAsync();
            var newestDate = await _dbContext.ManageEngineRequests
                .OrderByDescending(r => r.CreatedTime)
                .Select(r => r.CreatedTime)
                .FirstOrDefaultAsync();

            var technicianCount = await _dbContext.ManageEngineRequests
                .Where(r => !string.IsNullOrEmpty(r.TechnicianName))
                .Select(r => r.TechnicianName)
                .Distinct()
                .CountAsync();

            return new RequestStorageStats
            {
                TotalRequests = totalCount,
                OldestDate = oldestDate,
                NewestDate = newestDate,
                UniqueTechnicians = technicianCount
            };
        }

        /// <summary>
        /// Clear all stored requests
        /// </summary>
        public async Task ClearAllAsync()
        {
            await _dbContext.ManageEngineRequests.ExecuteDeleteAsync();
        }
    }

    public class RequestStorageStats
    {
        public int TotalRequests { get; set; }
        public DateTimeOffset OldestDate { get; set; }
        public DateTimeOffset NewestDate { get; set; }
        public int UniqueTechnicians { get; set; }
    }
}