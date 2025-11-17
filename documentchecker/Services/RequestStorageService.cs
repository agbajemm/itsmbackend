using documentchecker.Models;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace documentchecker.Services
{
    public class RequestStorageService
    {
        private readonly AppDbContext _dbContext; // Assuming you have a DbContext

        public RequestStorageService(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task<bool> RequestExistsAsync(string requestId)
        {
            return await _dbContext.ManageEngineRequests.AnyAsync(r => r.Id == requestId);
        }

        public async Task StoreRequestAsync(Dictionary<string, object> req)
        {
            if (!req.TryGetValue("created_time", out var createdTimeObj) || createdTimeObj is not JsonElement createdTimeElem || createdTimeElem.ValueKind != JsonValueKind.Object || !createdTimeElem.TryGetProperty("value", out var valElem) || valElem.ValueKind != JsonValueKind.String)
            {
                throw new Exception("Invalid created_time format.");
            }

            if (!long.TryParse(valElem.GetString(), out var createdTimeMs))
            {
                throw new Exception("Invalid value in created_time.");
            }
            var createdTime = DateTimeOffset.FromUnixTimeMilliseconds(createdTimeMs);

            var technicianName = string.Empty;
            if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
            {
                if (techElem.TryGetProperty("name", out var nameElem) && nameElem.ValueKind == JsonValueKind.String)
                {
                    technicianName = nameElem.GetString() ?? string.Empty;
                }
            }

            var entity = new ManageEngineRequest
            {
                Id = req["id"].ToString(),
                Subject = req.TryGetValue("subject", out var subjObj) && subjObj is JsonElement subjElem && subjElem.ValueKind == JsonValueKind.String ? subjElem.GetString() : null,
                CreatedTime = createdTime,
                TechnicianName = technicianName,
                JsonData = JsonSerializer.Serialize(req)
            };

            _dbContext.ManageEngineRequests.Add(entity);
            await _dbContext.SaveChangesAsync();
        }

        public async Task<DateTimeOffset?> GetLastStoredDateAsync()
        {
            var lastRequest = await _dbContext.ManageEngineRequests.OrderByDescending(r => r.CreatedTime).FirstOrDefaultAsync();
            return lastRequest?.CreatedTime;
        }
    }
}
