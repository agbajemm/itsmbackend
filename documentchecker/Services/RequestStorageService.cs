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
            if (!req.TryGetValue("created_time", out var createdTimeObj) || createdTimeObj is not Dictionary<string, object> createdTimeDict || !createdTimeDict.TryGetValue("milliseconds", out var msObj))
            {
                throw new Exception("Invalid created_time format.");
            }

            var createdTimeMs = Convert.ToInt64(msObj);
            var createdTime = DateTimeOffset.FromUnixTimeMilliseconds(createdTimeMs);

            var technicianName = string.Empty;
            if (req.TryGetValue("technician", out var techObj) && techObj is Dictionary<string, object> techDict && techDict.TryGetValue("name", out var nameObj))
            {
                technicianName = nameObj?.ToString() ?? string.Empty;
            }

            var entity = new ManageEngineRequest
            {
                Id = req["id"].ToString(),
                Subject = req.TryGetValue("subject", out var subjObj) ? subjObj?.ToString() : null,
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
