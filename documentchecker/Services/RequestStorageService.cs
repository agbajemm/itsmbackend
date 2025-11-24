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
        /// Store a request from Zoho API response with enhanced data parsing
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

            if (!createdTimeElem.TryGetProperty("value", out var valElem))
                throw new Exception("Invalid created_time value format.");

            string valueStr = valElem.ValueKind == JsonValueKind.String ? valElem.GetString() :
                              valElem.ValueKind == JsonValueKind.Number ? valElem.GetInt64().ToString() : null;

            if (string.IsNullOrEmpty(valueStr) || !long.TryParse(valueStr, out var createdTimeMs))
                throw new Exception("Could not parse created_time timestamp.");

            var createdTime = DateTimeOffset.FromUnixTimeMilliseconds(createdTimeMs);

            // Extract technician name and email
            var technicianName = string.Empty;
            var technicianEmail = string.Empty;
            if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
            {
                if (techElem.TryGetProperty("name", out var nameElem) && nameElem.ValueKind == JsonValueKind.String)
                {
                    technicianName = nameElem.GetString() ?? string.Empty;
                }
                if (techElem.TryGetProperty("email_id", out var emailElem) && emailElem.ValueKind == JsonValueKind.String)
                {
                    technicianEmail = emailElem.GetString() ?? string.Empty;
                }
            }

            // Extract subject
            var subject = string.Empty;
            if (req.TryGetValue("subject", out var subjObj) && subjObj is JsonElement subjElem && subjElem.ValueKind == JsonValueKind.String)
            {
                subject = subjElem.GetString() ?? string.Empty;
            }

            // Extract status
            var status = "unknown";
            if (req.TryGetValue("status", out var statusObj) && statusObj is JsonElement statusElem && statusElem.ValueKind == JsonValueKind.Object)
            {
                if (statusElem.TryGetProperty("name", out var statusNameElem) && statusNameElem.ValueKind == JsonValueKind.String)
                {
                    status = statusNameElem.GetString()?.ToLower() ?? "unknown";
                }
            }

            // Extract requester information
            var requesterName = string.Empty;
            var requesterEmail = string.Empty;
            if (req.TryGetValue("requester", out var requesterObj) && requesterObj is JsonElement requesterElem && requesterElem.ValueKind == JsonValueKind.Object)
            {
                if (requesterElem.TryGetProperty("name", out var reqNameElem) && reqNameElem.ValueKind == JsonValueKind.String)
                {
                    requesterName = reqNameElem.GetString() ?? string.Empty;
                }
                if (requesterElem.TryGetProperty("email_id", out var reqEmailElem) && reqEmailElem.ValueKind == JsonValueKind.String)
                {
                    requesterEmail = reqEmailElem.GetString() ?? string.Empty;
                }
            }

            // Extract display_id
            var displayId = string.Empty;
            if (req.TryGetValue("display_id", out var displayIdObj) && displayIdObj is JsonElement displayIdElem && displayIdElem.ValueKind == JsonValueKind.String)
            {
                displayId = displayIdElem.GetString() ?? string.Empty;
            }
            // Alternative: try display_key if display_id is not available
            else if (req.TryGetValue("display_key", out var displayKeyObj) && displayKeyObj is JsonElement displayKeyElem && displayKeyElem.ValueKind == JsonValueKind.Object)
            {
                if (displayKeyElem.TryGetProperty("value", out var displayKeyValueElem) && displayKeyValueElem.ValueKind == JsonValueKind.String)
                {
                    displayId = displayKeyValueElem.GetString() ?? string.Empty;
                }
            }

            // Create and store entity with enhanced properties
            var entity = new ManageEngineRequest
            {
                Id = req["id"]?.ToString() ?? Guid.NewGuid().ToString(),
                Subject = subject,
                CreatedTime = createdTime,
                TechnicianName = technicianName,
                TechnicianEmail = technicianEmail,
                JsonData = JsonSerializer.Serialize(req),
                Status = status,
                RequesterName = requesterName,
                RequesterEmail = requesterEmail,
                DisplayId = displayId
            };

            _dbContext.ManageEngineRequests.Add(entity);
            await _dbContext.SaveChangesAsync();
        }

        /// <summary>
        /// Store multiple requests in bulk for better performance
        /// </summary>
        public async Task<int> StoreRequestsBulkAsync(List<Dictionary<string, object>> requests)
        {
            if (requests == null || !requests.Any())
                return 0;

            var existingIds = await _dbContext.ManageEngineRequests
                .Select(r => r.Id)
                .ToListAsync();

            var newRequests = new List<ManageEngineRequest>();
            var batchSize = 100;
            var storedCount = 0;

            foreach (var req in requests)
            {
                try
                {
                    var requestId = req["id"]?.ToString();
                    if (string.IsNullOrEmpty(requestId) || existingIds.Contains(requestId))
                        continue;

                    var entity = await ParseRequestToEntityAsync(req);
                    if (entity != null)
                    {
                        newRequests.Add(entity);
                        // Save in batches to avoid memory issues
                        if (newRequests.Count >= batchSize)
                        {
                            _dbContext.ManageEngineRequests.AddRange(newRequests);
                            await _dbContext.SaveChangesAsync();
                            storedCount += newRequests.Count;
                            newRequests.Clear();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error parsing request {req["id"]}: {ex.Message}");
                    // Continue with next request
                }
            }

            // Save any remaining requests
            if (newRequests.Any())
            {
                _dbContext.ManageEngineRequests.AddRange(newRequests);
                await _dbContext.SaveChangesAsync();
                storedCount += newRequests.Count;
            }

            return storedCount;
        }

        /// <summary>
        /// Parse request dictionary to ManageEngineRequest entity
        /// </summary>
        private async Task<ManageEngineRequest> ParseRequestToEntityAsync(Dictionary<string, object> req)
        {
            if (req == null)
                return null;

            // Extract created_time
            if (!req.TryGetValue("created_time", out var createdTimeObj) ||
                !(createdTimeObj is JsonElement createdTimeElem) ||
                createdTimeElem.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            if (!createdTimeElem.TryGetProperty("value", out var valElem))
                return null;

            string valueStr = valElem.ValueKind == JsonValueKind.String ? valElem.GetString() :
                              valElem.ValueKind == JsonValueKind.Number ? valElem.GetInt64().ToString() : null;

            if (string.IsNullOrEmpty(valueStr) || !long.TryParse(valueStr, out var createdTimeMs))
                return null;

            var createdTime = DateTimeOffset.FromUnixTimeMilliseconds(createdTimeMs);

            // Extract technician name and email
            var technicianName = string.Empty;
            var technicianEmail = string.Empty;
            if (req.TryGetValue("technician", out var techObj) && techObj is JsonElement techElem && techElem.ValueKind == JsonValueKind.Object)
            {
                if (techElem.TryGetProperty("name", out var nameElem) && nameElem.ValueKind == JsonValueKind.String)
                {
                    technicianName = nameElem.GetString() ?? string.Empty;
                }
                if (techElem.TryGetProperty("email_id", out var emailElem) && emailElem.ValueKind == JsonValueKind.String)
                {
                    technicianEmail = emailElem.GetString() ?? string.Empty;
                }
            }

            // Extract subject
            var subject = string.Empty;
            if (req.TryGetValue("subject", out var subjObj) && subjObj is JsonElement subjElem && subjElem.ValueKind == JsonValueKind.String)
            {
                subject = subjElem.GetString() ?? string.Empty;
            }

            // Extract status
            var status = "unknown";
            if (req.TryGetValue("status", out var statusObj) && statusObj is JsonElement statusElem && statusElem.ValueKind == JsonValueKind.Object)
            {
                if (statusElem.TryGetProperty("name", out var statusNameElem) && statusNameElem.ValueKind == JsonValueKind.String)
                {
                    status = statusNameElem.GetString()?.ToLower() ?? "unknown";
                }
            }

            // Extract requester information
            var requesterName = string.Empty;
            var requesterEmail = string.Empty;
            if (req.TryGetValue("requester", out var requesterObj) && requesterObj is JsonElement requesterElem && requesterElem.ValueKind == JsonValueKind.Object)
            {
                if (requesterElem.TryGetProperty("name", out var reqNameElem) && reqNameElem.ValueKind == JsonValueKind.String)
                {
                    requesterName = reqNameElem.GetString() ?? string.Empty;
                }
                if (requesterElem.TryGetProperty("email_id", out var reqEmailElem) && reqEmailElem.ValueKind == JsonValueKind.String)
                {
                    requesterEmail = reqEmailElem.GetString() ?? string.Empty;
                }
            }

            // Extract display_id
            var displayId = string.Empty;
            if (req.TryGetValue("display_id", out var displayIdObj) && displayIdObj is JsonElement displayIdElem && displayIdElem.ValueKind == JsonValueKind.String)
            {
                displayId = displayIdElem.GetString() ?? string.Empty;
            }
            else if (req.TryGetValue("display_key", out var displayKeyObj) && displayKeyObj is JsonElement displayKeyElem && displayKeyElem.ValueKind == JsonValueKind.Object)
            {
                if (displayKeyElem.TryGetProperty("value", out var displayKeyValueElem) && displayKeyValueElem.ValueKind == JsonValueKind.String)
                {
                    displayId = displayKeyValueElem.GetString() ?? string.Empty;
                }
            }

            return new ManageEngineRequest
            {
                Id = req["id"]?.ToString() ?? Guid.NewGuid().ToString(),
                Subject = subject,
                CreatedTime = createdTime,
                TechnicianName = technicianName,
                TechnicianEmail = technicianEmail,
                JsonData = JsonSerializer.Serialize(req),
                Status = status,
                RequesterName = requesterName,
                RequesterEmail = requesterEmail,
                DisplayId = displayId
            };
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
        /// Get request statistics with enhanced metrics
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

            var statusStats = await _dbContext.ManageEngineRequests
                .GroupBy(r => r.Status)
                .Select(g => new { Status = g.Key, Count = g.Count() })
                .ToListAsync();

            var requesterCount = await _dbContext.ManageEngineRequests
                .Where(r => !string.IsNullOrEmpty(r.RequesterEmail))
                .Select(r => r.RequesterEmail)
                .Distinct()
                .CountAsync();

            return new RequestStorageStats
            {
                TotalRequests = totalCount,
                OldestDate = oldestDate,
                NewestDate = newestDate,
                UniqueTechnicians = technicianCount,
                UniqueRequesters = requesterCount,
                StatusBreakdown = statusStats.ToDictionary(s => s.Status, s => s.Count)
            };
        }

        /// <summary>
        /// Get requests by status
        /// </summary>
        public async Task<List<ManageEngineRequest>> GetRequestsByStatusAsync(string status, int? limit = null)
        {
            var query = _dbContext.ManageEngineRequests
                .Where(r => r.Status == status)
                .OrderByDescending(r => r.CreatedTime);

            if (limit.HasValue)
            {
                query = (IOrderedQueryable<ManageEngineRequest>)query.Take(limit.Value);
            }

            return await query.ToListAsync();
        }

        /// <summary>
        /// Get requests by technician
        /// </summary>
        public async Task<List<ManageEngineRequest>> GetRequestsByTechnicianAsync(string technicianName, int? limit = null)
        {
            var query = _dbContext.ManageEngineRequests
                .Where(r => r.TechnicianName == technicianName)
                .OrderByDescending(r => r.CreatedTime);

            if (limit.HasValue)
            {
                query = (IOrderedQueryable<ManageEngineRequest>)query.Take(limit.Value);
            }

            return await query.ToListAsync();
        }

        /// <summary>
        /// Get requests by requester email
        /// </summary>
        public async Task<List<ManageEngineRequest>> GetRequestsByRequesterAsync(string requesterEmail, int? limit = null)
        {
            var query = _dbContext.ManageEngineRequests
                .Where(r => r.RequesterEmail == requesterEmail)
                .OrderByDescending(r => r.CreatedTime);

            if (limit.HasValue)
            {
                query = (IOrderedQueryable<ManageEngineRequest>)query.Take(limit.Value);
            }

            return await query.ToListAsync();
        }

        /// <summary>
        /// Clear all stored requests
        /// </summary>
        public async Task ClearAllAsync()
        {
            await _dbContext.ManageEngineRequests.ExecuteDeleteAsync();
        }

        /// <summary>
        /// Clean up old requests (older than specified date)
        /// </summary>
        public async Task<int> CleanupOldRequestsAsync(DateTimeOffset olderThan)
        {
            var deletedCount = await _dbContext.ManageEngineRequests
                .Where(r => r.CreatedTime < olderThan)
                .ExecuteDeleteAsync();

            return deletedCount;
        }
    }

    public class RequestStorageStats
    {
        public int TotalRequests { get; set; }
        public DateTimeOffset OldestDate { get; set; }
        public DateTimeOffset NewestDate { get; set; }
        public int UniqueTechnicians { get; set; }
        public int UniqueRequesters { get; set; }
        public Dictionary<string, int> StatusBreakdown { get; set; } = new Dictionary<string, int>();
    }
}