// Services/QueryHistoryService.cs
using documentchecker.Models;
using Microsoft.EntityFrameworkCore;

namespace documentchecker.Services
{
    public class QueryHistoryService
    {
        private readonly AppDbContext _db;
        public QueryHistoryService(AppDbContext db) => _db = db;

        public async Task LogQueryAsync(ManageEngineQuery query)
        {
            _db.ManageEngineQueries.Add(query);
            await _db.SaveChangesAsync();
        }

        public async Task<List<ManageEngineQuery>> GetRecentQueries(int top = 50)
        {
            return await _db.ManageEngineQueries
                .OrderByDescending(q => q.ExecutedAt)
                .Take(top)
                .ToListAsync();
        }
    }
}