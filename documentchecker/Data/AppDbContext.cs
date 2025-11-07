// Data/AppDbContext.cs
using documentchecker.Models;
using Microsoft.EntityFrameworkCore;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<ChatConversation> ChatConversations => Set<ChatConversation>();
    public DbSet<ChatMessage> ChatMessages => Set<ChatMessage>();
    public DbSet<ManageEngineQuery> ManageEngineQueries => Set<ManageEngineQuery>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Optimize queries
        modelBuilder.Entity<ChatMessage>()
            .HasIndex(m => m.ConversationId);

        modelBuilder.Entity<ChatMessage>()
            .HasIndex(m => m.SentAt);

        modelBuilder.Entity<ManageEngineQuery>()
            .HasIndex(q => q.ExecutedAt);

        modelBuilder.Entity<ManageEngineQuery>()
            .HasIndex(q => q.QueryId);
    }
}