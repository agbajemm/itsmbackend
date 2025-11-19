using Azure.Identity;
using documentchecker.Services;
using Microsoft.EntityFrameworkCore;
using OfficeOpenXml;


var builder = WebApplication.CreateBuilder(args);

// ---------------------------------------------------------------------
// 1. Service registration (DI)
// ---------------------------------------------------------------------

builder.Services.AddControllers();

builder.Services.AddHttpClient();               // IHttpClientFactory
builder.Services.AddMemoryCache();              // IMemoryCache

// Swagger (dev only, but harmless in prod)
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// ---------------------------------------------------------------------
// 2. CORS – allow Swagger UI (localhost:xxxx) and any front‑end you need
// ---------------------------------------------------------------------
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowSwaggerOrigins", policy =>
    {
        policy.WithOrigins(
                "http://localhost:5173",   // Vite/React default
                "https://localhost:7173",  // Swagger UI when using HTTPS
                "http://localhost:5000")   // any other dev origin
              .AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials();
    });
});

// ---------------------------------------------------------------------
// 3. EF Core – Azure SQL with Azure AD auth
// ---------------------------------------------------------------------
var connectionString = builder.Configuration.GetConnectionString("AzureSql")
                       ?? throw new InvalidOperationException("Connection string 'AzureSql' not found.");

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(connectionString, sql =>
    {
        sql.EnableRetryOnFailure(
            maxRetryCount: 5,
            maxRetryDelay: TimeSpan.FromSeconds(10),
            errorNumbersToAdd: null);
    }));

// ---------------------------------------------------------------------
// 4. Application‑level services
// ---------------------------------------------------------------------
builder.Services.AddScoped<ChatService>();
builder.Services.AddScoped<QueryHistoryService>();
builder.Services.AddScoped<RequestStorageService>();
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowReactApp",
        policy =>
        {
            policy.WithOrigins("http://localhost:3000")
                  .AllowAnyHeader()
                  .AllowAnyMethod()
                  .AllowCredentials();
        });
});

// ---------------------------------------------------------------------
// 5. Build the app
// ---------------------------------------------------------------------
var app = builder.Build();

// ---------------------------------------------------------------------
// 6. Middleware pipeline (order matters!)
// ---------------------------------------------------------------------

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "DocumentChecker API v1"));
}

// CORS must come **before** routing / authorization
app.UseCors("AllowSwaggerOrigins");

// Force HTTPS (optional in prod, recommended)
app.UseHttpsRedirection();

app.UseCors("AllowLocalhost3000");

app.UseAuthorization();

app.MapControllers();

// ---------------------------------------------------------------------
// 7. (Optional) Auto‑migrate on start – great for local dev / demos
// ---------------------------------------------------------------------
if (app.Environment.IsDevelopment())
{
    using var scope = app.Services.CreateScope();
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    db.Database.Migrate();   // creates DB + applies pending migrations
}

// ---------------------------------------------------------------------
// 8. Run
// ---------------------------------------------------------------------
app.Run();