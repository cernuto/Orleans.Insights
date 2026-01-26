using Orleans.Insights;
using Orleans.Insights.Dashboard;
using Orleans.Insights.Sample.Services;
using Serilog;
using Serilog.Events;

// Configure Serilog with file logging for diagnostics
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .MinimumLevel.Override("Orleans", LogEventLevel.Warning)
    .MinimumLevel.Override("Orleans.Insights", LogEventLevel.Information) // Keep Insights logs at Info level
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .WriteTo.File("logs/orleans-insights-.log",
        rollingInterval: RollingInterval.Day,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

var builder = WebApplication.CreateBuilder(args);

// Use Serilog for logging
builder.Host.UseSerilog();

// Configure Orleans silo with insights
builder.UseOrleans(siloBuilder =>
{
    siloBuilder
        .UseLocalhostClustering()
        .AddOrleansInsights(options =>
        {
            options.RetentionPeriod = TimeSpan.FromHours(1);
        });
});

// Add Orleans.Insights Dashboard services (includes SignalR)
builder.Services.AddOrleansInsightsDashboard();

// Add background service to simulate grain activity for demo
builder.Services.AddHostedService<GrainActivitySimulator>();

var app = builder.Build();

// Enable static files (required for Blazor WASM static web assets)
app.UseStaticFiles();

app.MapGet("/", () => "Orleans.Insights Sample - Dashboard at /dashboard");

// Map the Orleans Insights dashboard at /dashboard
app.MapOrleansInsightsDashboard("/dashboard");

app.Run();
