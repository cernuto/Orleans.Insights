using Orleans.Insights;
using Orleans.Insights.Dashboard;
using Orleans.Insights.Sample.Services;

var builder = WebApplication.CreateBuilder(args);

// Configure Orleans silo with insights
builder.UseOrleans(siloBuilder =>
{
    siloBuilder
        .UseLocalhostClustering()
        .AddOrleansInsights(options =>
        {
            options.RetentionPeriod = TimeSpan.FromHours(1);
            options.BroadcastInterval = TimeSpan.FromSeconds(1);
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
