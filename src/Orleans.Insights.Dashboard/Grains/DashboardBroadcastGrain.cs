using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Orleans.Insights.Dashboard.Hubs;
using Orleans.Insights.Models;
using System.Text.Json;
using System.Threading.Tasks;

namespace Orleans.Insights.Dashboard.Grains;

/// <summary>
/// Stateless worker grain that broadcasts dashboard data to SignalR clients.
///
/// Called by InsightsGrain when page data changes.
/// Uses [StatelessWorker] for horizontal scaling - Orleans can spawn multiple instances
/// across silos to handle high broadcast volume.
///
/// Uses [OneWay] on interface methods for fire-and-forget semantics - InsightsGrain
/// doesn't need to wait for SignalR delivery confirmation.
///
/// Architecture:
///   InsightsGrain --[OneWay]--> DashboardBroadcastGrain --> IHubContext --> SignalR Groups --> WASM Clients
///                                                                        --> Orleans Backplane --> Other Silos
/// </summary>
[StatelessWorker]
public sealed class DashboardBroadcastGrain : Grain, IDashboardBroadcastGrain
{
    private readonly IHubContext<DashboardHub> _hubContext;
    private readonly ILogger<DashboardBroadcastGrain> _logger;
    private readonly JsonSerializerOptions _jsonOptions;

    public DashboardBroadcastGrain(
        IHubContext<DashboardHub> hubContext,
        ILogger<DashboardBroadcastGrain> logger)
    {
        _hubContext = hubContext;
        _logger = logger;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
    }

    public Task BroadcastHealthData(HealthPageData data)
    {
        _logger.LogDebug("Broadcasting Health data to SignalR group");
        var json = JsonSerializer.SerializeToElement(data, _jsonOptions);
        return _hubContext.Clients.Group(DashboardPages.Health).SendAsync("ReceiveHealthData", json);
    }

    public Task BroadcastOverviewData(OverviewPageData data)
    {
        _logger.LogDebug("Broadcasting Overview data to SignalR group");
        var json = JsonSerializer.SerializeToElement(data, _jsonOptions);
        return _hubContext.Clients.Group(DashboardPages.Overview).SendAsync("ReceiveOverviewData", json);
    }

    public Task BroadcastOrleansData(OrleansPageData data)
    {
        _logger.LogDebug("Broadcasting Orleans data to SignalR group");
        var json = JsonSerializer.SerializeToElement(data, _jsonOptions);
        return _hubContext.Clients.Group(DashboardPages.Orleans).SendAsync("ReceiveOrleansData", json);
    }

    public Task BroadcastInsightsData(InsightsPageData data)
    {
        _logger.LogDebug("Broadcasting Insights data to SignalR group");
        var json = JsonSerializer.SerializeToElement(data, _jsonOptions);
        return _hubContext.Clients.Group(DashboardPages.Insights).SendAsync("ReceiveInsightsData", json);
    }
}
