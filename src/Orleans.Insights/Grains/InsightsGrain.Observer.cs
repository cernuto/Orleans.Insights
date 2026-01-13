using Microsoft.Extensions.Logging;
using Orleans.Insights.Models;
using Orleans.Runtime;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Insights.Grains;

/// <summary>
/// Partial class implementing dashboard broadcasting for real-time push notifications.
/// Calls IDashboardBroadcastGrain (a StatelessWorker in the dashboard host) to push updates via SignalR.
/// Uses [OneWay] calls for fire-and-forget semantics - no need to await SignalR delivery.
/// </summary>
public partial class InsightsGrain
{
    #region Broadcaster

    /// <summary>
    /// Broadcast grain reference, obtained via GrainFactory.
    /// Uses StatelessWorker pattern - Orleans can spawn multiple instances for scale.
    /// [OneWay] methods mean we don't await the result.
    /// </summary>
    private IDashboardBroadcastGrain? _broadcastGrain;

    /// <summary>
    /// Tracks the last broadcast data for change detection.
    /// Only broadcast when data actually changes.
    /// </summary>
    private OverviewPageData? _lastBroadcastOverview;
    private OrleansPageData? _lastBroadcastOrleans;
    private InsightsPageData? _lastBroadcastInsights;

    /// <summary>
    /// Timer for periodic broadcasts.
    /// Checks for data changes and pushes to SignalR.
    /// </summary>
    private IDisposable? _broadcastTimer;

    /// <summary>
    /// How often to check for changes and broadcast.
    /// </summary>
    private static readonly TimeSpan BroadcastInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Counter for less frequent page broadcasts (insights).
    /// Incremented each broadcast tick, reset when reaching threshold.
    /// </summary>
    private int _slowBroadcastCounter;

    /// <summary>
    /// How many ticks between slow page broadcasts (5 = every 5 seconds).
    /// </summary>
    private const int SlowBroadcastInterval = 5;

    #endregion

    #region Broadcast Timer

    /// <summary>
    /// Starts the broadcast timer.
    /// Called from OnActivateAsync.
    /// </summary>
    private void StartBroadcastTimer()
    {
        // Get the broadcast grain reference (StatelessWorker in dashboard host)
        // Key doesn't matter for StatelessWorker - Orleans routes to any available instance
        _broadcastGrain = GrainFactory.GetGrain<IDashboardBroadcastGrain>(0);

        _broadcastTimer = this.RegisterGrainTimer(
            BroadcastChangesAsync,
            new GrainTimerCreationOptions(BroadcastInterval, BroadcastInterval)
            {
                Interleave = true,
                KeepAlive = false
            });
    }

    /// <summary>
    /// Periodic callback that checks for data changes and broadcasts via SignalR.
    /// Only sends broadcasts when data has actually changed.
    /// [OneWay] methods return immediately - no need to await.
    /// </summary>
    private async Task BroadcastChangesAsync()
    {
        if (_broadcastGrain == null)
            return;

        try
        {
            // Get current page data (uses caching internally)
            var overviewTask = GetOverviewPageData();
            var orleansTask = GetOrleansPageData();

            await Task.WhenAll(overviewTask, orleansTask);

            var overview = await overviewTask;
            var orleans = await orleansTask;

            // Broadcast only if data changed
            // [OneWay] calls are fire-and-forget - use discard to suppress warnings
            if (HasOverviewDataChanged(overview))
            {
                _lastBroadcastOverview = overview;
                _ = _broadcastGrain.BroadcastOverviewData(overview);
            }

            if (HasOrleansDataChanged(orleans))
            {
                _lastBroadcastOrleans = orleans;
                _ = _broadcastGrain.BroadcastOrleansData(orleans);
            }

            // Less frequent pages - check every 5 seconds using counter for consistent timing
            _slowBroadcastCounter++;
            if (_slowBroadcastCounter >= SlowBroadcastInterval)
            {
                _slowBroadcastCounter = 0;
                var insights = await GetInsightsPageData();

                if (HasInsightsDataChanged(insights))
                {
                    _lastBroadcastInsights = insights;
                    _ = _broadcastGrain.BroadcastInsightsData(insights);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to broadcast dashboard data");
        }
    }

    #endregion

    #region Change Detection

    /// <summary>
    /// Detects if overview data has meaningfully changed.
    /// Checks cluster metrics and silo statuses.
    /// </summary>
    private bool HasOverviewDataChanged(OverviewPageData current)
    {
        if (_lastBroadcastOverview is null) return true;

        var last = _lastBroadcastOverview;

        // Check aggregate metrics
        if (last.SiloCount != current.SiloCount
            || last.TotalGrains != current.TotalGrains
            || Math.Abs(last.CpuPercent - current.CpuPercent) > 1.0
            || last.MemoryUsedMb != current.MemoryUsedMb)
            return true;

        // Check list counts
        if (last.Silos.Count != current.Silos.Count)
            return true;

        // Check silo changes
        foreach (var currentSilo in current.Silos)
        {
            var lastSilo = last.Silos.FirstOrDefault(s => s.Address == currentSilo.Address);
            if (lastSilo is null)
                return true;

            if (lastSilo.Status != currentSilo.Status
                || lastSilo.ActivationCount != currentSilo.ActivationCount
                || Math.Abs(lastSilo.CpuUsage - currentSilo.CpuUsage) > 1.0
                || lastSilo.MemoryUsageMb != currentSilo.MemoryUsageMb)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Detects if Orleans data has meaningfully changed.
    /// Checks OTel metrics, silo stats, grain stats, and method metrics.
    /// </summary>
    private bool HasOrleansDataChanged(OrleansPageData current)
    {
        if (_lastBroadcastOrleans is null) return true;

        var last = _lastBroadcastOrleans;
        var lastOTel = last.OTelMetrics;
        var currentOTel = current.OTelMetrics;

        // Check OTel metrics
        if (lastOTel.TotalActivations != currentOTel.TotalActivations
            || lastOTel.ConnectedClients != currentOTel.ConnectedClients
            || lastOTel.TotalRequests != currentOTel.TotalRequests
            || Math.Abs(lastOTel.CpuUsagePercent - currentOTel.CpuUsagePercent) > 1.0
            || Math.Abs(lastOTel.AverageRequestLatencyMs - currentOTel.AverageRequestLatencyMs) > 0.5
            || lastOTel.MessagesDropped != currentOTel.MessagesDropped)
            return true;

        // Check counts
        if (last.Silos.Count != current.Silos.Count
            || last.GrainStats.Count != current.GrainStats.Count
            || last.TopMethods.Count != current.TopMethods.Count)
            return true;

        // Check silo-level changes
        foreach (var currentSilo in current.Silos)
        {
            var lastSilo = last.Silos.FirstOrDefault(s => s.Address == currentSilo.Address);
            if (lastSilo is null)
                return true;

            if (lastSilo.Status != currentSilo.Status
                || lastSilo.ActivationCount != currentSilo.ActivationCount
                || Math.Abs(lastSilo.CpuUsage - currentSilo.CpuUsage) > 1.0
                || lastSilo.MemoryUsageMb != currentSilo.MemoryUsageMb)
                return true;
        }

        // Check top grain stats changes
        if (last.GrainStats.Count > 0 && current.GrainStats.Count > 0)
        {
            var lastTop = last.GrainStats[0];
            var currentTop = current.GrainStats[0];

            if (lastTop.GrainType != currentTop.GrainType
                || lastTop.TotalActivations != currentTop.TotalActivations
                || Math.Abs(lastTop.RequestsPerSecond - currentTop.RequestsPerSecond) > 0.1
                || Math.Abs(lastTop.AverageLatencyMs - currentTop.AverageLatencyMs) > 0.5
                || lastTop.TotalRequests != currentTop.TotalRequests)
                return true;
        }

        // Check top methods changes
        if (last.TopMethods.Count > 0 && current.TopMethods.Count > 0)
        {
            var lastTop = last.TopMethods[0];
            var currentTop = current.TopMethods[0];

            if (lastTop.MethodName != currentTop.MethodName
                || lastTop.TotalCalls != currentTop.TotalCalls
                || Math.Abs(lastTop.AvgLatencyMs - currentTop.AvgLatencyMs) > 0.5)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Detects if insights data has meaningfully changed.
    /// Checks anomaly counts, grain/method metrics, and silo comparisons.
    /// </summary>
    private bool HasInsightsDataChanged(InsightsPageData current)
    {
        if (_lastBroadcastInsights is null) return true;

        var last = _lastBroadcastInsights;

        // Check anomaly counts
        if (last.LatencyAnomalies.Count != current.LatencyAnomalies.Count
            || last.ErrorRateAnomalies.Count != current.ErrorRateAnomalies.Count)
            return true;

        // Check top grains by latency (compare top item latency if exists)
        if (last.TopGrainsByLatency.Count != current.TopGrainsByLatency.Count)
            return true;
        if (last.TopGrainsByLatency.Count > 0 && current.TopGrainsByLatency.Count > 0
            && Math.Abs(last.TopGrainsByLatency[0].AvgLatencyMs - current.TopGrainsByLatency[0].AvgLatencyMs) > 0.5)
            return true;

        // Check top grains by throughput (compare top item RPS if exists)
        if (last.TopGrainsByThroughput.Count != current.TopGrainsByThroughput.Count)
            return true;
        if (last.TopGrainsByThroughput.Count > 0 && current.TopGrainsByThroughput.Count > 0
            && Math.Abs(last.TopGrainsByThroughput[0].RequestsPerSecond - current.TopGrainsByThroughput[0].RequestsPerSecond) > 0.1)
            return true;

        // Check silo comparisons (compare count and top silo score if exists)
        if (last.SiloComparisons.Count != current.SiloComparisons.Count)
            return true;
        if (last.SiloComparisons.Count > 0 && current.SiloComparisons.Count > 0
            && Math.Abs(last.SiloComparisons[0].PerformanceScore - current.SiloComparisons[0].PerformanceScore) > 1.0)
            return true;

        // Check top methods by latency
        if (last.TopMethodsByLatency.Count != current.TopMethodsByLatency.Count)
            return true;
        if (last.TopMethodsByLatency.Count > 0 && current.TopMethodsByLatency.Count > 0
            && Math.Abs(last.TopMethodsByLatency[0].AvgLatencyMs - current.TopMethodsByLatency[0].AvgLatencyMs) > 0.5)
            return true;

        return false;
    }

    #endregion

    #region Lifecycle Integration

    /// <summary>
    /// Disposes the broadcast timer.
    /// Called from Dispose().
    /// </summary>
    private void DisposeBroadcastTimer()
    {
        _broadcastTimer?.Dispose();
        _broadcastTimer = null;
    }

    #endregion
}
