using Microsoft.Extensions.Logging;
using Orleans.Insights.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Insights.Grains;

/// <summary>
/// Partial class implementing dashboard page query methods.
/// Supports horizontal scaling by providing centralized state access.
/// </summary>
/// <remarks>
/// <para>
/// This class provides two sets of methods for page data:
/// </para>
/// <para>
/// 1. <b>GetXxxPageData()</b>: Public API methods called by dashboard clients.
///    These return pre-built data from the volatile cache populated by background refresh loops.
///    If cache is empty (startup), they fall back to building data directly.
/// </para>
/// <para>
/// 2. <b>BuildXxxPageDataForCacheAsync()</b>: Internal methods called by background refresh loops.
///    These execute DuckDB queries and build the page data objects.
///    Run on thread pool to avoid blocking grain scheduler.
/// </para>
/// </remarks>
public partial class InsightsGrain
{
    #region IDashboardPageQueryGrain Implementation

    /// <inheritdoc/>
    /// <remarks>
    /// Returns pre-built data from volatile cache if available.
    /// Falls back to building directly on cache miss (e.g., during startup).
    /// </remarks>
    public async Task<OverviewPageData> GetOverviewPageData()
    {
        // Return pre-built data from cache if available (populated by FastRefreshLoop)
        var cached = _refreshedOverview;
        if (cached != null)
        {
            return cached;
        }

        // Fallback: build directly (startup or cache miss)
        return await BuildOverviewPageDataForCacheAsync();
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Returns pre-built data from volatile cache if available.
    /// Falls back to building directly on cache miss (e.g., during startup).
    /// </remarks>
    public async Task<OrleansPageData> GetOrleansPageData()
    {
        // Return pre-built data from cache if available (populated by FastRefreshLoop)
        var cached = _refreshedOrleans;
        if (cached != null)
        {
            return cached;
        }

        // Fallback: build directly (startup or cache miss)
        return await BuildOrleansPageDataForCacheAsync();
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Returns pre-built data from volatile cache if available.
    /// Falls back to building directly on cache miss (e.g., during startup).
    /// </remarks>
    public async Task<InsightsPageData> GetInsightsPageData()
    {
        // Return pre-built data from cache if available (populated by SlowRefreshLoop)
        var cached = _refreshedInsights;
        if (cached != null)
        {
            return cached;
        }

        // Fallback: build directly (startup or cache miss)
        return await BuildInsightsPageDataForCacheAsync();
    }

    #endregion

    #region Background Cache Build Methods

    /// <summary>
    /// Builds overview page data for the background refresh loop cache.
    /// Called by FastRefreshLoop on thread pool.
    /// </summary>
    private async Task<OverviewPageData> BuildOverviewPageDataForCacheAsync()
    {
        var now = DateTime.UtcNow;

        // Get aggregated cluster metrics
        var aggregated = await GetAggregatedMetrics();

        // Build silo summaries
        var siloInfos = await GetReportingSilos();
        var silos = siloInfos.Select(s => new SiloSummary
        {
            Address = s.SiloId,
            Status = s.IsStale ? "Stale" : "Active",
            HostName = s.HostName,
            ActivationCount = (int)(aggregated.ClusterMetrics.TotalActivations / Math.Max(1, siloInfos.Length)),
            CpuUsage = aggregated.ClusterMetrics.AverageCpuUsagePercent,
            MemoryUsageMb = aggregated.ClusterMetrics.TotalMemoryUsageMb / Math.Max(1, siloInfos.Length)
        }).ToList();

        return new OverviewPageData
        {
            SiloCount = siloInfos.Length,
            TotalGrains = (int)aggregated.ClusterMetrics.TotalActivations,
            CpuPercent = aggregated.ClusterMetrics.AverageCpuUsagePercent,
            MemoryUsedMb = aggregated.ClusterMetrics.TotalMemoryUsageMb,
            Timestamp = now,
            Silos = silos
        };
    }

    /// <summary>
    /// Builds Orleans page data for the background refresh loop cache.
    /// Called by FastRefreshLoop on thread pool.
    /// </summary>
    private async Task<OrleansPageData> BuildOrleansPageDataForCacheAsync()
    {
        var now = DateTime.UtcNow;

        // Get aggregated metrics
        var aggregated = await GetAggregatedMetrics();

        // Get all methods for grain detail view (not just top 10 by latency)
        var topMethods = await GetAllMethodProfiles();

        // Get per-silo grain stats for filtering
        var cutoff = DateTime.UtcNow.AddMinutes(-5);
        var perSiloStats = GetPerSiloGrainStats(cutoff);

        // Get per-silo cluster metrics (activations, CPU, memory) from database
        var perSiloClusterMetrics = GetPerSiloClusterMetrics(cutoff);

        // Build silo summaries from aggregated data
        var siloInfos = await GetReportingSilos();
        var silos = siloInfos.Select(s =>
        {
            var hasMetrics = perSiloClusterMetrics.TryGetValue(s.SiloId, out var metrics);
            return new SiloSummary
            {
                Address = s.SiloId,
                Status = s.IsStale ? "Stale" : "Active",
                HostName = s.HostName,
                ActivationCount = hasMetrics ? (int)metrics.activations : 0,
                CpuUsage = hasMetrics ? metrics.cpu : aggregated.ClusterMetrics.AverageCpuUsagePercent,
                MemoryUsageMb = hasMetrics ? metrics.memoryMb : aggregated.ClusterMetrics.TotalMemoryUsageMb / Math.Max(1, siloInfos.Length)
            };
        }).ToList();

        // Build grain stats from aggregated grain types with per-silo breakdown
        // Activations come from per-silo stats if available
        var grainStats = aggregated.GrainTypeMetrics.Values
            .Select(g => new GrainStatsSummary
            {
                GrainType = g.GrainType,
                TotalActivations = GetGrainTypeActivations(g.GrainType, perSiloStats),
                SiloCount = perSiloStats.TryGetValue(g.GrainType, out var stats) ? stats.Count : siloInfos.Length,
                RequestsPerSecond = g.RequestsPerSecond,
                AverageLatencyMs = g.AverageLatencyMs,
                ExceptionRate = g.TotalRequests > 0 ? (g.FailedRequests / (double)g.TotalRequests) * 100 : 0,
                TotalRequests = g.TotalRequests,
                FailedRequests = g.FailedRequests,
                PerSiloStats = perSiloStats.TryGetValue(g.GrainType, out var siloStats) ? siloStats : null
            })
            .ToList();

        return new OrleansPageData
        {
            Silos = silos,
            GrainStats = grainStats,
            TopMethods = topMethods,
            OTelMetrics = new OrleansOTelSummary
            {
                TotalActivations = aggregated.ClusterMetrics.TotalActivations,
                ConnectedClients = aggregated.ClusterMetrics.TotalConnectedClients,
                MessagesSent = aggregated.ClusterMetrics.TotalMessagesSent,
                MessagesReceived = aggregated.ClusterMetrics.TotalMessagesReceived,
                MessagesDropped = aggregated.ClusterMetrics.TotalMessagesDropped,
                CpuUsagePercent = aggregated.ClusterMetrics.AverageCpuUsagePercent,
                MemoryUsageMb = aggregated.ClusterMetrics.TotalMemoryUsageMb,
                AverageRequestLatencyMs = aggregated.ClusterMetrics.AverageRequestLatencyMs,
                TotalRequests = aggregated.ClusterMetrics.TotalRequests,
                // Catalog metrics (activation lifecycle)
                ActivationWorkingSet = aggregated.ClusterMetrics.TotalActivationWorkingSet,
                ActivationsCreated = aggregated.ClusterMetrics.TotalActivationsCreated,
                ActivationsDestroyed = aggregated.ClusterMetrics.TotalActivationsDestroyed,
                ActivationsFailedToActivate = aggregated.ClusterMetrics.TotalActivationsFailedToActivate,
                ActivationCollections = aggregated.ClusterMetrics.TotalActivationCollections,
                ActivationShutdowns = aggregated.ClusterMetrics.TotalActivationShutdowns,
                ActivationNonExistent = aggregated.ClusterMetrics.TotalActivationNonExistent,
                ConcurrentRegistrationAttempts = aggregated.ClusterMetrics.TotalConcurrentRegistrationAttempts,
                // Miscellaneous grain metrics
                GrainCount = aggregated.ClusterMetrics.TotalGrainCount,
                SystemTargets = aggregated.ClusterMetrics.TotalSystemTargets
            },
            Timestamp = now
        };
    }

    /// <summary>
    /// Builds insights page data for the background refresh loop cache.
    /// Called by SlowRefreshLoop on thread pool.
    /// </summary>
    private async Task<InsightsPageData> BuildInsightsPageDataForCacheAsync()
    {
        var now = DateTime.UtcNow;
        var analysisDuration = TimeSpan.FromHours(1);

        // Parallel fetch all analytics data
        var clusterTrendTask = GetClusterTrend(analysisDuration, 60);
        var topGrainsByLatencyTask = GetTopGrainTypes(InsightMetric.Latency, 10, analysisDuration);
        var topGrainsByThroughputTask = GetTopGrainTypes(InsightMetric.Throughput, 10, analysisDuration);
        var topMethodsByLatencyTask = GetTopMethodsByLatency(10, analysisDuration);
        var topMethodsByExceptionsTask = GetTopMethodsByExceptions(10, analysisDuration);
        var anomalyTask = DetectAnomalies(TimeSpan.FromMinutes(5), analysisDuration);
        var siloComparisonTask = CompareSilos(analysisDuration);
        var dbSummaryTask = GetDatabaseSummary();

        await Task.WhenAll(
            clusterTrendTask, topGrainsByLatencyTask, topGrainsByThroughputTask,
            topMethodsByLatencyTask, topMethodsByExceptionsTask, anomalyTask,
            siloComparisonTask, dbSummaryTask);

        var anomalyReport = await anomalyTask;

        return new InsightsPageData
        {
            ClusterTrend = await clusterTrendTask,
            TopGrainsByLatency = await topGrainsByLatencyTask,
            TopGrainsByThroughput = await topGrainsByThroughputTask,
            TopMethodsByLatency = await topMethodsByLatencyTask,
            TopMethodsByExceptions = await topMethodsByExceptionsTask,
            LatencyAnomalies = anomalyReport.LatencyAnomalies,
            ErrorRateAnomalies = anomalyReport.ErrorRateAnomalies,
            SiloComparisons = await siloComparisonTask,
            DatabaseSummary = await dbSummaryTask,
            Timestamp = now
        };
    }

    #endregion
}
