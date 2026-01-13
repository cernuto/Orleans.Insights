using Microsoft.Extensions.Logging;
using Orleans.Insights.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Insights.Grains;

/// <summary>
/// Partial class implementing health ingestion/query and dashboard page query methods.
/// Supports horizontal scaling by providing centralized state access.
/// </summary>
public partial class InsightsGrain
{
    #region Health Storage

    /// <summary>
    /// In-memory storage for silo health reports.
    /// Key: SiloId, Value: Most recent health report.
    /// </summary>
    private readonly ConcurrentDictionary<string, SiloHealthReport> _healthReports = new();

    /// <summary>
    /// How long before a health report is considered stale (30 seconds).
    /// </summary>
    private static readonly TimeSpan HealthStaleThreshold = TimeSpan.FromSeconds(30);

    #endregion

    #region Dashboard Page Cache

    /// <summary>
    /// TTL for cached dashboard page data. Multiple callers within this window
    /// receive the same cached result, reducing downstream grain calls.
    /// </summary>
    private static readonly TimeSpan PageCacheTtl = TimeSpan.FromMilliseconds(500);

    private OverviewPageData? _cachedOverview;
    private DateTime _overviewCacheTime;

    private OrleansPageData? _cachedOrleans;
    private DateTime _orleansCacheTime;

    private HealthPageData? _cachedHealth;
    private DateTime _healthCacheTime;

    private InsightsPageData? _cachedInsights;
    private DateTime _insightsCacheTime;

    #endregion

    #region IHealthIngestGrain Implementation

    /// <inheritdoc/>
    public Task IngestHealthReport(HealthReportData report)
    {
        try
        {
            var overallStatus = SiloHealthReport.ComputeOverallStatus(report.Checks);

            var siloReport = new SiloHealthReport
            {
                SiloId = report.SiloId,
                HostName = report.HostName,
                LastReported = report.Timestamp,
                OverallStatus = overallStatus,
                Checks = report.Checks
            };

            _healthReports.AddOrUpdate(report.SiloId, siloReport, (_, _) => siloReport);

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    "Ingested health report from {SiloId}: {Status} ({Checks} checks)",
                    report.SiloId, overallStatus, report.Checks.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to ingest health report from {SiloId}", report.SiloId);
        }

        return Task.CompletedTask;
    }

    #endregion

    #region IHealthQueryGrain Implementation

    /// <inheritdoc/>
    public Task<List<SiloHealthReport>> GetClusterHealth()
    {
        var now = DateTime.UtcNow;
        var staleThreshold = now - HealthStaleThreshold;

        // Filter out stale reports and collect active ones
        var activeReports = new List<SiloHealthReport>();

        foreach (var kvp in _healthReports)
        {
            if (kvp.Value.LastReported >= staleThreshold)
            {
                activeReports.Add(kvp.Value);
            }
            else
            {
                // Remove stale entry
                _healthReports.TryRemove(kvp.Key, out _);
            }
        }

        return Task.FromResult(activeReports);
    }

    #endregion

    #region IDashboardPageQueryGrain Implementation

    /// <inheritdoc/>
    public async Task<OverviewPageData> GetOverviewPageData()
    {
        var now = DateTime.UtcNow;

        // Return cached data if still valid
        if (_cachedOverview != null && now - _overviewCacheTime < PageCacheTtl)
        {
            return _cachedOverview;
        }

        // Get aggregated cluster metrics
        var aggregated = await GetAggregatedMetrics();

        // Get health data
        var healthReports = await GetClusterHealth();

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

        // Build health endpoint summaries
        int healthyCount = 0, unhealthyCount = 0;
        var healthEndpoints = new List<HealthEndpointSummary>();

        foreach (var report in healthReports)
        {
            healthEndpoints.Add(new HealthEndpointSummary
            {
                Name = report.HostName,
                OverallStatus = report.OverallStatus,
                CheckCount = report.Checks.Count
            });

            if (report.OverallStatus == "Healthy") healthyCount++;
            else if (report.OverallStatus == "Unhealthy") unhealthyCount++;
        }

        _cachedOverview = new OverviewPageData
        {
            SiloCount = siloInfos.Length,
            TotalGrains = (int)aggregated.ClusterMetrics.TotalActivations,
            CpuPercent = aggregated.ClusterMetrics.AverageCpuUsagePercent,
            MemoryUsedMb = aggregated.ClusterMetrics.TotalMemoryUsageMb,
            HealthyEndpoints = healthyCount,
            UnhealthyEndpoints = unhealthyCount,
            Timestamp = now,
            Silos = silos,
            HealthEndpoints = healthEndpoints
        };
        _overviewCacheTime = now;
        return _cachedOverview;
    }

    /// <inheritdoc/>
    public async Task<OrleansPageData> GetOrleansPageData()
    {
        var now = DateTime.UtcNow;

        // Return cached data if still valid
        if (_cachedOrleans != null && now - _orleansCacheTime < PageCacheTtl)
        {
            return _cachedOrleans;
        }

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

        _cachedOrleans = new OrleansPageData
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
        _orleansCacheTime = now;
        return _cachedOrleans;
    }

    /// <inheritdoc/>
    public async Task<HealthPageData> GetHealthPageData()
    {
        var now = DateTime.UtcNow;

        // Return cached data if still valid
        if (_cachedHealth != null && now - _healthCacheTime < PageCacheTtl)
        {
            return _cachedHealth;
        }

        var reports = await GetClusterHealth();

        int healthyCount = 0, degradedCount = 0, unhealthyCount = 0;

        foreach (var report in reports)
        {
            foreach (var check in report.Checks)
            {
                switch (check.Status)
                {
                    case "Healthy": healthyCount++; break;
                    case "Degraded": degradedCount++; break;
                    case "Unhealthy": unhealthyCount++; break;
                }
            }
        }

        _cachedHealth = new HealthPageData
        {
            SiloReports = reports,
            Timestamp = now,
            HealthyCount = healthyCount,
            DegradedCount = degradedCount,
            UnhealthyCount = unhealthyCount
        };
        _healthCacheTime = now;
        return _cachedHealth;
    }

    /// <inheritdoc/>
    public async Task<InsightsPageData> GetInsightsPageData()
    {
        var now = DateTime.UtcNow;

        // Return cached data if still valid
        if (_cachedInsights != null && now - _insightsCacheTime < PageCacheTtl)
        {
            return _cachedInsights;
        }

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

        _cachedInsights = new InsightsPageData
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
        _insightsCacheTime = now;
        return _cachedInsights;
    }

    #endregion
}
