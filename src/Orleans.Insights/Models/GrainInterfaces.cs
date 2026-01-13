using Orleans;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Insights.Models;

#region Segregated Interfaces (ISP - Interface Segregation Principle)

/// <summary>
/// Interface for ingesting metrics into InsightsGrain.
/// Follows ISP by separating ingestion concerns from query concerns.
/// </summary>
public interface IInsightsIngestGrain : IGrainWithStringKey
{
    /// <summary>
    /// Ingests metrics from a silo for historical storage.
    /// Called by MetricAggregatorReporter alongside ReportSiloMetrics.
    /// Uses [OneWay] for fire-and-forget semantics.
    /// </summary>
    [OneWay]
    Task IngestMetrics(SiloMetricsReport metrics);

    /// <summary>
    /// Ingests method profile data using OrleansDashboard-style accurate totals.
    /// Called by GrainMethodProfiler from each silo every second.
    /// Stores raw totals (count + totalElapsedMs) for accurate average calculation.
    /// Uses [OneWay] for fire-and-forget semantics.
    /// Uses Immutable&lt;T&gt; wrapper for efficient serialization (no copy).
    /// </summary>
    [OneWay]
    Task IngestMethodProfile(Immutable<MethodProfileReport> report);
}

/// <summary>
/// Interface for real-time cluster aggregation queries.
/// Follows ISP by separating real-time metrics from historical queries.
/// </summary>
public interface IInsightsRealTimeGrain : IGrainWithStringKey
{
    /// <summary>
    /// Gets aggregated metrics from all reporting silos.
    /// Used by the dashboard to display cluster-wide metrics.
    /// Uses [AlwaysInterleave] to allow concurrent reads without blocking other operations.
    /// </summary>
    /// <returns>Aggregated cluster metrics from all silos</returns>
    [AlwaysInterleave]
    Task<AggregatedClusterMetrics> GetAggregatedMetrics();

    /// <summary>
    /// Gets the list of silos currently reporting metrics.
    /// Useful for monitoring silo health and detecting stale silos.
    /// </summary>
    [AlwaysInterleave]
    Task<SiloMetricsInfo[]> GetReportingSilos();
}

/// <summary>
/// Interface for accurate method profile queries using OrleansDashboard pattern.
/// Follows ISP by separating method profiling from other metric types.
/// </summary>
public interface IMethodProfileQueryGrain : IGrainWithStringKey
{
    /// <summary>
    /// Gets accurate method profile trend over a time range.
    /// Uses totals-based calculation: avg = SUM(total_elapsed_ms) / SUM(call_count).
    /// </summary>
    /// <param name="duration">How far back to look</param>
    /// <param name="bucketSeconds">Time bucket size in seconds (e.g., 1 for 1-second buckets)</param>
    [AlwaysInterleave]
    Task<List<MethodProfileTrendPoint>> GetMethodProfileTrend(TimeSpan duration, int bucketSeconds = 1);

    /// <summary>
    /// Gets accurate method profile trend for a specific grain type.
    /// </summary>
    /// <param name="grainType">The grain type name (short name, e.g., "Device")</param>
    /// <param name="duration">How far back to look</param>
    /// <param name="bucketSeconds">Time bucket size in seconds</param>
    [AlwaysInterleave]
    Task<List<MethodProfileTrendPoint>> GetMethodProfileTrendForGrain(string grainType, TimeSpan duration, int bucketSeconds = 1);

    /// <summary>
    /// Gets accurate method profile trend for a specific method.
    /// </summary>
    /// <param name="grainType">The grain type name</param>
    /// <param name="methodName">The method name</param>
    /// <param name="duration">How far back to look</param>
    /// <param name="bucketSeconds">Time bucket size in seconds</param>
    [AlwaysInterleave]
    Task<List<MethodProfileTrendPoint>> GetMethodProfileTrendForMethod(string grainType, string methodName, TimeSpan duration, int bucketSeconds = 1);

    /// <summary>
    /// Gets top N methods by accurate average latency.
    /// </summary>
    /// <param name="count">Number of results to return</param>
    /// <param name="duration">Time window to analyze</param>
    [AlwaysInterleave]
    Task<List<MethodProfileSummary>> GetTopMethodsByLatency(int count = 10, TimeSpan? duration = null);

    /// <summary>
    /// Gets top N methods by call count (throughput).
    /// </summary>
    /// <param name="count">Number of results to return</param>
    /// <param name="duration">Time window to analyze</param>
    [AlwaysInterleave]
    Task<List<MethodProfileSummary>> GetTopMethodsByCallCount(int count = 10, TimeSpan? duration = null);

    /// <summary>
    /// Gets top N methods by exception count.
    /// </summary>
    /// <param name="count">Number of results to return</param>
    /// <param name="duration">Time window to analyze</param>
    [AlwaysInterleave]
    Task<List<MethodProfileSummary>> GetTopMethodsByExceptions(int count = 10, TimeSpan? duration = null);

    /// <summary>
    /// Gets method profile data aggregated by silo for a specific method.
    /// Shows which silos are handling the most load for a given method.
    /// </summary>
    /// <param name="grainType">The grain type name</param>
    /// <param name="methodName">The method name</param>
    /// <param name="duration">Time window to analyze</param>
    [AlwaysInterleave]
    Task<List<MethodProfileSiloSummary>> GetMethodProfileBySilo(string grainType, string methodName, TimeSpan? duration = null);
}

/// <summary>
/// Interface for Orleans cluster metrics queries.
/// Follows ISP by separating Orleans-specific queries.
/// </summary>
public interface IOrleansMetricsQueryGrain : IGrainWithStringKey
{
    /// <summary>
    /// Gets cluster metrics trend over a time range.
    /// Returns time-bucketed aggregations for charting.
    /// </summary>
    /// <param name="duration">How far back to look (e.g., 1 hour, 24 hours)</param>
    /// <param name="bucketSeconds">Time bucket size in seconds (e.g., 60 for 1-minute buckets)</param>
    [AlwaysInterleave]
    Task<List<ClusterMetricsTrend>> GetClusterTrend(TimeSpan duration, int bucketSeconds = 60);

    /// <summary>
    /// Gets the top N grain types by a specific metric.
    /// </summary>
    /// <param name="metric">Metric to sort by (latency, requests, errors, rps)</param>
    /// <param name="count">Number of results to return</param>
    /// <param name="duration">Time window to analyze</param>
    [AlwaysInterleave]
    Task<List<GrainTypeInsight>> GetTopGrainTypes(InsightMetric metric, int count = 10, TimeSpan? duration = null);

    /// <summary>
    /// Gets the top N methods by a specific metric.
    /// </summary>
    /// <param name="metric">Metric to sort by (latency, requests, errors, rps)</param>
    /// <param name="count">Number of results to return</param>
    /// <param name="duration">Time window to analyze</param>
    [AlwaysInterleave]
    Task<List<MethodInsight>> GetTopMethods(InsightMetric metric, int count = 10, TimeSpan? duration = null);

    /// <summary>
    /// Gets latency trend for a specific grain type.
    /// </summary>
    /// <param name="grainType">The grain type name</param>
    /// <param name="duration">How far back to look</param>
    /// <param name="bucketSeconds">Time bucket size in seconds</param>
    [AlwaysInterleave]
    Task<List<LatencyTrendPoint>> GetGrainLatencyTrend(string grainType, TimeSpan duration, int bucketSeconds = 60);

    /// <summary>
    /// Gets latency trend for a specific method.
    /// </summary>
    /// <param name="grainType">The grain type name</param>
    /// <param name="methodName">The method name</param>
    /// <param name="duration">How far back to look</param>
    /// <param name="bucketSeconds">Time bucket size in seconds</param>
    [AlwaysInterleave]
    Task<List<LatencyTrendPoint>> GetMethodLatencyTrend(string grainType, string methodName, TimeSpan duration, int bucketSeconds = 60);

    /// <summary>
    /// Detects anomalies by comparing recent metrics to baseline.
    /// Returns grains/methods where current performance deviates significantly from baseline.
    /// </summary>
    /// <param name="recentWindow">Recent time window (e.g., 5 minutes)</param>
    /// <param name="baselineWindow">Baseline time window (e.g., 1 hour)</param>
    /// <param name="thresholdMultiplier">Deviation threshold (e.g., 2.0 = 2x baseline)</param>
    [AlwaysInterleave]
    Task<AnomalyReport> DetectAnomalies(TimeSpan recentWindow, TimeSpan baselineWindow, double thresholdMultiplier = 2.0);

    /// <summary>
    /// Compares performance across silos.
    /// Identifies underperforming or overloaded silos.
    /// </summary>
    /// <param name="duration">Time window to analyze</param>
    [AlwaysInterleave]
    Task<List<SiloComparison>> CompareSilos(TimeSpan duration);
}

/// <summary>
/// Interface for database management operations.
/// Follows ISP by separating admin operations from metric queries.
/// </summary>
public interface IInsightsDatabaseGrain : IGrainWithStringKey
{
    /// <summary>
    /// Gets a summary of database health and retention.
    /// </summary>
    [AlwaysInterleave]
    Task<InsightDatabaseSummary> GetDatabaseSummary();

    /// <summary>
    /// Executes a custom SQL query against the metrics database.
    /// For advanced dashboard visualizations.
    /// </summary>
    /// <param name="sql">SQL query (SELECT only)</param>
    [AlwaysInterleave]
    Task<string> ExecuteQuery(string sql);
}

/// <summary>
/// Interface for ingesting health reports from silos.
/// Each silo periodically reports its health check results to this grain.
/// Uses fire-and-forget semantics for minimal latency impact.
/// </summary>
public interface IHealthIngestGrain : IGrainWithStringKey
{
    /// <summary>
    /// Ingests a health report from a silo.
    /// Called by HealthReportingService on each silo.
    /// Uses [OneWay] for fire-and-forget semantics.
    /// </summary>
    /// <param name="report">The health report from the silo</param>
    [OneWay]
    Task IngestHealthReport(HealthReportData report);
}

/// <summary>
/// Interface for querying cluster-wide health status.
/// Aggregates health reports from all silos for dashboard display.
/// </summary>
public interface IHealthQueryGrain : IGrainWithStringKey
{
    /// <summary>
    /// Gets aggregated health status from all silos.
    /// Returns health reports from all silos that have reported within the stale threshold.
    /// Uses [AlwaysInterleave] for concurrent reads.
    /// </summary>
    [AlwaysInterleave]
    Task<List<SiloHealthReport>> GetClusterHealth();
}

/// <summary>
/// Interface for page-specific dashboard queries.
/// Each method returns exactly the data needed for a specific dashboard page,
/// enabling efficient data fetching in horizontally scaled deployments.
/// All methods use [AlwaysInterleave] for concurrent access.
/// </summary>
public interface IDashboardPageQueryGrain : IGrainWithStringKey
{
    /// <summary>
    /// Gets data for the Overview page - high-level cluster summary.
    /// </summary>
    [AlwaysInterleave]
    Task<OverviewPageData> GetOverviewPageData();

    /// <summary>
    /// Gets data for the Orleans page - full cluster details.
    /// </summary>
    [AlwaysInterleave]
    Task<OrleansPageData> GetOrleansPageData();

    /// <summary>
    /// Gets data for the Health page - all silo health reports.
    /// </summary>
    [AlwaysInterleave]
    Task<HealthPageData> GetHealthPageData();

    /// <summary>
    /// Gets data for the Insights page - anomalies, trends, rankings.
    /// </summary>
    [AlwaysInterleave]
    Task<InsightsPageData> GetInsightsPageData();
}

#endregion

/// <summary>
/// Singleton grain that provides both real-time cluster aggregation and deep historical analytics.
/// Uses in-memory tracking for real-time views and DuckDB for time-series storage.
///
/// This is a composite interface that inherits from all segregated interfaces,
/// following the Interface Segregation Principle (ISP). Clients can depend on
/// only the specific interface they need:
/// - <see cref="IInsightsIngestGrain"/>: For metric ingestion
/// - <see cref="IInsightsRealTimeGrain"/>: For real-time cluster aggregation
/// - <see cref="IMethodProfileQueryGrain"/>: For method profiling queries
/// - <see cref="IOrleansMetricsQueryGrain"/>: For Orleans cluster metrics queries
/// - <see cref="IInsightsDatabaseGrain"/>: For database management operations
///
/// Data Flow:
/// - Receives SiloMetricsReport from MetricAggregatorReporter (Orleans metrics)
/// - Receives MethodProfileReport from GrainMethodProfiler (accurate method metrics)
/// - Maintains in-memory real-time state for immediate queries
/// - Stores time-series data in DuckDB tables using batch appender
///
/// Use Cases:
/// - Real-time cluster status (silos, activations, connected clients)
/// - Latency trends over time
/// - Top-N slowest grains/methods with accurate averages
/// - Anomaly detection (current vs baseline)
/// - Silo performance comparison and health monitoring
/// - Cluster-wide health monitoring with horizontal scaling support
/// - Page-specific queries for efficient dashboard data fetching
/// </summary>
public interface IInsightsGrain :
    IInsightsIngestGrain,
    IInsightsRealTimeGrain,
    IMethodProfileQueryGrain,
    IOrleansMetricsQueryGrain,
    IInsightsDatabaseGrain,
    IHealthIngestGrain,
    IHealthQueryGrain,
    IDashboardPageQueryGrain
{
    /// <summary>
    /// The singleton instance ID for the Insights grain.
    /// </summary>
    public const string SingletonKey = "Insights";
}

/// <summary>
/// Grain interface for broadcasting dashboard data changes to SignalR clients.
/// Implemented in the dashboard host, called from InsightsGrain when data changes.
/// Uses [OneWay] for fire-and-forget calls since broadcast results don't need to be awaited.
/// </summary>
public interface IDashboardBroadcastGrain : IGrainWithIntegerKey
{
    /// <summary>Broadcast Health page data to subscribed clients.</summary>
    [OneWay]
    Task BroadcastHealthData(HealthPageData data);

    /// <summary>Broadcast Overview page data to subscribed clients.</summary>
    [OneWay]
    Task BroadcastOverviewData(OverviewPageData data);

    /// <summary>Broadcast Orleans page data to subscribed clients.</summary>
    [OneWay]
    Task BroadcastOrleansData(OrleansPageData data);

    /// <summary>Broadcast Insights page data to subscribed clients.</summary>
    [OneWay]
    Task BroadcastInsightsData(InsightsPageData data);
}
