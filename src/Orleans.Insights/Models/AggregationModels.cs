using Orleans;
using System;
using System.Collections.Generic;

namespace Orleans.Insights.Models;

/// <summary>
/// Metrics report sent from a single silo to InsightsGrain.
/// Contains all local metrics collected by the silo's OrleansMetricsListener.
/// Immutable record for safe transfer across Orleans grain boundaries.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record SiloMetricsReport
{
    /// <summary>
    /// Unique identifier for the silo (typically the SiloAddress.ToString()).
    /// </summary>
    [Id(0)]
    public required string SiloId { get; init; }

    /// <summary>
    /// The host name or machine name running the silo.
    /// </summary>
    [Id(1)]
    public required string HostName { get; init; }

    /// <summary>
    /// Timestamp when these metrics were collected.
    /// </summary>
    [Id(2)]
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Orleans cluster-level metrics from this silo's perspective.
    /// </summary>
    [Id(3)]
    public required SiloClusterMetrics ClusterMetrics { get; init; }

    /// <summary>
    /// Per-grain-type metrics collected from this silo.
    /// Key is the grain type name.
    /// </summary>
    [Id(4)]
    public Dictionary<string, SiloGrainTypeMetrics> GrainTypeMetrics { get; init; } = new();

    /// <summary>
    /// Per-method metrics collected from this silo.
    /// Key is "{GrainType}::{MethodName}".
    /// </summary>
    [Id(5)]
    public Dictionary<string, SiloMethodMetrics> MethodMetrics { get; init; } = new();
}

/// <summary>
/// Cluster-level metrics from a single silo.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record SiloClusterMetrics
{
    [Id(0)] public long TotalActivations { get; init; }
    [Id(1)] public int ConnectedClients { get; init; }
    [Id(2)] public long MessagesSent { get; init; }
    [Id(3)] public long MessagesReceived { get; init; }
    [Id(4)] public long MessagesDropped { get; init; }
    [Id(5)] public double CpuUsagePercent { get; init; }
    [Id(6)] public long MemoryUsageMb { get; init; }
    [Id(7)] public long AvailableMemoryMb { get; init; }
    [Id(8)] public double AverageRequestLatencyMs { get; init; }
    [Id(9)] public long TotalRequests { get; init; }

    // Catalog metrics (activation lifecycle)
    [Id(10)] public long ActivationWorkingSet { get; init; }
    [Id(11)] public long ActivationsCreated { get; init; }
    [Id(12)] public long ActivationsDestroyed { get; init; }
    [Id(13)] public long ActivationsFailedToActivate { get; init; }
    [Id(14)] public long ActivationCollections { get; init; }
    [Id(15)] public long ActivationShutdowns { get; init; }
    [Id(16)] public long ActivationNonExistent { get; init; }
    [Id(17)] public long ConcurrentRegistrationAttempts { get; init; }

    // Miscellaneous grain metrics
    [Id(18)] public long GrainCount { get; init; }
    [Id(19)] public long SystemTargets { get; init; }
}

/// <summary>
/// Per-grain-type metrics from a single silo.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record SiloGrainTypeMetrics
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public long TotalRequests { get; init; }
    [Id(2)] public long FailedRequests { get; init; }
    [Id(3)] public double AverageLatencyMs { get; init; }
    [Id(4)] public double MinLatencyMs { get; init; }
    [Id(5)] public double MaxLatencyMs { get; init; }
    [Id(6)] public double RequestsPerSecond { get; init; }
    [Id(7)] public DateTime LastUpdated { get; init; }
    /// <summary>Activation count from orleans-grains metric with grain type tag.</summary>
    [Id(8)] public long Activations { get; init; }
}

/// <summary>
/// Per-method metrics from a single silo.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record SiloMethodMetrics
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public required string MethodName { get; init; }
    [Id(2)] public long TotalRequests { get; init; }
    [Id(3)] public long FailedRequests { get; init; }
    [Id(4)] public double AverageLatencyMs { get; init; }
    [Id(5)] public double RequestsPerSecond { get; init; }
    [Id(6)] public DateTime LastUpdated { get; init; }

    /// <summary>
    /// Recent time-series samples for method profiling charts.
    /// Limited to most recent samples to control message size.
    /// </summary>
    [Id(7)]
    public List<MethodSample> RecentSamples { get; init; } = new();
}

/// <summary>
/// A single time-series sample for method profiling.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record MethodSample
{
    [Id(0)] public DateTime Timestamp { get; init; }
    [Id(1)] public double RequestsPerSecond { get; init; }
    [Id(2)] public double LatencyMs { get; init; }
    [Id(3)] public double FailedCount { get; init; }
}

/// <summary>
/// Aggregated cluster metrics returned by InsightsGrain.
/// Combines metrics from all reporting silos.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record AggregatedClusterMetrics
{
    /// <summary>
    /// Number of silos currently reporting metrics.
    /// </summary>
    [Id(0)]
    public int ReportingSiloCount { get; init; }

    /// <summary>
    /// Timestamp when this aggregation was computed.
    /// </summary>
    [Id(1)]
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Aggregated cluster-level metrics (sums/averages across all silos).
    /// </summary>
    [Id(2)]
    public required AggregatedOrleansMetrics ClusterMetrics { get; init; }

    /// <summary>
    /// Aggregated per-grain-type metrics.
    /// Key is the grain type name.
    /// </summary>
    [Id(3)]
    public Dictionary<string, AggregatedGrainTypeMetrics> GrainTypeMetrics { get; init; } = new();

    /// <summary>
    /// Aggregated per-method metrics.
    /// Key is "{GrainType}::{MethodName}".
    /// </summary>
    [Id(4)]
    public Dictionary<string, AggregatedMethodMetrics> MethodMetrics { get; init; } = new();
}

/// <summary>
/// Aggregated Orleans cluster metrics across all silos.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record AggregatedOrleansMetrics
{
    /// <summary>Sum of activations across all silos.</summary>
    [Id(0)] public long TotalActivations { get; init; }

    /// <summary>Sum of connected clients across all silos.</summary>
    [Id(1)] public int TotalConnectedClients { get; init; }

    /// <summary>Sum of messages sent across all silos.</summary>
    [Id(2)] public long TotalMessagesSent { get; init; }

    /// <summary>Sum of messages received across all silos.</summary>
    [Id(3)] public long TotalMessagesReceived { get; init; }

    /// <summary>Sum of messages dropped across all silos.</summary>
    [Id(4)] public long TotalMessagesDropped { get; init; }

    /// <summary>Average CPU usage across all silos.</summary>
    [Id(5)] public double AverageCpuUsagePercent { get; init; }

    /// <summary>Sum of memory usage across all silos.</summary>
    [Id(6)] public long TotalMemoryUsageMb { get; init; }

    /// <summary>Sum of available memory across all silos.</summary>
    [Id(7)] public long TotalAvailableMemoryMb { get; init; }

    /// <summary>Weighted average request latency across all silos.</summary>
    [Id(8)] public double AverageRequestLatencyMs { get; init; }

    /// <summary>Sum of total requests across all silos.</summary>
    [Id(9)] public long TotalRequests { get; init; }

    // Catalog metrics (activation lifecycle) - summed across all silos
    [Id(10)] public long TotalActivationWorkingSet { get; init; }
    [Id(11)] public long TotalActivationsCreated { get; init; }
    [Id(12)] public long TotalActivationsDestroyed { get; init; }
    [Id(13)] public long TotalActivationsFailedToActivate { get; init; }
    [Id(14)] public long TotalActivationCollections { get; init; }
    [Id(15)] public long TotalActivationShutdowns { get; init; }
    [Id(16)] public long TotalActivationNonExistent { get; init; }
    [Id(17)] public long TotalConcurrentRegistrationAttempts { get; init; }

    // Miscellaneous grain metrics - summed across all silos
    [Id(18)] public long TotalGrainCount { get; init; }
    [Id(19)] public long TotalSystemTargets { get; init; }
}

/// <summary>
/// Aggregated metrics for a grain type across all silos.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record AggregatedGrainTypeMetrics
{
    [Id(0)] public required string GrainType { get; init; }

    /// <summary>Sum of total requests across all silos.</summary>
    [Id(1)] public long TotalRequests { get; init; }

    /// <summary>Sum of failed requests across all silos.</summary>
    [Id(2)] public long FailedRequests { get; init; }

    /// <summary>Weighted average latency across all silos.</summary>
    [Id(3)] public double AverageLatencyMs { get; init; }

    /// <summary>Minimum latency observed across all silos.</summary>
    [Id(4)] public double MinLatencyMs { get; init; }

    /// <summary>Maximum latency observed across all silos.</summary>
    [Id(5)] public double MaxLatencyMs { get; init; }

    /// <summary>Sum of requests per second across all silos.</summary>
    [Id(6)] public double RequestsPerSecond { get; init; }

    /// <summary>Exception rate calculated from aggregated requests.</summary>
    [Id(7)] public double ExceptionRate { get; init; }

    /// <summary>Number of silos reporting metrics for this grain type.</summary>
    [Id(8)] public int ReportingSiloCount { get; init; }

    /// <summary>Most recent update from any silo.</summary>
    [Id(9)] public DateTime LastUpdated { get; init; }

    /// <summary>Per-silo breakdown of metrics for this grain type.</summary>
    [Id(10)] public List<SiloGrainBreakdown> SiloBreakdown { get; init; } = new();

    /// <summary>Sum of activations across all silos for this grain type.</summary>
    [Id(11)] public long TotalActivations { get; init; }
}

/// <summary>
/// Per-silo metrics breakdown for a grain type.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record SiloGrainBreakdown
{
    [Id(0)] public required string SiloId { get; init; }
    [Id(1)] public required string HostName { get; init; }
    [Id(2)] public long TotalRequests { get; init; }
    [Id(3)] public double RequestsPerSecond { get; init; }
    [Id(4)] public double AverageLatencyMs { get; init; }
    [Id(5)] public long FailedRequests { get; init; }
}

/// <summary>
/// Aggregated metrics for a method across all silos.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record AggregatedMethodMetrics
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public required string MethodName { get; init; }

    /// <summary>Sum of total requests across all silos.</summary>
    [Id(2)] public long TotalRequests { get; init; }

    /// <summary>Sum of failed requests across all silos.</summary>
    [Id(3)] public long FailedRequests { get; init; }

    /// <summary>Weighted average latency across all silos.</summary>
    [Id(4)] public double AverageLatencyMs { get; init; }

    /// <summary>Sum of requests per second across all silos.</summary>
    [Id(5)] public double RequestsPerSecond { get; init; }

    /// <summary>Number of silos reporting metrics for this method.</summary>
    [Id(6)] public int ReportingSiloCount { get; init; }

    /// <summary>Most recent update from any silo.</summary>
    [Id(7)] public DateTime LastUpdated { get; init; }

    /// <summary>
    /// Merged time-series samples from all silos.
    /// Samples are aggregated by timestamp (summing throughput, averaging latency).
    /// </summary>
    [Id(8)]
    public List<MethodSample> RecentSamples { get; init; } = new();

    /// <summary>Per-silo breakdown of metrics for this method.</summary>
    [Id(9)] public List<SiloMethodBreakdown> SiloBreakdown { get; init; } = new();
}

/// <summary>
/// Per-silo metrics breakdown for a method.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record SiloMethodBreakdown
{
    [Id(0)] public required string SiloId { get; init; }
    [Id(1)] public required string HostName { get; init; }
    [Id(2)] public long TotalRequests { get; init; }
    [Id(3)] public double RequestsPerSecond { get; init; }
    [Id(4)] public double AverageLatencyMs { get; init; }
    [Id(5)] public long FailedRequests { get; init; }
}

/// <summary>
/// Information about a silo reporting metrics.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record SiloMetricsInfo
{
    [Id(0)] public required string SiloId { get; init; }
    [Id(1)] public required string HostName { get; init; }
    [Id(2)] public DateTime LastReportTime { get; init; }
    [Id(3)] public bool IsStale { get; init; }
}

#region Method Profiling (OrleansDashboard Pattern)

/// <summary>
/// Method profile report sent from GrainMethodProfiler to InsightsGrain.
/// Contains accumulated method trace data from the last reporting interval (1 second).
///
/// Key difference from OTel-based metrics:
/// - Stores TOTALS (count + totalElapsedMs) not averages
/// - Enables accurate average calculation: avg = totalElapsedMs / count
/// - Atomic swap ensures no data loss and accurate aggregation
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record MethodProfileReport
{
    /// <summary>
    /// Unique identifier for the silo.
    /// </summary>
    [Id(0)]
    public required string SiloId { get; init; }

    /// <summary>
    /// Host name of the silo.
    /// </summary>
    [Id(1)]
    public required string HostName { get; init; }

    /// <summary>
    /// Timestamp when this report was generated.
    /// </summary>
    [Id(2)]
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Method trace entries accumulated during the reporting interval.
    /// </summary>
    [Id(3)]
    public List<MethodProfileEntry> Entries { get; init; } = new();
}

/// <summary>
/// A single method's accumulated trace data for one reporting interval.
/// Immutable for safe transfer across Orleans grain boundaries.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record MethodProfileEntry
{
    /// <summary>
    /// Short grain type name (e.g., "Device" not "MyApp.Grains.DeviceGrain").
    /// </summary>
    [Id(0)]
    public required string GrainType { get; init; }

    /// <summary>
    /// Method name (e.g., "GetState").
    /// </summary>
    [Id(1)]
    public required string Method { get; init; }

    /// <summary>
    /// Number of calls during this interval.
    /// </summary>
    [Id(2)]
    public long Count { get; init; }

    /// <summary>
    /// Total elapsed time in milliseconds for all calls.
    /// Average latency = TotalElapsedMs / Count
    /// </summary>
    [Id(3)]
    public double TotalElapsedMs { get; init; }

    /// <summary>
    /// Number of calls that threw exceptions.
    /// Exception rate = ExceptionCount / Count * 100
    /// </summary>
    [Id(4)]
    public long ExceptionCount { get; init; }
}

/// <summary>
/// Aggregated method profile data from all silos for a specific method.
/// Used by the dashboard to display accurate method profiling.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record AggregatedMethodProfile
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public required string MethodName { get; init; }

    /// <summary>Total calls across all silos in the current aggregation window.</summary>
    [Id(2)] public long TotalCount { get; init; }

    /// <summary>Total elapsed time across all silos (for accurate average calculation).</summary>
    [Id(3)] public double TotalElapsedMs { get; init; }

    /// <summary>Total exceptions across all silos.</summary>
    [Id(4)] public long TotalExceptions { get; init; }

    /// <summary>Number of silos reporting data for this method.</summary>
    [Id(5)] public int ReportingSiloCount { get; init; }

    /// <summary>Most recent update timestamp.</summary>
    [Id(6)] public DateTime LastUpdated { get; init; }

    /// <summary>Per-silo breakdown for this method.</summary>
    [Id(7)] public List<MethodProfileSiloBreakdown> SiloBreakdown { get; init; } = new();

    /// <summary>Time-series history for charting (rolling window).</summary>
    [Id(8)] public List<MethodProfileHistoryEntry> History { get; init; } = new();

    /// <summary>Calculated average latency: TotalElapsedMs / TotalCount</summary>
    public double AverageLatencyMs => TotalCount > 0 ? TotalElapsedMs / TotalCount : 0;

    /// <summary>Calculated exception rate: (TotalExceptions / TotalCount) * 100</summary>
    public double ExceptionRate => TotalCount > 0 ? (TotalExceptions / (double)TotalCount) * 100 : 0;
}

/// <summary>
/// Per-silo breakdown for method profiling.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record MethodProfileSiloBreakdown
{
    [Id(0)] public required string SiloId { get; init; }
    [Id(1)] public required string HostName { get; init; }
    [Id(2)] public long Count { get; init; }
    [Id(3)] public double TotalElapsedMs { get; init; }
    [Id(4)] public long ExceptionCount { get; init; }
    [Id(5)] public DateTime LastUpdated { get; init; }

    /// <summary>Average latency for this silo.</summary>
    public double AverageLatencyMs => Count > 0 ? TotalElapsedMs / Count : 0;

    /// <summary>Requests per second (based on 1-second reporting interval).</summary>
    public double RequestsPerSecond => Count; // Each report is 1 second of data
}

/// <summary>
/// A single time-series entry in the method profile history.
/// Each entry represents one aggregation period (1 second).
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record MethodProfileHistoryEntry
{
    /// <summary>Timestamp for this entry (truncated to second).</summary>
    [Id(0)] public DateTime Timestamp { get; init; }

    /// <summary>Total calls during this second (cluster-wide).</summary>
    [Id(1)] public long Count { get; init; }

    /// <summary>Total elapsed time during this second.</summary>
    [Id(2)] public double TotalElapsedMs { get; init; }

    /// <summary>Total exceptions during this second.</summary>
    [Id(3)] public long ExceptionCount { get; init; }

    /// <summary>Average latency for this second.</summary>
    public double AverageLatencyMs => Count > 0 ? TotalElapsedMs / Count : 0;

    /// <summary>Requests per second (equals Count since each entry is 1 second).</summary>
    public double RequestsPerSecond => Count;
}

#endregion
