using Orleans;
using System;
using System.Collections.Generic;

namespace Orleans.Insights.Models;

#region Query Result DTOs

/// <summary>
/// Metrics to sort insights by.
/// </summary>
public enum InsightMetric
{
    Latency,
    Requests,
    Errors,
    Throughput,
    ErrorRate
}

/// <summary>
/// A time-bucketed cluster metrics trend point.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record ClusterMetricsTrend
{
    [Id(0)] public DateTime Timestamp { get; init; }
    [Id(1)] public int SiloCount { get; init; }
    [Id(2)] public long TotalActivations { get; init; }
    [Id(3)] public long TotalRequests { get; init; }
    [Id(4)] public double AvgLatencyMs { get; init; }
    [Id(5)] public double AvgCpuPercent { get; init; }
    [Id(6)] public long TotalMemoryMb { get; init; }
    [Id(7)] public long MessagesDropped { get; init; }
    [Id(8)] public double RequestsPerSecond { get; init; }
}

/// <summary>
/// Insight about a grain type's performance.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record GrainTypeInsight
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public long TotalRequests { get; init; }
    [Id(2)] public long FailedRequests { get; init; }
    [Id(3)] public double AvgLatencyMs { get; init; }
    [Id(4)] public double MinLatencyMs { get; init; }
    [Id(5)] public double MaxLatencyMs { get; init; }
    [Id(6)] public double RequestsPerSecond { get; init; }
    [Id(7)] public double ErrorRate { get; init; }
    [Id(8)] public int SiloCount { get; init; }
}

/// <summary>
/// Insight about a method's performance.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record MethodInsight
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public required string MethodName { get; init; }
    [Id(2)] public long TotalRequests { get; init; }
    [Id(3)] public long FailedRequests { get; init; }
    [Id(4)] public double AvgLatencyMs { get; init; }
    [Id(5)] public double RequestsPerSecond { get; init; }
    [Id(6)] public double ErrorRate { get; init; }
    [Id(7)] public int SiloCount { get; init; }
}

/// <summary>
/// A time-bucketed latency trend point.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record LatencyTrendPoint
{
    [Id(0)] public DateTime Timestamp { get; init; }
    [Id(1)] public double AvgLatencyMs { get; init; }
    [Id(2)] public double MinLatencyMs { get; init; }
    [Id(3)] public double MaxLatencyMs { get; init; }
    [Id(4)] public long RequestCount { get; init; }
    [Id(5)] public double RequestsPerSecond { get; init; }
}

/// <summary>
/// Report of detected performance anomalies.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record AnomalyReport
{
    [Id(0)] public DateTime GeneratedAt { get; init; }
    [Id(1)] public TimeSpan RecentWindow { get; init; }
    [Id(2)] public TimeSpan BaselineWindow { get; init; }
    [Id(3)] public double ThresholdMultiplier { get; init; }
    [Id(4)] public List<LatencyAnomaly> LatencyAnomalies { get; init; } = [];
    [Id(5)] public List<ErrorRateAnomaly> ErrorRateAnomalies { get; init; } = [];
}

/// <summary>
/// A latency anomaly detection result.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record LatencyAnomaly
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public string? MethodName { get; init; }
    [Id(2)] public double CurrentLatencyMs { get; init; }
    [Id(3)] public double BaselineLatencyMs { get; init; }
    [Id(4)] public double DeviationMultiplier { get; init; }
}

/// <summary>
/// An error rate anomaly detection result.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record ErrorRateAnomaly
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public string? MethodName { get; init; }
    [Id(2)] public double CurrentErrorRate { get; init; }
    [Id(3)] public double BaselineErrorRate { get; init; }
    [Id(4)] public double DeviationMultiplier { get; init; }
}

/// <summary>
/// Comparison of silo performance.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record SiloComparison
{
    [Id(0)] public required string SiloId { get; init; }
    [Id(1)] public required string HostName { get; init; }
    [Id(2)] public double AvgCpuPercent { get; init; }
    [Id(3)] public long AvgMemoryMb { get; init; }
    [Id(4)] public double AvgLatencyMs { get; init; }
    [Id(5)] public long TotalRequests { get; init; }
    [Id(6)] public double RequestsPerSecond { get; init; }
    [Id(7)] public long MessagesDropped { get; init; }
    [Id(8)] public double PerformanceScore { get; init; }
}

#endregion

#region Method Profile DTOs (Accurate)

/// <summary>
/// A time-bucketed method profile trend point with accurate averages.
/// Uses totals-based calculation: avg = total_elapsed_ms / call_count.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record MethodProfileTrendPoint
{
    [Id(0)] public DateTime Timestamp { get; init; }
    [Id(1)] public required string GrainType { get; init; }
    [Id(2)] public required string MethodName { get; init; }
    [Id(3)] public long CallCount { get; init; }
    [Id(4)] public double TotalElapsedMs { get; init; }
    [Id(5)] public long ExceptionCount { get; init; }
    /// <summary>
    /// Accurate average latency: TotalElapsedMs / CallCount
    /// </summary>
    [Id(6)] public double AvgLatencyMs { get; init; }
    [Id(7)] public double CallsPerSecond { get; init; }
    [Id(8)] public double ExceptionRate { get; init; }
}

/// <summary>
/// Summary of method profile for top-N queries.
/// Aggregated across all silos and time buckets.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record MethodProfileSummary
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public required string MethodName { get; init; }
    [Id(2)] public long TotalCalls { get; init; }
    [Id(3)] public double TotalElapsedMs { get; init; }
    [Id(4)] public long TotalExceptions { get; init; }
    /// <summary>
    /// Accurate average latency: TotalElapsedMs / TotalCalls
    /// </summary>
    [Id(5)] public double AvgLatencyMs { get; init; }
    [Id(6)] public double CallsPerSecond { get; init; }
    [Id(7)] public double ExceptionRate { get; init; }
    [Id(8)] public int SiloCount { get; init; }
    [Id(9)] public DateTime LastUpdated { get; init; }
}

/// <summary>
/// Method profile data broken down by silo.
/// Shows load distribution across silos for a specific method.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record MethodProfileSiloSummary
{
    [Id(0)] public required string SiloId { get; init; }
    [Id(1)] public required string HostName { get; init; }
    [Id(2)] public long TotalCalls { get; init; }
    [Id(3)] public double TotalElapsedMs { get; init; }
    [Id(4)] public long TotalExceptions { get; init; }
    /// <summary>
    /// Accurate average latency: TotalElapsedMs / TotalCalls
    /// </summary>
    [Id(5)] public double AvgLatencyMs { get; init; }
    [Id(6)] public double CallsPerSecond { get; init; }
    [Id(7)] public double ExceptionRate { get; init; }
    [Id(8)] public DateTime LastUpdated { get; init; }
}

#endregion

#region Database Summary DTOs

/// <summary>
/// Summary of the Insights database health and contents.
/// </summary>
[Immutable]
[GenerateSerializer]
public sealed record InsightDatabaseSummary
{
    [Id(0)] public DateTime GeneratedAt { get; init; }
    [Id(1)] public long EstimatedSizeBytes { get; init; }
    [Id(2)] public required string EstimatedSizeFormatted { get; init; }
    [Id(3)] public long TotalRows { get; init; }
    [Id(4)] public TimeSpan RetentionPeriod { get; init; }
    [Id(5)] public DateTime OldestDataPoint { get; init; }
    [Id(6)] public DateTime NewestDataPoint { get; init; }
    [Id(7)] public int UniqueSilos { get; init; }
    [Id(8)] public int UniqueGrainTypes { get; init; }
    [Id(9)] public int UniqueMethods { get; init; }
}

#endregion
