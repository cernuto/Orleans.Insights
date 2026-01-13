using Orleans;
using System;
using System.Collections.Generic;

namespace Orleans.Insights.Models;

#region Page Data Models

/// <summary>
/// Data for the Overview page - high-level cluster summary.
/// Provides a quick snapshot of cluster health for horizontal scaling.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record OverviewPageData
{
    /// <summary>Number of active silos in the cluster.</summary>
    [Id(0)] public int SiloCount { get; init; }

    /// <summary>Total grain activations across all silos.</summary>
    [Id(1)] public int TotalGrains { get; init; }

    /// <summary>Average CPU usage across all silos.</summary>
    [Id(2)] public double CpuPercent { get; init; }

    /// <summary>Total memory used across all silos (MB).</summary>
    [Id(3)] public long MemoryUsedMb { get; init; }

    /// <summary>Number of healthy endpoints.</summary>
    [Id(4)] public int HealthyEndpoints { get; init; }

    /// <summary>Number of unhealthy endpoints.</summary>
    [Id(5)] public int UnhealthyEndpoints { get; init; }

    /// <summary>When this data was generated.</summary>
    [Id(6)] public DateTime Timestamp { get; init; }

    /// <summary>List of silos for the cluster status table.</summary>
    [Id(7)] public required List<SiloSummary> Silos { get; init; }

    /// <summary>List of health endpoints for the health status table.</summary>
    [Id(8)] public required List<HealthEndpointSummary> HealthEndpoints { get; init; }
}

/// <summary>
/// Summary of a silo for Overview page display.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record SiloSummary
{
    [Id(0)] public required string Address { get; init; }
    [Id(1)] public required string Status { get; init; }
    [Id(2)] public required string HostName { get; init; }
    [Id(3)] public int ActivationCount { get; init; }
    [Id(4)] public double CpuUsage { get; init; }
    [Id(5)] public long MemoryUsageMb { get; init; }
}

/// <summary>
/// Summary of a health endpoint for Overview page display.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record HealthEndpointSummary
{
    [Id(0)] public required string Name { get; init; }
    [Id(1)] public required string OverallStatus { get; init; }
    [Id(2)] public int CheckCount { get; init; }
}

/// <summary>
/// Data for the Orleans page - full cluster details.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record OrleansPageData
{
    /// <summary>Detailed silo information.</summary>
    [Id(0)] public required List<SiloSummary> Silos { get; init; }

    /// <summary>Per-grain-type statistics.</summary>
    [Id(1)] public required List<GrainStatsSummary> GrainStats { get; init; }

    /// <summary>Top methods by latency.</summary>
    [Id(2)] public required List<MethodProfileSummary> TopMethods { get; init; }

    /// <summary>OTel metrics summary.</summary>
    [Id(3)] public required OrleansOTelSummary OTelMetrics { get; init; }

    /// <summary>When this data was generated.</summary>
    [Id(4)] public DateTime Timestamp { get; init; }
}

/// <summary>
/// Grain statistics summary for Orleans page.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record GrainStatsSummary
{
    [Id(0)] public required string GrainType { get; init; }
    [Id(1)] public int TotalActivations { get; init; }
    [Id(2)] public int SiloCount { get; init; }
    [Id(3)] public double RequestsPerSecond { get; init; }
    [Id(4)] public double AverageLatencyMs { get; init; }
    [Id(5)] public double ExceptionRate { get; init; }
    [Id(6)] public long TotalRequests { get; init; }
    [Id(7)] public long FailedRequests { get; init; }
    /// <summary>Per-silo breakdown of this grain type's stats. Key is silo address.</summary>
    [Id(8)] public Dictionary<string, SiloGrainStats>? PerSiloStats { get; init; }
}

/// <summary>
/// Per-silo grain statistics for a single grain type.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record SiloGrainStats
{
    [Id(0)] public int Activations { get; init; }
    [Id(1)] public double RequestsPerSecond { get; init; }
    [Id(2)] public double AverageLatencyMs { get; init; }
    [Id(3)] public long TotalRequests { get; init; }
    [Id(4)] public long FailedRequests { get; init; }
}

/// <summary>
/// Orleans OTel metrics summary.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record OrleansOTelSummary
{
    [Id(0)] public long TotalActivations { get; init; }
    [Id(1)] public int ConnectedClients { get; init; }
    [Id(2)] public long MessagesSent { get; init; }
    [Id(3)] public long MessagesReceived { get; init; }
    [Id(4)] public long MessagesDropped { get; init; }
    [Id(5)] public double CpuUsagePercent { get; init; }
    [Id(6)] public long MemoryUsageMb { get; init; }
    [Id(7)] public double AverageRequestLatencyMs { get; init; }
    [Id(8)] public long TotalRequests { get; init; }

    // Catalog metrics (activation lifecycle)
    [Id(9)] public long ActivationWorkingSet { get; init; }
    [Id(10)] public long ActivationsCreated { get; init; }
    [Id(11)] public long ActivationsDestroyed { get; init; }
    [Id(12)] public long ActivationsFailedToActivate { get; init; }
    [Id(13)] public long ActivationCollections { get; init; }
    [Id(14)] public long ActivationShutdowns { get; init; }
    [Id(15)] public long ActivationNonExistent { get; init; }
    [Id(16)] public long ConcurrentRegistrationAttempts { get; init; }

    // Miscellaneous grain metrics
    [Id(17)] public long GrainCount { get; init; }
    [Id(18)] public long SystemTargets { get; init; }
}

/// <summary>
/// Data for the Health page - all silo health reports.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record HealthPageData
{
    /// <summary>Health reports from all silos.</summary>
    [Id(0)] public required List<SiloHealthReport> SiloReports { get; init; }

    /// <summary>When this data was generated.</summary>
    [Id(1)] public DateTime Timestamp { get; init; }

    /// <summary>Total healthy check count across all silos.</summary>
    [Id(2)] public int HealthyCount { get; init; }

    /// <summary>Total degraded check count across all silos.</summary>
    [Id(3)] public int DegradedCount { get; init; }

    /// <summary>Total unhealthy check count across all silos.</summary>
    [Id(4)] public int UnhealthyCount { get; init; }
}

/// <summary>
/// Data for the Insights page - anomalies, trends, rankings.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record InsightsPageData
{
    /// <summary>Cluster metrics trend over time.</summary>
    [Id(0)] public required List<ClusterMetricsTrend> ClusterTrend { get; init; }

    /// <summary>Top grain types by latency.</summary>
    [Id(1)] public required List<GrainTypeInsight> TopGrainsByLatency { get; init; }

    /// <summary>Top grain types by throughput.</summary>
    [Id(2)] public required List<GrainTypeInsight> TopGrainsByThroughput { get; init; }

    /// <summary>Detected latency anomalies.</summary>
    [Id(3)] public required List<LatencyAnomaly> LatencyAnomalies { get; init; }

    /// <summary>Detected error rate anomalies.</summary>
    [Id(4)] public required List<ErrorRateAnomaly> ErrorRateAnomalies { get; init; }

    /// <summary>Silo performance comparisons.</summary>
    [Id(5)] public required List<SiloComparison> SiloComparisons { get; init; }

    /// <summary>When this data was generated.</summary>
    [Id(6)] public DateTime Timestamp { get; init; }

    /// <summary>Top methods by latency.</summary>
    [Id(7)] public required List<MethodProfileSummary> TopMethodsByLatency { get; init; }

    /// <summary>Top methods by exceptions.</summary>
    [Id(8)] public required List<MethodProfileSummary> TopMethodsByExceptions { get; init; }

    /// <summary>Database summary.</summary>
    [Id(9)] public InsightDatabaseSummary? DatabaseSummary { get; init; }
}

#endregion
