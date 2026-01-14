using System;
using System.Collections.Generic;
using System.Text.Json;

namespace Orleans.Insights.Dashboard.Client.Models;

/// <summary>
/// JSON serializer options for deserializing page data.
/// </summary>
public static class JsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public static T? Deserialize<T>(JsonElement json) where T : class
    {
        return json.Deserialize<T>(Default);
    }
}

#region Overview Page

public record OverviewPageViewModel
{
    public int SiloCount { get; init; }
    public int TotalGrains { get; init; }
    public double CpuPercent { get; init; }
    public long MemoryUsedMb { get; init; }
    public DateTime Timestamp { get; init; }
    public List<SiloSummaryViewModel> Silos { get; init; } = [];

    public static OverviewPageViewModel FromJson(JsonElement json)
        => json.Deserialize<OverviewPageViewModel>(JsonOptions.Default) ?? Empty;

    public static OverviewPageViewModel Empty => new()
    {
        Silos = [],
        Timestamp = DateTime.UtcNow
    };
}

public record SiloSummaryViewModel
{
    public string Address { get; init; } = "";
    public string Status { get; init; } = "";
    public string HostName { get; init; } = "";
    public int ActivationCount { get; init; }
    public double CpuUsage { get; init; }
    public long MemoryUsageMb { get; init; }
}

#endregion

#region Orleans Page

public record OrleansPageViewModel
{
    public List<SiloSummaryViewModel> Silos { get; init; } = [];
    public List<GrainStatsSummaryViewModel> GrainStats { get; init; } = [];
    public List<MethodProfileSummaryViewModel> TopMethods { get; init; } = [];
    public OrleansOTelSummaryViewModel OTelMetrics { get; init; } = new();
    public DateTime Timestamp { get; init; }

    public static OrleansPageViewModel FromJson(JsonElement json)
        => json.Deserialize<OrleansPageViewModel>(JsonOptions.Default) ?? Empty;

    public static OrleansPageViewModel Empty => new()
    {
        Silos = [],
        GrainStats = [],
        TopMethods = [],
        OTelMetrics = new(),
        Timestamp = DateTime.UtcNow
    };
}

public record GrainStatsSummaryViewModel
{
    public string GrainType { get; init; } = "";
    public int TotalActivations { get; init; }
    public int SiloCount { get; init; }
    public double RequestsPerSecond { get; init; }
    public double AverageLatencyMs { get; init; }
    public double ExceptionRate { get; init; }
    public long TotalRequests { get; init; }
    public long FailedRequests { get; init; }
    /// <summary>Per-silo stats for this grain type. Key is silo address.</summary>
    public Dictionary<string, SiloGrainStatsViewModel>? PerSiloStats { get; init; }
}

public record SiloGrainStatsViewModel
{
    public int Activations { get; init; }
    public double RequestsPerSecond { get; init; }
    public double AverageLatencyMs { get; init; }
    public long TotalRequests { get; init; }
    public long FailedRequests { get; init; }
}

public record OrleansOTelSummaryViewModel
{
    public long TotalActivations { get; init; }
    public int ConnectedClients { get; init; }
    public long MessagesSent { get; init; }
    public long MessagesReceived { get; init; }
    public long MessagesDropped { get; init; }
    public double CpuUsagePercent { get; init; }
    public long MemoryUsageMb { get; init; }
    public double AverageRequestLatencyMs { get; init; }
    public long TotalRequests { get; init; }

    // Catalog metrics (activation lifecycle)
    public long ActivationWorkingSet { get; init; }
    public long ActivationsCreated { get; init; }
    public long ActivationsDestroyed { get; init; }
    public long ActivationsFailedToActivate { get; init; }
    public long ActivationCollections { get; init; }
    public long ActivationShutdowns { get; init; }
    public long ActivationNonExistent { get; init; }
    public long ConcurrentRegistrationAttempts { get; init; }

    // Miscellaneous grain metrics
    public long GrainCount { get; init; }
    public long SystemTargets { get; init; }
}

public record MethodProfileSummaryViewModel
{
    public string GrainType { get; init; } = "";
    public string MethodName { get; init; } = "";
    /// <summary>Average latency in milliseconds. Maps to server's AvgLatencyMs.</summary>
    public double AvgLatencyMs { get; init; }
    public long TotalCalls { get; init; }
    public long TotalExceptions { get; init; }
    public double ExceptionRate { get; init; }
    public double RequestsPerSecond { get; init; }
    public double CallsPerSecond { get; init; }
}

#endregion

#region Insights Page

public record InsightsPageViewModel
{
    public List<ClusterMetricsTrendViewModel> ClusterTrend { get; init; } = [];
    public List<GrainTypeInsightViewModel> TopGrainsByLatency { get; init; } = [];
    public List<GrainTypeInsightViewModel> TopGrainsByThroughput { get; init; } = [];
    public List<LatencyAnomalyViewModel> LatencyAnomalies { get; init; } = [];
    public List<ErrorRateAnomalyViewModel> ErrorRateAnomalies { get; init; } = [];
    public List<SiloComparisonViewModel> SiloComparisons { get; init; } = [];
    public DateTime Timestamp { get; init; }
    public List<MethodProfileSummaryViewModel> TopMethodsByLatency { get; init; } = [];
    public List<MethodProfileSummaryViewModel> TopMethodsByExceptions { get; init; } = [];
    public InsightDatabaseSummaryViewModel? DatabaseSummary { get; init; }

    public static InsightsPageViewModel FromJson(JsonElement json)
        => json.Deserialize<InsightsPageViewModel>(JsonOptions.Default) ?? Empty;

    public static InsightsPageViewModel Empty => new()
    {
        ClusterTrend = [],
        TopGrainsByLatency = [],
        TopGrainsByThroughput = [],
        LatencyAnomalies = [],
        ErrorRateAnomalies = [],
        SiloComparisons = [],
        TopMethodsByLatency = [],
        TopMethodsByExceptions = [],
        Timestamp = DateTime.UtcNow
    };
}

public record ClusterMetricsTrendViewModel
{
    public DateTime Timestamp { get; init; }
    public int Activations { get; init; }
    public double CpuPercent { get; init; }
    public long MemoryMb { get; init; }
    public double RequestsPerSecond { get; init; }
}

public record GrainTypeInsightViewModel
{
    public string GrainType { get; init; } = "";
    /// <summary>Average latency in milliseconds. Maps to server's AvgLatencyMs.</summary>
    public double AvgLatencyMs { get; init; }
    public double RequestsPerSecond { get; init; }
    public long TotalRequests { get; init; }
    public double ErrorRate { get; init; }

    // Convenience property for backward compatibility
    public double AverageLatencyMs => AvgLatencyMs;
}

public record LatencyAnomalyViewModel
{
    public string GrainType { get; init; } = "";
    public string MethodName { get; init; } = "";
    public double CurrentLatencyMs { get; init; }
    public double BaselineLatencyMs { get; init; }
    public double DeviationPercent { get; init; }
    public double DeviationMultiplier { get; init; }
}

public record ErrorRateAnomalyViewModel
{
    public string GrainType { get; init; } = "";
    public string MethodName { get; init; } = "";
    public double CurrentErrorRate { get; init; }
    public double BaselineErrorRate { get; init; }
    public double DeviationPercent { get; init; }
    public double DeviationMultiplier { get; init; }
}

public record SiloComparisonViewModel
{
    /// <summary>Silo identifier. Maps to server's SiloId.</summary>
    public string SiloId { get; init; } = "";
    public string HostName { get; init; } = "";
    public double AvgLatencyMs { get; init; }
    public double RequestsPerSecond { get; init; }
    public double AvgCpuPercent { get; init; }
    public long AvgMemoryMb { get; init; }
    public long TotalRequests { get; init; }
    public double PerformanceScore { get; init; }
}

public record InsightDatabaseSummaryViewModel
{
    public long TotalRows { get; init; }
    public long EstimatedSizeBytes { get; init; }
    public string EstimatedSizeFormatted { get; init; } = "";
    public DateTime OldestRecord { get; init; }
    public DateTime NewestRecord { get; init; }
    public DateTime OldestDataPoint { get; init; }
    public DateTime NewestDataPoint { get; init; }
    public int RetentionDays { get; init; }
    public TimeSpan RetentionPeriod { get; init; }
    public int UniqueSilos { get; init; }
    public int UniqueGrainTypes { get; init; }
    public int UniqueMethods { get; init; }
}

#endregion

#region Chart Models

/// <summary>
/// Data point for pie chart visualization.
/// </summary>
public record PieChartDataPoint(string Label, double Value, string? Color = null);

/// <summary>
/// Time-bucketed method profile trend point for streaming charts.
/// Mirrors Orleans.Insights.Models.MethodProfileTrendPoint.
/// </summary>
public record MethodProfileTrendViewModel
{
    public DateTime Timestamp { get; init; }
    public string GrainType { get; init; } = "";
    public string MethodName { get; init; } = "";
    public long CallCount { get; init; }
    public double TotalElapsedMs { get; init; }
    public long ExceptionCount { get; init; }
    public double AvgLatencyMs { get; init; }
    public double CallsPerSecond { get; init; }
    public double ExceptionRate { get; init; }

    public static List<MethodProfileTrendViewModel> FromJson(JsonElement json)
        => json.Deserialize<List<MethodProfileTrendViewModel>>(JsonOptions.Default) ?? [];
}

#endregion
