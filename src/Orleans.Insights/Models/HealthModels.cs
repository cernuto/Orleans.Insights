using Orleans;
using System;
using System.Collections.Generic;

namespace Orleans.Insights.Models;

#region Health Ingestion Models

/// <summary>
/// Health report data sent from each silo to the InsightsGrain.
/// Contains the results of health checks performed by the silo's HealthCheckService.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record HealthReportData
{
    /// <summary>Unique identifier for the silo (e.g., "MachineName:ProcessId").</summary>
    [Id(0)] public required string SiloId { get; init; }

    /// <summary>Human-readable host name.</summary>
    [Id(1)] public required string HostName { get; init; }

    /// <summary>When this health report was generated.</summary>
    [Id(2)] public required DateTime Timestamp { get; init; }

    /// <summary>Individual health check results from this silo.</summary>
    [Id(3)] public required List<HealthCheckData> Checks { get; init; }
}

/// <summary>
/// Individual health check result within a health report.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record HealthCheckData
{
    /// <summary>Name of the health check (e.g., "Orleans", "Database", "Redis").</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>Status: "Healthy", "Degraded", or "Unhealthy".</summary>
    [Id(1)] public required string Status { get; init; }

    /// <summary>Optional description of the health check result.</summary>
    [Id(2)] public string? Description { get; init; }

    /// <summary>Optional tag for categorization (e.g., "Infra", "Cache", "DB").</summary>
    [Id(3)] public string? Tag { get; init; }

    /// <summary>Optional duration in milliseconds for the health check.</summary>
    [Id(4)] public double? DurationMs { get; init; }
}

#endregion

#region Health Query Models

/// <summary>
/// Aggregated health report for a single silo, returned by health queries.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record SiloHealthReport
{
    /// <summary>Unique identifier for the silo.</summary>
    [Id(0)] public required string SiloId { get; init; }

    /// <summary>Human-readable host name.</summary>
    [Id(1)] public required string HostName { get; init; }

    /// <summary>When this silo last reported health data.</summary>
    [Id(2)] public required DateTime LastReported { get; init; }

    /// <summary>Overall status: "Healthy", "Degraded", or "Unhealthy".</summary>
    [Id(3)] public required string OverallStatus { get; init; }

    /// <summary>Individual health check results.</summary>
    [Id(4)] public required List<HealthCheckData> Checks { get; init; }

    /// <summary>
    /// Computes the overall status from individual checks.
    /// Any Unhealthy → Unhealthy, any Degraded → Degraded, else Healthy.
    /// </summary>
    public static string ComputeOverallStatus(IEnumerable<HealthCheckData> checks)
    {
        var hasUnhealthy = false;
        var hasDegraded = false;

        foreach (var check in checks)
        {
            if (check.Status == "Unhealthy") hasUnhealthy = true;
            else if (check.Status == "Degraded") hasDegraded = true;
        }

        return hasUnhealthy ? "Unhealthy" : hasDegraded ? "Degraded" : "Healthy";
    }
}

#endregion
