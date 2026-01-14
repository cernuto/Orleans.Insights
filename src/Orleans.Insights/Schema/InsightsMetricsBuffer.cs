using Orleans.Insights.Database;
using Orleans.Insights.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Orleans.Insights.Schema;

/// <summary>
/// Manages batch buffering and flushing for Insights metrics.
/// Implements the single responsibility of buffering metrics before high-throughput
/// batch writes to DuckDB using the Appender API.
/// </summary>
/// <remarks>
/// <para>
/// Optimized for single-threaded Orleans grain execution - no locking required.
/// Orleans guarantees that grain methods are never executed concurrently.
/// </para>
/// <para>
/// Uses CollectionsMarshal.AsSpan for zero-copy iteration during flush operations.
/// Buffer lists are pre-sized based on typical batch sizes to minimize reallocations.
/// </para>
/// </remarks>
internal sealed class InsightsMetricsBuffer
{
    private readonly ILogger _logger;
    private readonly InsightsOptions _settings;
    private readonly Stopwatch _flushStopwatch = new();

    // No lock needed - Orleans grains are single-threaded
    private readonly List<ClusterMetricRecord> _clusterBuffer;
    private readonly List<GrainMetricRecord> _grainBuffer;
    private readonly List<MethodMetricRecord> _methodBuffer;
    private readonly List<MethodProfileRecord> _methodProfileBuffer;
    private readonly List<GrainTypeActivationRecord> _grainTypeActivationBuffer;

    public InsightsMetricsBuffer(ILogger logger, InsightsOptions settings)
    {
        _logger = logger;
        _settings = settings;
        _flushStopwatch.Start();

        // Pre-size buffers based on expected batch sizes to reduce allocations
        var expectedBatchSize = settings.BatchFlushThreshold;
        _clusterBuffer = new List<ClusterMetricRecord>(Math.Min(expectedBatchSize, 100));
        _grainBuffer = new List<GrainMetricRecord>(Math.Min(expectedBatchSize, 500));
        _methodBuffer = new List<MethodMetricRecord>(Math.Min(expectedBatchSize, 1000));
        // Method profile buffer - expects high volume from all silos every second
        _methodProfileBuffer = new List<MethodProfileRecord>(Math.Min(expectedBatchSize, 2000));
        // Grain type activation buffer - one entry per grain type per silo per report
        _grainTypeActivationBuffer = new List<GrainTypeActivationRecord>(Math.Min(expectedBatchSize, 500));
    }

    /// <summary>
    /// Gets the total count of buffered records across all metric types.
    /// </summary>
    public int TotalCount
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _clusterBuffer.Count + _grainBuffer.Count + _methodBuffer.Count +
               _methodProfileBuffer.Count + _grainTypeActivationBuffer.Count;
    }

    /// <summary>
    /// Buffers metrics from a silo report.
    /// No locking - Orleans grains are single-threaded.
    /// </summary>
    public void BufferSiloMetrics(SiloMetricsReport metrics)
    {
        var timestamp = metrics.Timestamp;
        var siloId = metrics.SiloId;
        var hostName = metrics.HostName;
        var cm = metrics.ClusterMetrics;

        // Buffer cluster metrics
        _clusterBuffer.Add(new ClusterMetricRecord(
            timestamp, siloId, hostName,
            cm.TotalActivations, cm.ConnectedClients,
            cm.MessagesSent, cm.MessagesReceived, cm.MessagesDropped,
            cm.CpuUsagePercent, cm.MemoryUsageMb, cm.AvailableMemoryMb,
            cm.AverageRequestLatencyMs, cm.TotalRequests,
            // Catalog metrics
            cm.ActivationWorkingSet, cm.ActivationsCreated, cm.ActivationsDestroyed,
            cm.ActivationsFailedToActivate, cm.ActivationCollections, cm.ActivationShutdowns,
            cm.ActivationNonExistent, cm.ConcurrentRegistrationAttempts,
            // Miscellaneous grain metrics
            cm.GrainCount, cm.SystemTargets));

        // Buffer grain type metrics
        foreach (var (grainType, gm) in metrics.GrainTypeMetrics)
        {
            _grainBuffer.Add(new GrainMetricRecord(
                timestamp, siloId, grainType,
                gm.TotalRequests, gm.FailedRequests,
                gm.AverageLatencyMs, gm.MinLatencyMs, gm.MaxLatencyMs,
                gm.RequestsPerSecond));
        }

        // Buffer method metrics
        foreach (var (_, mm) in metrics.MethodMetrics)
        {
            _methodBuffer.Add(new MethodMetricRecord(
                timestamp, siloId, mm.GrainType, mm.MethodName,
                mm.TotalRequests, mm.FailedRequests,
                mm.AverageLatencyMs, mm.RequestsPerSecond));
        }

        // Buffer grain type activations from IManagementGrain data
        // IMPORTANT: Always write activation records (even 0) to overwrite stale data
        // This ensures that when a grain moves to another silo, the old silo's stale
        // activation count is replaced with 0 rather than persisting forever.
        foreach (var (grainType, gm) in metrics.GrainTypeMetrics)
        {
            _grainTypeActivationBuffer.Add(new GrainTypeActivationRecord(
                timestamp, siloId, grainType, gm.Activations));
        }
    }

    /// <summary>
    /// Buffers method profile data from GrainMethodProfiler.
    /// Stores raw totals (count + elapsedMs) for accurate average calculation.
    /// No locking - Orleans grains are single-threaded.
    /// </summary>
    public void BufferMethodProfile(MethodProfileReport report)
    {
        var timestamp = report.Timestamp;
        var siloId = report.SiloId;
        var hostName = report.HostName;

        foreach (var entry in report.Entries)
        {
            _methodProfileBuffer.Add(new MethodProfileRecord(
                timestamp,
                siloId,
                hostName,
                entry.GrainType,
                entry.Method,
                entry.Count,
                entry.TotalElapsedMs,
                entry.ExceptionCount));
        }
    }

    /// <summary>
    /// Checks if a flush is needed based on count or time thresholds.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ShouldFlush() =>
        TotalCount >= _settings.BatchFlushThreshold ||
        _flushStopwatch.Elapsed >= _settings.BatchFlushInterval;

    /// <summary>
    /// Drains all buffers and flushes to the database using batch Appender.
    /// Returns true if any records were flushed.
    /// No locking - Orleans grains are single-threaded.
    /// Uses CollectionsMarshal.AsSpan for zero-copy iteration.
    /// </summary>
    public bool FlushTo(InsightsDatabase database)
    {
        if (_clusterBuffer.Count == 0 && _grainBuffer.Count == 0 &&
            _methodBuffer.Count == 0 && _methodProfileBuffer.Count == 0 &&
            _grainTypeActivationBuffer.Count == 0)
        {
            _flushStopwatch.Restart();
            return false;
        }

        // Capture counts before clearing (for logging)
        var clusterCount = _clusterBuffer.Count;
        var grainCount = _grainBuffer.Count;
        var methodCount = _methodBuffer.Count;
        var methodProfileCount = _methodProfileBuffer.Count;
        var grainTypeActivationCount = _grainTypeActivationBuffer.Count;
        var totalRecords = clusterCount + grainCount + methodCount + methodProfileCount + grainTypeActivationCount;

        var sw = Stopwatch.StartNew();

        try
        {
            // Flush directly from buffers using span-based iteration
            FlushBuffersDirect(database);

            // Clear buffers after successful flush
            _clusterBuffer.Clear();
            _grainBuffer.Clear();
            _methodBuffer.Clear();
            _methodProfileBuffer.Clear();
            _grainTypeActivationBuffer.Clear();
            _flushStopwatch.Restart();

            sw.Stop();
            _logger.LogDebug(
                "Flushed {TotalRecords} records in {ElapsedMs}ms (cluster={Cluster}, grain={Grain}, method={Method}, methodProfile={MethodProfile}, grainTypeActivation={GrainTypeActivation})",
                totalRecords, sw.ElapsedMilliseconds,
                clusterCount, grainCount, methodCount, methodProfileCount, grainTypeActivationCount);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush metric buffers");
            // Buffers are not cleared on failure - data is preserved for retry
            return false;
        }
    }

    /// <summary>
    /// Flushes buffers directly using CollectionsMarshal.AsSpan for zero-copy iteration.
    /// Avoids creating snapshot copies - more efficient for single-threaded execution.
    /// </summary>
    private void FlushBuffersDirect(InsightsDatabase database)
    {
        var connection = database.WriteConnection;

        // Flush cluster metrics using Appender with span-based iteration
        if (_clusterBuffer.Count > 0)
        {
            using var appender = connection.CreateAppender("cluster_metrics");
            var span = CollectionsMarshal.AsSpan(_clusterBuffer);
            foreach (ref readonly var r in span)
            {
                appender.CreateRow()
                    .AppendValue(r.Timestamp)
                    .AppendValue(r.SiloId)
                    .AppendValue(r.HostName)
                    .AppendValue(r.TotalActivations)
                    .AppendValue(r.ConnectedClients)
                    .AppendValue(r.MessagesSent)
                    .AppendValue(r.MessagesReceived)
                    .AppendValue(r.MessagesDropped)
                    .AppendValue(r.CpuUsagePercent)
                    .AppendValue(r.MemoryUsageMb)
                    .AppendValue(r.AvailableMemoryMb)
                    .AppendValue(r.AvgLatencyMs)
                    .AppendValue(r.TotalRequests)
                    // Catalog metrics
                    .AppendValue(r.ActivationWorkingSet)
                    .AppendValue(r.ActivationsCreated)
                    .AppendValue(r.ActivationsDestroyed)
                    .AppendValue(r.ActivationsFailedToActivate)
                    .AppendValue(r.ActivationCollections)
                    .AppendValue(r.ActivationShutdowns)
                    .AppendValue(r.ActivationNonExistent)
                    .AppendValue(r.ConcurrentRegistrationAttempts)
                    // Miscellaneous grain metrics
                    .AppendValue(r.GrainCount)
                    .AppendValue(r.SystemTargets)
                    .EndRow();
            }
        }

        // Flush grain metrics using Appender with span-based iteration
        if (_grainBuffer.Count > 0)
        {
            using var appender = connection.CreateAppender("grain_metrics");
            var span = CollectionsMarshal.AsSpan(_grainBuffer);
            foreach (ref readonly var r in span)
            {
                appender.CreateRow()
                    .AppendValue(r.Timestamp)
                    .AppendValue(r.SiloId)
                    .AppendValue(r.GrainType)
                    .AppendValue(r.TotalRequests)
                    .AppendValue(r.FailedRequests)
                    .AppendValue(r.AvgLatencyMs)
                    .AppendValue(r.MinLatencyMs)
                    .AppendValue(r.MaxLatencyMs)
                    .AppendValue(r.RequestsPerSecond)
                    .EndRow();
            }
        }

        // Flush method metrics using Appender with span-based iteration
        if (_methodBuffer.Count > 0)
        {
            using var appender = connection.CreateAppender("method_metrics");
            var span = CollectionsMarshal.AsSpan(_methodBuffer);
            foreach (ref readonly var r in span)
            {
                appender.CreateRow()
                    .AppendValue(r.Timestamp)
                    .AppendValue(r.SiloId)
                    .AppendValue(r.GrainType)
                    .AppendValue(r.MethodName)
                    .AppendValue(r.TotalRequests)
                    .AppendValue(r.FailedRequests)
                    .AppendValue(r.AvgLatencyMs)
                    .AppendValue(r.RequestsPerSecond)
                    .EndRow();
            }
        }

        // Flush method profile metrics using Appender with span-based iteration
        // This stores raw totals for accurate average calculation: avg = total_elapsed_ms / call_count
        if (_methodProfileBuffer.Count > 0)
        {
            using var appender = connection.CreateAppender("method_profile");
            var span = CollectionsMarshal.AsSpan(_methodProfileBuffer);
            foreach (ref readonly var r in span)
            {
                appender.CreateRow()
                    .AppendValue(r.Timestamp)
                    .AppendValue(r.SiloId)
                    .AppendValue(r.HostName)
                    .AppendValue(r.GrainType)
                    .AppendValue(r.MethodName)
                    .AppendValue(r.CallCount)
                    .AppendValue(r.TotalElapsedMs)
                    .AppendValue(r.ExceptionCount)
                    .EndRow();
            }
        }

        // Flush grain type activation metrics using Appender with span-based iteration
        // This stores per-grain-type activation counts from the orleans-grains metric
        if (_grainTypeActivationBuffer.Count > 0)
        {
            using var appender = connection.CreateAppender("grain_type_activations");
            var span = CollectionsMarshal.AsSpan(_grainTypeActivationBuffer);
            foreach (ref readonly var r in span)
            {
                appender.CreateRow()
                    .AppendValue(r.Timestamp)
                    .AppendValue(r.SiloId)
                    .AppendValue(r.GrainType)
                    .AppendValue(r.Activations)
                    .EndRow();
            }
        }
    }
}

#region Buffer Record Types

internal readonly record struct ClusterMetricRecord(
    DateTime Timestamp,
    string SiloId,
    string HostName,
    long TotalActivations,
    int ConnectedClients,
    long MessagesSent,
    long MessagesReceived,
    long MessagesDropped,
    double CpuUsagePercent,
    long MemoryUsageMb,
    long AvailableMemoryMb,
    double AvgLatencyMs,
    long TotalRequests,
    // Catalog metrics (activation lifecycle)
    long ActivationWorkingSet,
    long ActivationsCreated,
    long ActivationsDestroyed,
    long ActivationsFailedToActivate,
    long ActivationCollections,
    long ActivationShutdowns,
    long ActivationNonExistent,
    long ConcurrentRegistrationAttempts,
    // Miscellaneous grain metrics
    long GrainCount,
    long SystemTargets);

internal readonly record struct GrainMetricRecord(
    DateTime Timestamp,
    string SiloId,
    string GrainType,
    long TotalRequests,
    long FailedRequests,
    double AvgLatencyMs,
    double MinLatencyMs,
    double MaxLatencyMs,
    double RequestsPerSecond);

internal readonly record struct MethodMetricRecord(
    DateTime Timestamp,
    string SiloId,
    string GrainType,
    string MethodName,
    long TotalRequests,
    long FailedRequests,
    double AvgLatencyMs,
    double RequestsPerSecond);

/// <summary>
/// Buffer record for method profile data (OrleansDashboard-style accurate totals).
/// Stores raw totals enabling accurate average calculation: avg = TotalElapsedMs / CallCount.
/// </summary>
internal readonly record struct MethodProfileRecord(
    DateTime Timestamp,
    string SiloId,
    string HostName,
    string GrainType,
    string MethodName,
    long CallCount,
    double TotalElapsedMs,
    long ExceptionCount);

/// <summary>
/// Buffer record for per-grain-type activation counts from orleans-grains metric.
/// Captures the grain.type tag value from OTel metrics for per-silo grain type tracking.
/// </summary>
internal readonly record struct GrainTypeActivationRecord(
    DateTime Timestamp,
    string SiloId,
    string GrainType,
    long Activations);

#endregion
