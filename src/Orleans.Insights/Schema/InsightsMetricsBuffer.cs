using Orleans.Insights.Database;
using Orleans.Insights.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Orleans.Insights.Schema;

/// <summary>
/// Manages batch buffering and flushing for Insights metrics using a Channel-based
/// producer-consumer pattern with lazy consumer startup.
/// </summary>
/// <remarks>
/// <para>
/// The consumer task starts on-demand when items are written and automatically exits
/// after an idle timeout. This avoids permanent background threads when there's no work.
/// </para>
/// <para>
/// Uses a bounded channel with backpressure - when full, oldest items are dropped
/// to prevent unbounded memory growth under heavy load.
/// </para>
/// <para>
/// Thread safety: Producer methods (Buffer*) are called from the Orleans grain's single-threaded
/// context. The consumer runs on a thread pool thread and has exclusive read access to the channel.
/// </para>
/// </remarks>
internal sealed class InsightsMetricsBuffer : IAsyncDisposable
{
    private readonly Channel<MetricRecord> _channel;
    private readonly ILogger _logger;
    private readonly InsightsOptions _settings;
    private readonly InsightsDatabase _database;

    private int _consumerRunning;
    private volatile bool _disposed;

    public InsightsMetricsBuffer(ILogger logger, InsightsOptions settings, InsightsDatabase database)
    {
        _logger = logger;
        _settings = settings;
        _database = database;

        _channel = Channel.CreateBounded<MetricRecord>(new BoundedChannelOptions(settings.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });
    }

    /// <summary>
    /// Gets the approximate count of pending records in the channel.
    /// </summary>
    public int TotalCount => _channel.Reader.Count;

    /// <summary>
    /// Buffers metrics from a silo report.
    /// </summary>
    public void BufferSiloMetrics(SiloMetricsReport metrics)
    {
        if (_disposed) return;

        var timestamp = metrics.Timestamp;
        var siloId = metrics.SiloId;
        var hostName = metrics.HostName;
        var cm = metrics.ClusterMetrics;

        // Buffer cluster metrics
        TryWrite(new ClusterMetricRecord(
            timestamp, siloId, hostName,
            cm.TotalActivations, cm.ConnectedClients,
            cm.MessagesSent, cm.MessagesReceived, cm.MessagesDropped,
            cm.CpuUsagePercent, cm.MemoryUsageMb, cm.AvailableMemoryMb,
            cm.AverageRequestLatencyMs, cm.TotalRequests,
            cm.ActivationWorkingSet, cm.ActivationsCreated, cm.ActivationsDestroyed,
            cm.ActivationsFailedToActivate, cm.ActivationCollections, cm.ActivationShutdowns,
            cm.ActivationNonExistent, cm.ConcurrentRegistrationAttempts,
            cm.GrainCount, cm.SystemTargets));

        // Buffer grain type metrics and activations
        foreach (var (grainType, gm) in metrics.GrainTypeMetrics)
        {
            TryWrite(new GrainMetricRecord(
                timestamp, siloId, grainType,
                gm.TotalRequests, gm.FailedRequests,
                gm.AverageLatencyMs, gm.MinLatencyMs, gm.MaxLatencyMs,
                gm.RequestsPerSecond));

            // IMPORTANT: Always write activation records (even 0) to overwrite stale data
            // This ensures that when a grain moves to another silo, the old silo's stale
            // activation count is replaced with 0 rather than persisting forever.
            TryWrite(new GrainTypeActivationRecord(
                timestamp, siloId, grainType, gm.Activations));
        }

        // Buffer method metrics
        foreach (var (_, mm) in metrics.MethodMetrics)
        {
            TryWrite(new MethodMetricRecord(
                timestamp, siloId, mm.GrainType, mm.MethodName,
                mm.TotalRequests, mm.FailedRequests,
                mm.AverageLatencyMs, mm.RequestsPerSecond));
        }
    }

    /// <summary>
    /// Buffers method profile data from GrainMethodProfiler.
    /// Stores raw totals (count + elapsedMs) for accurate average calculation.
    /// </summary>
    public void BufferMethodProfile(MethodProfileReport report)
    {
        if (_disposed) return;

        var timestamp = report.Timestamp;
        var siloId = report.SiloId;
        var hostName = report.HostName;

        foreach (var entry in report.Entries)
        {
            TryWrite(new MethodProfileRecord(
                timestamp, siloId, hostName,
                entry.GrainType, entry.Method,
                entry.Count, entry.TotalElapsedMs, entry.ExceptionCount));
        }
    }

    /// <summary>
    /// Ensures the consumer is running. Used during maintenance to flush pending items.
    /// </summary>
    public void EnsureProcessing()
    {
        if (!_disposed && _channel.Reader.TryPeek(out _))
        {
            EnsureConsumerRunning();
        }
    }

    private void TryWrite(MetricRecord record)
    {
        if (!_channel.Writer.TryWrite(record))
        {
            _logger.LogDebug("Metrics channel full, dropped record");
            return;
        }

        EnsureConsumerRunning();
    }

    private void EnsureConsumerRunning()
    {
        if (Interlocked.CompareExchange(ref _consumerRunning, 1, 0) == 0)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await ConsumeAsync();
                }
                finally
                {
                    Interlocked.Exchange(ref _consumerRunning, 0);

                    // Check if items arrived while we were exiting
                    if (_channel.Reader.TryPeek(out _) && !_disposed)
                    {
                        EnsureConsumerRunning();
                    }
                }
            });
        }
    }

    private async Task ConsumeAsync()
    {
        var batch = new List<MetricRecord>(_settings.BatchFlushThreshold);
        var idleTimeout = _settings.BatchFlushInterval;

        while (!_disposed)
        {
            batch.Clear();

            // Drain all available items up to batch size
            while (batch.Count < _settings.BatchFlushThreshold &&
                   _channel.Reader.TryRead(out var record))
            {
                batch.Add(record);
            }

            if (batch.Count > 0)
            {
                FlushBatch(batch);
            }
            else
            {
                // Channel empty - wait briefly for more items or exit
                using var cts = new CancellationTokenSource(idleTimeout);
                try
                {
                    var record = await _channel.Reader.ReadAsync(cts.Token);
                    batch.Add(record);

                    // Got one - drain any others that arrived
                    while (batch.Count < _settings.BatchFlushThreshold &&
                           _channel.Reader.TryRead(out var additional))
                    {
                        batch.Add(additional);
                    }

                    FlushBatch(batch);
                }
                catch (OperationCanceledException)
                {
                    // Timeout with no items - exit consumer
                    _logger.LogDebug("Metrics consumer exiting after idle timeout");
                    return;
                }
            }
        }
    }

    private void FlushBatch(List<MetricRecord> batch)
    {
        var sw = Stopwatch.StartNew();

        try
        {
            // Partition records by type in a single pass
            var partitioned = PartitionRecords(batch);
            var connection = _database.WriteConnection;

            FlushClusterMetrics(connection, partitioned.Cluster);
            FlushGrainMetrics(connection, partitioned.Grain);
            FlushGrainTypeActivations(connection, partitioned.GrainTypeActivation);
            FlushMethodMetrics(connection, partitioned.Method);
            FlushMethodProfileMetrics(connection, partitioned.MethodProfile);

            sw.Stop();
            _logger.LogDebug("Flushed {Count} metric records in {ElapsedMs}ms", batch.Count, sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush {Count} metric records", batch.Count);
        }
    }

    private static PartitionedRecords PartitionRecords(List<MetricRecord> batch)
    {
        var cluster = new List<ClusterMetricRecord>();
        var grain = new List<GrainMetricRecord>();
        var grainTypeActivation = new List<GrainTypeActivationRecord>();
        var method = new List<MethodMetricRecord>();
        var methodProfile = new List<MethodProfileRecord>();

        foreach (var record in batch)
        {
            switch (record)
            {
                case ClusterMetricRecord r: cluster.Add(r); break;
                case GrainMetricRecord r: grain.Add(r); break;
                case GrainTypeActivationRecord r: grainTypeActivation.Add(r); break;
                case MethodMetricRecord r: method.Add(r); break;
                case MethodProfileRecord r: methodProfile.Add(r); break;
            }
        }

        return new PartitionedRecords(cluster, grain, grainTypeActivation, method, methodProfile);
    }

    private readonly record struct PartitionedRecords(
        List<ClusterMetricRecord> Cluster,
        List<GrainMetricRecord> Grain,
        List<GrainTypeActivationRecord> GrainTypeActivation,
        List<MethodMetricRecord> Method,
        List<MethodProfileRecord> MethodProfile);

    private static void FlushClusterMetrics(DuckDB.NET.Data.DuckDBConnection connection, List<ClusterMetricRecord> records)
    {
        if (records.Count == 0) return;

        using var appender = connection.CreateAppender("cluster_metrics");
        var span = CollectionsMarshal.AsSpan(records);

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
                .AppendValue(r.ActivationWorkingSet)
                .AppendValue(r.ActivationsCreated)
                .AppendValue(r.ActivationsDestroyed)
                .AppendValue(r.ActivationsFailedToActivate)
                .AppendValue(r.ActivationCollections)
                .AppendValue(r.ActivationShutdowns)
                .AppendValue(r.ActivationNonExistent)
                .AppendValue(r.ConcurrentRegistrationAttempts)
                .AppendValue(r.GrainCount)
                .AppendValue(r.SystemTargets)
                .EndRow();
        }
    }

    private static void FlushGrainMetrics(DuckDB.NET.Data.DuckDBConnection connection, List<GrainMetricRecord> records)
    {
        if (records.Count == 0) return;

        using var appender = connection.CreateAppender("grain_metrics");
        var span = CollectionsMarshal.AsSpan(records);

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

    private static void FlushGrainTypeActivations(DuckDB.NET.Data.DuckDBConnection connection, List<GrainTypeActivationRecord> records)
    {
        if (records.Count == 0) return;

        using var appender = connection.CreateAppender("grain_type_activations");
        var span = CollectionsMarshal.AsSpan(records);

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

    private static void FlushMethodMetrics(DuckDB.NET.Data.DuckDBConnection connection, List<MethodMetricRecord> records)
    {
        if (records.Count == 0) return;

        using var appender = connection.CreateAppender("method_metrics");
        var span = CollectionsMarshal.AsSpan(records);

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

    private static void FlushMethodProfileMetrics(DuckDB.NET.Data.DuckDBConnection connection, List<MethodProfileRecord> records)
    {
        if (records.Count == 0) return;

        using var appender = connection.CreateAppender("method_profile");
        var span = CollectionsMarshal.AsSpan(records);

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

    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        _channel.Writer.Complete();

        // Wait for consumer to finish processing remaining items
        var spinWait = new SpinWait();
        while (Volatile.Read(ref _consumerRunning) == 1)
        {
            spinWait.SpinOnce();
            if (spinWait.NextSpinWillYield)
            {
                await Task.Delay(10);
            }
        }
    }
}

#region Buffer Record Types

internal abstract record MetricRecord;

internal sealed record ClusterMetricRecord(
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
    long ActivationWorkingSet,
    long ActivationsCreated,
    long ActivationsDestroyed,
    long ActivationsFailedToActivate,
    long ActivationCollections,
    long ActivationShutdowns,
    long ActivationNonExistent,
    long ConcurrentRegistrationAttempts,
    long GrainCount,
    long SystemTargets) : MetricRecord;

internal sealed record GrainMetricRecord(
    DateTime Timestamp,
    string SiloId,
    string GrainType,
    long TotalRequests,
    long FailedRequests,
    double AvgLatencyMs,
    double MinLatencyMs,
    double MaxLatencyMs,
    double RequestsPerSecond) : MetricRecord;

internal sealed record GrainTypeActivationRecord(
    DateTime Timestamp,
    string SiloId,
    string GrainType,
    long Activations) : MetricRecord;

internal sealed record MethodMetricRecord(
    DateTime Timestamp,
    string SiloId,
    string GrainType,
    string MethodName,
    long TotalRequests,
    long FailedRequests,
    double AvgLatencyMs,
    double RequestsPerSecond) : MetricRecord;

internal sealed record MethodProfileRecord(
    DateTime Timestamp,
    string SiloId,
    string HostName,
    string GrainType,
    string MethodName,
    long CallCount,
    double TotalElapsedMs,
    long ExceptionCount) : MetricRecord;

#endregion
