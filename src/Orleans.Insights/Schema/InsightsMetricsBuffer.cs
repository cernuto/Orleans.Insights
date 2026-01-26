using Orleans.Insights.Database;
using Orleans.Insights.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Orleans.Insights.Schema;

/// <summary>
/// Manages batch buffering and flushing for Insights metrics using a Channel-based
/// producer-consumer pattern with a persistent consumer thread.
/// </summary>
/// <remarks>
/// <para>
/// <b>Design Goals:</b>
/// <list type="bullet">
/// <item>Ingestion is completely non-blocking for the grain (fire-and-forget)</item>
/// <item>Single persistent consumer thread eliminates race conditions with SingleReader channel</item>
/// <item>Consumer uses WaitToReadAsync for efficient signaling (releases thread when idle)</item>
/// <item>Backpressure via bounded channel with DropOldest (graceful degradation)</item>
/// <item>Dedicated LongRunning thread avoids Orleans thread pool starvation</item>
/// </list>
/// </para>
/// <para>
/// <b>Thread Safety:</b>
/// Producer methods (Buffer*) can be called from any context - they only do a TryWrite
/// to the channel which is lock-free. A single persistent consumer thread has exclusive
/// read access to the channel (SingleReader=true), eliminating race conditions.
/// </para>
/// <para>
/// <b>Span Safety:</b>
/// Span-based iterations (CollectionsMarshal.AsSpan) are safe because they only occur
/// within the single-threaded consumer context. The batch list is local to the consumer
/// and never escapes that context.
/// </para>
/// <para>
/// <b>Optimization:</b> Uses ObjectPool for batch lists to reduce GC pressure.
/// </para>
/// </remarks>
internal sealed class InsightsMetricsBuffer : IAsyncDisposable
{
    private readonly Channel<MetricRecord> _channel;
    private readonly ILogger _logger;
    private readonly InsightsOptions _settings;
    private readonly InsightsDatabase _database;
    private readonly CancellationTokenSource _cts;
    private readonly Task _consumerTask;
    private readonly ObjectPool<List<MetricRecord>> _batchPool;

    private volatile bool _disposed;
    private long _totalWritten;
    private long _totalDropped;
    private long _totalFlushed;

    public InsightsMetricsBuffer(ILogger logger, InsightsOptions settings, InsightsDatabase database)
    {
        _logger = logger;
        _settings = settings;
        _database = database;
        _cts = new CancellationTokenSource();

        // Object pool for batch lists to avoid repeated allocations
        _batchPool = new DefaultObjectPool<List<MetricRecord>>(
            new BatchListPoolPolicy(settings.BatchFlushThreshold),
            maximumRetained: 4);

        // Bounded channel with DropOldest provides backpressure without blocking producers
        // SingleReader=true enables lock-free optimizations in the channel
        _channel = Channel.CreateBounded<MetricRecord>(new BoundedChannelOptions(settings.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false // Multiple silos may send concurrently
        });

        // Start a single persistent consumer on a dedicated thread (LongRunning)
        // This avoids Orleans thread pool starvation and eliminates consumer state race conditions
        _consumerTask = Task.Factory.StartNew(
            () => ConsumerLoopAsync(_cts.Token),
            _cts.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();
    }

    /// <summary>
    /// ObjectPool policy for batch lists - avoids repeated allocations.
    /// </summary>
    private sealed class BatchListPoolPolicy(int capacity) : PooledObjectPolicy<List<MetricRecord>>
    {
        public override List<MetricRecord> Create() => new(capacity);

        public override bool Return(List<MetricRecord> obj)
        {
            obj.Clear();
            return true;
        }
    }

    /// <summary>
    /// Gets the approximate count of pending records in the channel.
    /// </summary>
    public int PendingCount => _channel.Reader.Count;

    /// <summary>
    /// Gets total records written to the channel.
    /// </summary>
    public long TotalWritten => Volatile.Read(ref _totalWritten);

    /// <summary>
    /// Gets total records dropped due to backpressure.
    /// </summary>
    public long TotalDropped => Volatile.Read(ref _totalDropped);

    /// <summary>
    /// Gets total records flushed to database.
    /// </summary>
    public long TotalFlushed => Volatile.Read(ref _totalFlushed);

    /// <summary>
    /// Buffers metrics from a silo report. Completely non-blocking.
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

            // Always write activation records (even 0) to overwrite stale data
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
        // Consumer is always running - no need to start it
    }

    /// <summary>
    /// Buffers method profile data. Completely non-blocking.
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
    /// Non-blocking write to channel. Returns immediately whether successful or not.
    /// </summary>
    private void TryWrite(MetricRecord record)
    {
        if (_channel.Writer.TryWrite(record))
        {
            Interlocked.Increment(ref _totalWritten);
        }
        else
        {
            Interlocked.Increment(ref _totalDropped);
        }
    }

    /// <summary>
    /// Persistent consumer loop that runs on a dedicated thread.
    /// Uses WaitToReadAsync for efficient signaling - the thread is released when idle
    /// and resumed when items arrive.
    /// Uses ObjectPool to avoid repeated batch list allocations.
    /// </summary>
    private async Task ConsumerLoopAsync(CancellationToken cancellationToken)
    {
        // Get batch from pool - reused across iterations
        var batch = _batchPool.Get();

        _logger.LogDebug("Metrics consumer started on dedicated thread");

        try
        {
            // Wait for items to arrive - this efficiently releases the thread when idle
            while (await _channel.Reader.WaitToReadAsync(cancellationToken))
            {
                batch.Clear();

                // Drain available items up to batch threshold
                while (batch.Count < _settings.BatchFlushThreshold &&
                       _channel.Reader.TryRead(out var record))
                {
                    batch.Add(record);
                }

                if (batch.Count > 0)
                {
                    FlushBatch(batch);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown - drain remaining items
            _logger.LogDebug("Metrics consumer shutdown requested, draining remaining items");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Metrics consumer encountered an error");
        }
        finally
        {
            // Drain any remaining items on shutdown
            batch.Clear();
            while (_channel.Reader.TryRead(out var record))
            {
                batch.Add(record);
                if (batch.Count >= _settings.BatchFlushThreshold)
                {
                    FlushBatch(batch);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                FlushBatch(batch);
            }

            // Return batch to pool
            _batchPool.Return(batch);

            _logger.LogDebug("Metrics consumer stopped (flushed: {TotalFlushed}, dropped: {TotalDropped})",
                TotalFlushed, TotalDropped);
        }
    }

    /// <summary>
    /// Flushes a batch to DuckDB. Runs on consumer thread, not grain thread.
    /// Span-based iteration is safe here because the batch is local to this thread.
    /// </summary>
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

            Interlocked.Add(ref _totalFlushed, batch.Count);

            sw.Stop();
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Flushed {Count} records in {ElapsedMs}ms (pending: {Pending})",
                    batch.Count, sw.ElapsedMilliseconds, _channel.Reader.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush {Count} metric records", batch.Count);
        }
    }

    private static PartitionedRecords PartitionRecords(List<MetricRecord> batch)
    {
        var count = batch.Count;
        // Pre-allocate based on typical distribution:
        // - MethodProfile records dominate (typically 60-70% of batch)
        // - Grain/GrainTypeActivation records are next (15-20% each)
        // - Cluster metrics are rare (1 per silo per report)
        // - Method metrics (legacy) are similar to grain metrics
        var cluster = new List<ClusterMetricRecord>(Math.Max(4, count / 100));
        var grain = new List<GrainMetricRecord>(count / 5);
        var grainTypeActivation = new List<GrainTypeActivationRecord>(count / 5);
        var method = new List<MethodMetricRecord>(count / 5);
        var methodProfile = new List<MethodProfileRecord>(count * 2 / 3);

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

    #region DuckDB Appender Methods (run on consumer thread only)

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

    #endregion

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Signal the consumer to stop and complete the channel
        _channel.Writer.Complete();
        await _cts.CancelAsync();

        // Wait for consumer to finish with timeout
        try
        {
            await _consumerTask.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Metrics consumer did not stop within timeout, {Pending} records may be lost",
                _channel.Reader.Count);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }

        _cts.Dispose();

        _logger.LogInformation("InsightsMetricsBuffer disposed (written: {Written}, flushed: {Flushed}, dropped: {Dropped})",
            TotalWritten, TotalFlushed, TotalDropped);
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
