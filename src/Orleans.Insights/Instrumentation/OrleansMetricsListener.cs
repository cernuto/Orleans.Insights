using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Insights.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Interface for Orleans metrics listener following SOLID DIP.
/// Enables dependency injection and testing without concrete implementation.
/// </summary>
public interface IOrleansMetricsListener
{
    /// <summary>
    /// Gets summarized Orleans cluster metrics for dashboard display.
    /// </summary>
    OrleansClusterMetricsSnapshot GetClusterMetrics();

    /// <summary>
    /// Gets per-grain-type metrics collected from Orleans instrumentation.
    /// </summary>
    IReadOnlyDictionary<string, GrainTypeMetrics> GetGrainTypeMetrics();

    /// <summary>
    /// Gets per-method metrics for all grain methods.
    /// </summary>
    IReadOnlyDictionary<string, GrainMethodMetrics> GetGrainMethodMetrics();
}

/// <summary>
/// Listens to Orleans' built-in OpenTelemetry meters and captures metrics for dashboard display.
///
/// Key Features:
/// - MeterListener subscribes to Orleans meters to capture real-time metrics
/// - Delta-based latency calculation (not cumulative averages) for accurate display
/// - Exponential smoothing prevents sudden jumps in metrics display
/// - Process-level CPU/memory tracking (Orleans doesn't expose CPU metrics)
/// - Thread-safe with ConcurrentDictionary and Lock for complex state updates
/// - Span-based string building for efficient metric key construction
/// - LRU eviction to prevent unbounded memory growth
///
/// IMPORTANT: This service implements IHostedService to ensure it starts BEFORE Orleans.
/// MeterListener only captures instruments published AFTER it starts, so timing is critical.
/// </summary>
public sealed class OrleansMetricsListener : IOrleansMetricsListener, IHostedService, IDisposable
{
    private readonly ILogger<OrleansMetricsListener> _logger;
    private readonly InsightsOptions _options;
    private MeterListener? _meterListener;
    private Timer? _pollTimer;
    private bool _disposed;
    private int _instrumentCount;

    // Thread-safe metric storage with LRU eviction to prevent unbounded growth
    private readonly ConcurrentDictionary<string, double> _metrics = new();
    private readonly ConcurrentDictionary<string, GrainTypeMetrics> _grainTypeMetrics = new();
    private readonly ConcurrentDictionary<string, GrainMethodMetrics> _grainMethodMetrics = new();
    private readonly Stopwatch _evictionCheckStopwatch = Stopwatch.StartNew();

    // Cluster-wide latency tracking using delta calculation from cumulative counters
    // Orleans exposes latency-sum (cumulative total) and latency-count (request count)
    // We calculate: avg = (sum_delta) / (count_delta) to get true average over the interval
    private readonly Lock _clusterLatencyLock = new();
    private double _currentLatencySum;   // Current cumulative sum from OTel
    private long _currentLatencyCount;   // Current cumulative count from OTel
    private double _lastLatencySum;      // Sum at last calculation (for delta)
    private long _lastLatencyCount;      // Count at last calculation (for delta)
    private double _currentAvgLatency;

    // Process-level CPU/memory tracking (Orleans doesn't expose CPU metrics)
    private readonly Lock _processMetricsLock = new();
    private TimeSpan _prevProcessorTime;
    private readonly Stopwatch _cpuSampleStopwatch = new();
    private double _currentCpuPercent;
    private long _currentWorkingSetMb;

    // Exponential smoothing weights for display stability
    private const double LatencyOldWeight = 0.7;
    private const double LatencyNewWeight = 0.3;
    private const double MethodLatencyOldWeight = 0.9;
    private const double MethodLatencyNewWeight = 0.1;

    // LRU eviction constants
    private const int MaxMetricsEntries = 10000;
    private const double LruEvictionMultiplier = 1.2;
    private const int LruEvictionCount = 1000;
    private const int DefaultMetricsSampleLimit = 100;
    private const int DefaultMethodProfileSampleLimit = 120;

    // Orleans meter and metric names
    private const string OrleansMeterName = "Microsoft.Orleans";
    private const string OrleansMeterPrefix = "Microsoft.Orleans.";
    private const string AppRequestsLatencySum = "orleans-app-requests-latency-sum";
    private const string AppRequestsLatencyCount = "orleans-app-requests-latency-count";

    public OrleansMetricsListener(
        IOptions<InsightsOptions> options,
        ILogger<OrleansMetricsListener> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("OrleansMetricsListener starting - initializing MeterListener");

        _meterListener = new MeterListener
        {
            InstrumentPublished = OnInstrumentPublished
        };

        // Set up measurement callbacks for different value types
        _meterListener.SetMeasurementEventCallback<long>(OnMeasurement);
        _meterListener.SetMeasurementEventCallback<int>(OnMeasurement);
        _meterListener.SetMeasurementEventCallback<double>(OnMeasurement);
        _meterListener.SetMeasurementEventCallback<float>(OnMeasurement);

        // Start() triggers InstrumentPublished for all pre-existing instruments
        _meterListener.Start();

        // Observable instruments require periodic polling via RecordObservableInstruments()
        // Poll every second to capture current values from Orleans' observable instruments
        _pollTimer = new Timer(
            PollObservableInstruments,
            null,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1));

        _logger.LogInformation("Orleans metrics listener started (instruments found: {Count})", _instrumentCount);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        Dispose();
        return Task.CompletedTask;
    }

    private void PollObservableInstruments(object? state)
    {
        if (_disposed || _meterListener == null) return;

        try
        {
            // This triggers all observable instruments to emit their current values
            _meterListener.RecordObservableInstruments();

            // Update process-level CPU/memory metrics
            UpdateProcessMetrics();

            // Perform LRU eviction periodically to prevent unbounded growth
            EvictStaleEntriesIfNeeded();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error polling observable instruments");
        }
    }

    /// <summary>
    /// Updates CPU and memory metrics using Process.GetCurrentProcess().
    /// CPU percentage is calculated as delta of processor time / elapsed monotonic time.
    /// </summary>
    private void UpdateProcessMetrics()
    {
        try
        {
            using var process = Process.GetCurrentProcess();
            var currentProcessorTime = process.TotalProcessorTime;

            using (_processMetricsLock.EnterScope())
            {
                // Calculate CPU percentage from processor time delta
                if (_cpuSampleStopwatch.IsRunning)
                {
                    var elapsedTime = _cpuSampleStopwatch.Elapsed.TotalMilliseconds;
                    var cpuTimeUsed = (currentProcessorTime - _prevProcessorTime).TotalMilliseconds;

                    if (elapsedTime > 0)
                    {
                        // CPU% = (CPU time used / wall-clock time) * 100 / processor count
                        var rawCpuPercent = (cpuTimeUsed / elapsedTime) * 100.0 / Environment.ProcessorCount;

                        // Apply smoothing to avoid jitter
                        _currentCpuPercent = _currentCpuPercent > 0
                            ? (_currentCpuPercent * LatencyOldWeight) + (rawCpuPercent * LatencyNewWeight)
                            : rawCpuPercent;
                    }
                }

                _prevProcessorTime = currentProcessorTime;
                _cpuSampleStopwatch.Restart();

                // Memory metrics (in MB)
                _currentWorkingSetMb = process.WorkingSet64 / (1024 * 1024);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to update process metrics");
        }
    }

    /// <summary>
    /// Evicts stale entries from metric dictionaries using LRU policy.
    /// Prevents unbounded memory growth in long-running silos.
    /// </summary>
    private void EvictStaleEntriesIfNeeded()
    {
        // Only check every minute to avoid overhead
        if (_evictionCheckStopwatch.Elapsed.TotalMinutes < 1)
            return;

        _evictionCheckStopwatch.Restart();

        // Check if any dictionary exceeds the threshold
        var grainTypeCount = _grainTypeMetrics.Count;
        var methodCount = _grainMethodMetrics.Count;

        var needsEviction =
            grainTypeCount > MaxMetricsEntries * LruEvictionMultiplier ||
            methodCount > MaxMetricsEntries * LruEvictionMultiplier;

        if (!needsEviction)
            return;

        // Evict oldest entries from each dictionary
        if (grainTypeCount > MaxMetricsEntries)
        {
            var toRemove = _grainTypeMetrics
                .OrderBy(kv => kv.Value.LastUpdated)
                .Take(LruEvictionCount)
                .Select(kv => kv.Key)
                .ToList();

            foreach (var key in toRemove)
                _grainTypeMetrics.TryRemove(key, out _);
        }

        if (methodCount > MaxMetricsEntries)
        {
            var toRemove = _grainMethodMetrics
                .OrderBy(kv => kv.Value.LastUpdated)
                .Take(LruEvictionCount)
                .Select(kv => kv.Key)
                .ToList();

            foreach (var key in toRemove)
                _grainMethodMetrics.TryRemove(key, out _);
        }

        _logger.LogInformation("LRU eviction completed: grainTypes={GrainTypeCount}, methods={MethodCount}",
            _grainTypeMetrics.Count, _grainMethodMetrics.Count);
    }

    private void OnInstrumentPublished(Instrument instrument, MeterListener listener)
    {
        // Subscribe to Orleans built-in metrics
        if (instrument.Meter.Name == OrleansMeterName ||
            instrument.Meter.Name.StartsWith(OrleansMeterPrefix, StringComparison.OrdinalIgnoreCase))
        {
            Interlocked.Increment(ref _instrumentCount);
            _logger.LogDebug("Subscribed to Orleans instrument: {MeterName}/{InstrumentName} ({Type})",
                instrument.Meter.Name, instrument.Name, instrument.GetType().Name);
            listener.EnableMeasurementEvents(instrument);
        }
    }

    private void OnMeasurement<T>(Instrument instrument, T measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        var value = Convert.ToDouble(measurement);
        var name = instrument.Name;

        string? grainType = null;
        string? grainMethod = null;
        bool? isSuccess = null;

        // Extract tags - Orleans uses various naming conventions
        foreach (var tag in tags)
        {
            if (tag.Value == null) continue;

            var key = tag.Key;
            var tagValue = tag.Value.ToString() ?? string.Empty;

            // Check for grain type tag (Orleans patterns)
            if (key is "grain.type" or "orleans.grain.type" or "rpc.service" or "orleans.target.grain.type" or "type")
                grainType = tagValue;
            // Check for method tag
            else if (key is "grain.method" or "orleans.grain.method" or "rpc.method" or "orleans.target.grain.method")
                grainMethod = tagValue;
            else if (key is "success" or "orleans.success" or "rpc.grpc.status_code")
                isSuccess = tag.Value is bool b ? b : (tagValue == "0" || tagValue.Equals("true", StringComparison.OrdinalIgnoreCase));
        }

        // Store the raw metric value with key built from name and tags
        var metricKey = tags.Length == 0 ? name : BuildMetricKey(name, tags);
        _metrics[metricKey] = value;

        // Track cluster-wide latency using cumulative sum/count from Orleans histogram metrics
        if (name.Equals(AppRequestsLatencySum, StringComparison.OrdinalIgnoreCase))
        {
            UpdateLatencySum(value);
        }
        else if (name.Equals(AppRequestsLatencyCount, StringComparison.OrdinalIgnoreCase))
        {
            UpdateLatencyCount((long)value);
        }

        // Track per-grain-type metrics if grain type is available
        if (!string.IsNullOrEmpty(grainType))
        {
            // Normalize grain type (strip assembly suffix if present)
            var normalizedGrainType = grainType;
            var commaIdx = grainType.LastIndexOf(',');
            if (commaIdx >= 0)
                normalizedGrainType = grainType[..commaIdx];

            UpdateGrainTypeMetrics(name, normalizedGrainType, value, isSuccess);

            if (!string.IsNullOrEmpty(grainMethod))
            {
                UpdateGrainMethodMetrics(normalizedGrainType, grainMethod, name, value, isSuccess);
            }
        }
    }

    /// <summary>
    /// Builds a metric key from instrument name and tags using span-based approach for efficiency.
    /// </summary>
    private static string BuildMetricKey(string name, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        // Use Span-based building for efficiency
        Span<char> buffer = stackalloc char[256];
        var pos = 0;

        name.AsSpan().CopyTo(buffer);
        pos += name.Length;
        buffer[pos++] = '[';

        var first = true;
        foreach (var tag in tags)
        {
            if (tag.Value == null) continue;

            if (!first) buffer[pos++] = ',';
            first = false;

            var k = tag.Key;
            var v = tag.Value.ToString() ?? "";

            k.AsSpan().CopyTo(buffer[pos..]);
            pos += k.Length;
            buffer[pos++] = '=';
            v.AsSpan().CopyTo(buffer[pos..]);
            pos += v.Length;
        }

        buffer[pos++] = ']';
        return new string(buffer[..pos]);
    }

    private void UpdateGrainTypeMetrics(string instrumentName, string grainType, double value, bool? isSuccess)
    {
        var metrics = _grainTypeMetrics.GetOrAdd(grainType,
            _ => new GrainTypeMetrics(grainType, DefaultMetricsSampleLimit));

        // Match Orleans patterns (latency, requests)
        if (instrumentName.Contains("latency", StringComparison.OrdinalIgnoreCase) ||
            instrumentName.Contains("duration", StringComparison.OrdinalIgnoreCase))
        {
            metrics.AddLatencySample(value);
        }
        else if (instrumentName.Contains("requests", StringComparison.OrdinalIgnoreCase) ||
                 instrumentName.Contains("calls", StringComparison.OrdinalIgnoreCase))
        {
            metrics.IncrementRequests(isSuccess ?? true);
        }
        else if (instrumentName.Contains("errors", StringComparison.OrdinalIgnoreCase))
        {
            // Track errors as failed requests
            metrics.IncrementRequests(false);
        }

        metrics.LastUpdated = DateTime.UtcNow;
    }

    private void UpdateGrainMethodMetrics(string grainType, string method, string instrumentName,
        double value, bool? isSuccess)
    {
        var key = $"{grainType}::{method}";
        var metrics = _grainMethodMetrics.GetOrAdd(key,
            _ => new GrainMethodMetrics(grainType, method, DefaultMethodProfileSampleLimit));

        // Match Orleans patterns (latency, requests)
        if (instrumentName.Contains("latency", StringComparison.OrdinalIgnoreCase) ||
            instrumentName.Contains("duration", StringComparison.OrdinalIgnoreCase))
        {
            metrics.AddLatencySample(value);
        }
        else if (instrumentName.Contains("requests", StringComparison.OrdinalIgnoreCase) ||
                 instrumentName.Contains("calls", StringComparison.OrdinalIgnoreCase))
        {
            metrics.IncrementRequests(isSuccess ?? true);
        }
        else if (instrumentName.Contains("errors", StringComparison.OrdinalIgnoreCase))
        {
            // Track errors as failed requests
            metrics.IncrementRequests(false);
        }

        metrics.LastUpdated = DateTime.UtcNow;
    }

    /// <summary>
    /// Updates the cumulative latency sum from orleans-app-requests-latency-sum.
    /// </summary>
    private void UpdateLatencySum(double cumulativeSum)
    {
        using (_clusterLatencyLock.EnterScope())
        {
            _currentLatencySum = cumulativeSum;
            RecalculateAverageLatency();
        }
    }

    /// <summary>
    /// Updates the cumulative request count from orleans-app-requests-latency-count.
    /// </summary>
    private void UpdateLatencyCount(long cumulativeCount)
    {
        using (_clusterLatencyLock.EnterScope())
        {
            _currentLatencyCount = cumulativeCount;
            RecalculateAverageLatency();
        }
    }

    /// <summary>
    /// Recalculates the average latency using delta-based calculation.
    /// Computes the average latency for the recent interval only, not all-time average.
    /// Uses exponential smoothing to avoid sudden jumps in the display.
    /// </summary>
    private void RecalculateAverageLatency()
    {
        // Calculate deltas since last calculation
        var sumDelta = _currentLatencySum - _lastLatencySum;
        var countDelta = _currentLatencyCount - _lastLatencyCount;

        // Handle counter reset (e.g., silo restart) - deltas would be negative
        if (sumDelta < 0 || countDelta < 0)
        {
            // Reset baseline to current values
            _lastLatencySum = _currentLatencySum;
            _lastLatencyCount = _currentLatencyCount;
            return;
        }

        if (countDelta > 0)
        {
            // Calculate average latency for THIS interval only
            var intervalAvg = sumDelta / countDelta;

            if (_currentAvgLatency > 0)
            {
                // Apply exponential smoothing to prevent sudden jumps
                _currentAvgLatency = (_currentAvgLatency * LatencyOldWeight) + (intervalAvg * LatencyNewWeight);
            }
            else
            {
                _currentAvgLatency = intervalAvg;
            }
        }
        // else: no new requests this interval, keep previous average

        // Remember current values for next delta calculation
        _lastLatencySum = _currentLatencySum;
        _lastLatencyCount = _currentLatencyCount;
    }

    /// <summary>
    /// Gets summarized Orleans cluster metrics for dashboard display.
    /// </summary>
    public OrleansClusterMetricsSnapshot GetClusterMetrics()
    {
        // CPU and Memory from Process tracking
        double cpuPercent;
        long memoryMb;
        using (_processMetricsLock.EnterScope())
        {
            cpuPercent = _currentCpuPercent;
            memoryMb = _currentWorkingSetMb;
        }

        // Latency from delta-based calculation
        double avgLatency;
        using (_clusterLatencyLock.EnterScope())
        {
            avgLatency = _currentAvgLatency;
        }

        // Get metrics from Orleans OTel instruments
        var totalActivations = (long)GetMetricValue("orleans-catalog-activations");
        var connectedClients = (int)GetMetricValue("orleans-gateway-connected-clients");
        var messagesSent = (long)GetMetricValue("orleans-messaging-processing-dispatcher-processed");
        var messagesReceived = (long)GetMetricValue("orleans-messaging-processing-dispatcher-received");
        var messagesDropped = (long)GetMetricValue("orleans-messaging-sent-dropped");
        var availableMemoryBytes = GetMetricValue("orleans-runtime-available-memory");
        var availableMemoryMb = (long)(availableMemoryBytes / (1024 * 1024));

        // Catalog metrics (activation lifecycle)
        var activationWorkingSet = (long)GetMetricValue("orleans-catalog-activation-working-set");
        var activationsCreated = (long)GetMetricValue("orleans-catalog-activation-created");
        var activationsDestroyed = (long)GetMetricValue("orleans-catalog-activation-destroyed");
        var activationsFailedToActivate = (long)GetMetricValue("orleans-catalog-activation-failed-to-activate");
        var activationCollections = (long)GetMetricValue("orleans-catalog-activation-collection");
        var activationShutdowns = (long)GetMetricValue("orleans-catalog-activation-shutdown");
        var activationNonExistent = (long)GetMetricValue("orleans-catalog-activation-non-existent");
        var concurrentRegistrationAttempts = (long)GetMetricValue("orleans-catalog-activation-concurrent-registration-attempts");

        // Miscellaneous grain metrics
        var grainCount = (long)GetMetricValue("orleans-grains");
        var systemTargets = (long)GetMetricValue("orleans-system-targets");

        return new OrleansClusterMetricsSnapshot
        {
            TotalActivations = totalActivations,
            ConnectedClients = connectedClients,
            MessagesSent = messagesSent,
            MessagesReceived = messagesReceived,
            MessagesDropped = messagesDropped,
            CpuUsagePercent = cpuPercent,
            MemoryUsageMb = memoryMb,
            AvailableMemoryMb = availableMemoryMb,
            AverageRequestLatencyMs = avgLatency,
            TotalRequests = messagesSent, // Use messages sent as proxy for total requests
            ActivationWorkingSet = activationWorkingSet,
            ActivationsCreated = activationsCreated,
            ActivationsDestroyed = activationsDestroyed,
            ActivationsFailedToActivate = activationsFailedToActivate,
            ActivationCollections = activationCollections,
            ActivationShutdowns = activationShutdowns,
            ActivationNonExistent = activationNonExistent,
            ConcurrentRegistrationAttempts = concurrentRegistrationAttempts,
            GrainCount = grainCount,
            SystemTargets = systemTargets,
            LastUpdated = DateTime.UtcNow
        };
    }

    /// <summary>
    /// Gets per-grain-type metrics collected from Orleans instrumentation.
    /// </summary>
    public IReadOnlyDictionary<string, GrainTypeMetrics> GetGrainTypeMetrics() => _grainTypeMetrics;

    /// <summary>
    /// Gets per-method metrics for all grain methods.
    /// </summary>
    public IReadOnlyDictionary<string, GrainMethodMetrics> GetGrainMethodMetrics() => _grainMethodMetrics;

    private double GetMetricValue(string prefix)
    {
        // Sum all metrics that start with this prefix (may have tags appended)
        return _metrics
            .Where(kv => kv.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .Sum(kv => kv.Value);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _pollTimer?.Dispose();
        _meterListener?.Dispose();
        _logger.LogInformation("Orleans metrics listener disposed ({InstrumentCount} instruments, {MetricCount} metrics)",
            _instrumentCount, _metrics.Count);
    }
}

/// <summary>
/// Snapshot of Orleans cluster metrics from OTel MeterListener.
/// </summary>
public sealed record OrleansClusterMetricsSnapshot
{
    public long TotalActivations { get; init; }
    public int ConnectedClients { get; init; }
    public long MessagesSent { get; init; }
    public long MessagesReceived { get; init; }
    public long MessagesDropped { get; init; }
    public double CpuUsagePercent { get; init; }
    public long MemoryUsageMb { get; init; }
    public long AvailableMemoryMb { get; init; }
    public double AverageRequestLatencyMs { get; init; }
    public long TotalRequests { get; init; }
    public DateTime LastUpdated { get; init; }

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

/// <summary>
/// Aggregated metrics for a specific grain type.
/// Thread-safe with bounded queue to prevent memory growth.
/// </summary>
public sealed class GrainTypeMetrics
{
    private readonly Lock _lock = new();
    private readonly Queue<double> _recentLatencies;
    private readonly int _maxSamples;
    private double _latencySum; // Running sum for O(1) average calculation

    public string GrainType { get; }
    public long TotalRequests { get; private set; }
    public long FailedRequests { get; private set; }
    public double AverageLatencyMs { get; private set; }
    public double MinLatencyMs { get; private set; } = double.MaxValue;
    public double MaxLatencyMs { get; private set; }
    public DateTime FirstSeen { get; }
    public DateTime LastUpdated { get; set; }

    // For throughput calculation (requests per second) - uses Stopwatch for monotonic timing
    private long _requestsAtWindowStart;
    private readonly Stopwatch _windowStopwatch = Stopwatch.StartNew();
    public double RequestsPerSecond { get; private set; }

    public GrainTypeMetrics(string grainType, int maxSamples = 100)
    {
        GrainType = grainType;
        _maxSamples = maxSamples;
        _recentLatencies = new Queue<double>(maxSamples);
        FirstSeen = DateTime.UtcNow;
        LastUpdated = DateTime.UtcNow;
    }

    public void AddLatencySample(double latencyMs)
    {
        using (_lock.EnterScope())
        {
            // Maintain bounded rolling window of recent latencies
            if (_recentLatencies.Count >= _maxSamples)
            {
                // Remove oldest sample from running average
                var removed = _recentLatencies.Dequeue();
                _latencySum -= removed;
            }

            _recentLatencies.Enqueue(latencyMs);
            _latencySum += latencyMs;

            // Update statistics using running sum (O(1) instead of O(n))
            AverageLatencyMs = _recentLatencies.Count > 0 ? _latencySum / _recentLatencies.Count : 0;
            MinLatencyMs = Math.Min(MinLatencyMs, latencyMs);
            MaxLatencyMs = Math.Max(MaxLatencyMs, latencyMs);
        }
    }

    public void IncrementRequests(bool success)
    {
        using (_lock.EnterScope())
        {
            TotalRequests++;
            if (!success)
                FailedRequests++;

            // Calculate throughput over sliding window using monotonic Stopwatch
            var windowDuration = _windowStopwatch.Elapsed.TotalSeconds;

            if (windowDuration >= 1.0)
            {
                RequestsPerSecond = (TotalRequests - _requestsAtWindowStart) / windowDuration;
                _requestsAtWindowStart = TotalRequests;
                _windowStopwatch.Restart();
            }
        }
    }

    public double ExceptionRate => TotalRequests > 0
        ? (FailedRequests / (double)TotalRequests) * 100.0
        : 0.0;
}

/// <summary>
/// Aggregated metrics for a specific grain method.
/// Thread-safe with bounded sample queue to prevent memory growth.
/// </summary>
public sealed class GrainMethodMetrics
{
    private readonly Lock _lock = new();
    private readonly Queue<MethodMetricSample> _samples;
    private readonly int _maxSamples;

    public string GrainType { get; }
    public string MethodName { get; }
    public long TotalRequests { get; private set; }
    public long FailedRequests { get; private set; }
    public double AverageLatencyMs { get; private set; }
    public DateTime LastUpdated { get; set; }

    // For throughput tracking - uses Stopwatch for monotonic timing
    private readonly Stopwatch _sampleStopwatch = Stopwatch.StartNew();
    private long _requestsAtLastSample;
    private long _failedRequestsAtLastSample;
    private double _lastLatencyMs;

    public GrainMethodMetrics(string grainType, string methodName, int maxSamples = 120)
    {
        GrainType = grainType;
        MethodName = methodName;
        _maxSamples = maxSamples;
        _samples = new Queue<MethodMetricSample>(maxSamples);
    }

    public void AddLatencySample(double latencyMs)
    {
        using (_lock.EnterScope())
        {
            _lastLatencyMs = latencyMs;

            // Update running average using exponential moving average
            AverageLatencyMs = AverageLatencyMs == 0
                ? latencyMs
                : (AverageLatencyMs * 0.9) + (latencyMs * 0.1);

            // Try to record a sample if enough time has passed
            TryRecordSample();
        }
    }

    public void IncrementRequests(bool success)
    {
        using (_lock.EnterScope())
        {
            TotalRequests++;
            if (!success)
                FailedRequests++;

            // Try to record a sample if enough time has passed
            TryRecordSample();
        }
    }

    /// <summary>
    /// Records a time-series sample if at least 1 second has elapsed.
    /// </summary>
    private void TryRecordSample()
    {
        var elapsed = _sampleStopwatch.Elapsed.TotalSeconds;

        if (elapsed >= 1.0)
        {
            var requestsDelta = TotalRequests - _requestsAtLastSample;
            var throughput = requestsDelta / elapsed;
            var failedDelta = Math.Max(0, FailedRequests - _failedRequestsAtLastSample);

            // Maintain bounded queue
            if (_samples.Count >= _maxSamples)
                _samples.Dequeue();

            _samples.Enqueue(new MethodMetricSample(DateTime.UtcNow, throughput, _lastLatencyMs > 0 ? _lastLatencyMs : AverageLatencyMs, failedDelta));

            _sampleStopwatch.Restart();
            _requestsAtLastSample = TotalRequests;
            _failedRequestsAtLastSample = FailedRequests;
        }
    }

    /// <summary>
    /// Gets the recent samples for charting.
    /// </summary>
    public List<MethodMetricSample> GetRecentSamples(int maxCount = 60)
    {
        using (_lock.EnterScope())
        {
            // Pre-allocate list with exact capacity to avoid resizing
            var count = Math.Min(maxCount, _samples.Count);
            var result = new List<MethodMetricSample>(count);

            // Use Skip instead of TakeLast to avoid LINQ allocation
            var skipCount = _samples.Count - count;
            var i = 0;
            foreach (var sample in _samples)
            {
                if (i >= skipCount)
                    result.Add(sample);
                i++;
            }
            return result;
        }
    }

    /// <summary>
    /// Gets the current throughput and latency snapshot for real-time display.
    /// </summary>
    public (double RequestsPerSecond, double LatencyMs, double FailedCount) GetCurrentSnapshot()
    {
        using (_lock.EnterScope())
        {
            var elapsed = _sampleStopwatch.Elapsed.TotalSeconds;

            if (elapsed < 0.1) elapsed = 1.0; // Avoid division by near-zero

            var requestsDelta = TotalRequests - _requestsAtLastSample;
            var throughput = requestsDelta / elapsed;
            var failedDelta = Math.Max(0, FailedRequests - _failedRequestsAtLastSample);

            return (throughput, _lastLatencyMs > 0 ? _lastLatencyMs : AverageLatencyMs, failedDelta);
        }
    }
}

/// <summary>
/// A single time-series sample for method profiling charts.
/// </summary>
public record MethodMetricSample(
    DateTime Timestamp,
    double RequestsPerSecond,
    double LatencyMs,
    double FailedCount);
