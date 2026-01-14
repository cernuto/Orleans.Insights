using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Insights.Models;
using System;
using System.Collections.Concurrent;
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

    // Thread-safe metric storage
    private readonly ConcurrentDictionary<string, double> _metrics = new();

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

        // Store the raw metric value
        _metrics[name] = value;

        // Track cluster-wide latency using cumulative sum/count from Orleans histogram metrics
        if (name.Equals(AppRequestsLatencySum, StringComparison.OrdinalIgnoreCase))
        {
            UpdateLatencySum(value);
        }
        else if (name.Equals(AppRequestsLatencyCount, StringComparison.OrdinalIgnoreCase))
        {
            UpdateLatencyCount((long)value);
        }
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
