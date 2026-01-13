using Microsoft.Extensions.Logging;
using Orleans.Insights.Models;
using Orleans.Runtime;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Collects silo-level metrics and reports them to InsightsGrain.
/// Runs on each silo and periodically sends SiloMetricsReport containing
/// cluster metrics (CPU, memory, activations, etc.).
///
/// This complements GrainMethodProfiler which tracks per-method metrics.
/// Together they provide complete observability:
/// - SiloMetricsCollector: Silo health (CPU, memory, activations, messages)
/// - GrainMethodProfiler: Method performance (latency, throughput, errors)
///
/// Uses ILifecycleParticipant for proper Orleans silo lifecycle integration.
/// </summary>
public sealed class SiloMetricsCollector : ILifecycleParticipant<ISiloLifecycle>, IDisposable
{
    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<SiloMetricsCollector> _logger;
    private readonly string _hostName;
    private readonly Process _currentProcess;

    private Timer? _timer;
    private IInsightsGrain? _insightsGrain;
    private string? _siloAddress;
    private bool _disposed;
    private bool _isRunning;

    // Counters for delta calculations
    private long _lastTotalProcessorTime;
    private DateTime _lastMeasurementTime;

    public SiloMetricsCollector(
        IGrainFactory grainFactory,
        ILocalSiloDetails localSiloDetails,
        ILogger<SiloMetricsCollector> logger)
    {
        _grainFactory = grainFactory;
        _logger = logger;
        _hostName = Environment.MachineName;
        _currentProcess = Process.GetCurrentProcess();

        // Cache the silo address for reporting
        _siloAddress = localSiloDetails.SiloAddress.ToParsableString();
    }

    /// <summary>
    /// Participates in the silo lifecycle - starts after all services are ready.
    /// </summary>
    public void Participate(ISiloLifecycle lifecycle)
    {
        lifecycle.Subscribe<SiloMetricsCollector>(
            ServiceLifecycleStage.Last,
            OnStart,
            OnStop);
    }

    private Task OnStart(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "SiloMetricsCollector starting. SiloAddress={SiloAddress}, HostName={HostName}",
            _siloAddress, _hostName);

        _isRunning = true;

        // Initialize baseline measurements
        _lastTotalProcessorTime = _currentProcess.TotalProcessorTime.Ticks;
        _lastMeasurementTime = DateTime.UtcNow;

        // Start timer to collect and send metrics every 5 seconds
        // Using 5-second interval for silo metrics (less frequent than method profiling)
        _timer = new Timer(
            CollectAndSendMetrics,
            null,
            TimeSpan.FromSeconds(5),  // Due time: 5 seconds
            TimeSpan.FromSeconds(5)); // Period: 5 seconds

        return Task.CompletedTask;
    }

    private Task OnStop(CancellationToken cancellationToken)
    {
        _isRunning = false;
        _timer?.Change(Timeout.Infinite, 0);

        // Send final metrics before shutdown
        CollectAndSendMetrics(null);

        _logger.LogInformation("SiloMetricsCollector stopped");
        return Task.CompletedTask;
    }

    private void CollectAndSendMetrics(object? state)
    {
        if (_disposed || !_isRunning) return;

        try
        {
            var metrics = CollectMetrics();

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    "Collected silo metrics: CPU={Cpu:F1}%, Memory={Memory}MB, Activations=N/A",
                    metrics.ClusterMetrics.CpuUsagePercent,
                    metrics.ClusterMetrics.MemoryUsageMb);
            }

            // Get or cache the InsightsGrain reference
            _insightsGrain ??= _grainFactory.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);

            // Fire-and-forget send to InsightsGrain
            _insightsGrain.IngestMetrics(metrics).Ignore();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error collecting silo metrics");
        }
    }

    private SiloMetricsReport CollectMetrics()
    {
        var now = DateTime.UtcNow;

        // Calculate CPU usage since last measurement
        _currentProcess.Refresh();
        var currentTotalProcessorTime = _currentProcess.TotalProcessorTime.Ticks;
        var elapsedTime = (now - _lastMeasurementTime).TotalMilliseconds;

        double cpuUsagePercent = 0;
        if (elapsedTime > 0)
        {
            var cpuTimeDelta = currentTotalProcessorTime - _lastTotalProcessorTime;
            var cpuTimeMs = TimeSpan.FromTicks(cpuTimeDelta).TotalMilliseconds;
            cpuUsagePercent = (cpuTimeMs / elapsedTime / Environment.ProcessorCount) * 100;
            cpuUsagePercent = Math.Min(100, Math.Max(0, cpuUsagePercent)); // Clamp to 0-100
        }

        _lastTotalProcessorTime = currentTotalProcessorTime;
        _lastMeasurementTime = now;

        // Get memory metrics
        var workingSetMb = _currentProcess.WorkingSet64 / (1024 * 1024);
        var gcMemoryMb = GC.GetTotalMemory(false) / (1024 * 1024);

        // Get available system memory
        long availableMemoryMb = 0;
        try
        {
            var gcMemoryInfo = GC.GetGCMemoryInfo();
            availableMemoryMb = gcMemoryInfo.TotalAvailableMemoryBytes / (1024 * 1024);
        }
        catch
        {
            // Fallback if GC info not available
        }

        var clusterMetrics = new SiloClusterMetrics
        {
            CpuUsagePercent = cpuUsagePercent,
            MemoryUsageMb = workingSetMb,
            AvailableMemoryMb = availableMemoryMb,
            // Note: Orleans-specific metrics like activations, messages, etc.
            // would require access to internal Orleans statistics.
            // For now, we provide process-level metrics.
            // These can be extended when Orleans exposes more metrics APIs.
            TotalActivations = 0,
            ConnectedClients = 0,
            MessagesSent = 0,
            MessagesReceived = 0,
            MessagesDropped = 0,
            AverageRequestLatencyMs = 0,
            TotalRequests = 0,
            ActivationWorkingSet = 0,
            ActivationsCreated = 0,
            ActivationsDestroyed = 0,
            ActivationsFailedToActivate = 0,
            ActivationCollections = 0,
            ActivationShutdowns = 0,
            ActivationNonExistent = 0,
            ConcurrentRegistrationAttempts = 0,
            GrainCount = 0,
            SystemTargets = 0
        };

        return new SiloMetricsReport
        {
            SiloId = _siloAddress ?? _hostName,
            HostName = _hostName,
            Timestamp = now,
            ClusterMetrics = clusterMetrics
        };
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _isRunning = false;
        _timer?.Dispose();
    }
}
