using Microsoft.Extensions.Logging;
using Orleans.Insights.Models;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
    private readonly ILocalSiloDetails _localSiloDetails;

    private Timer? _timer;
    private IInsightsGrain? _insightsGrain;
    private IManagementGrain? _managementGrain;
    private string? _siloAddress;
    private SiloAddress? _siloAddressParsed;
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
        _localSiloDetails = localSiloDetails;
        _logger = logger;
        _hostName = Environment.MachineName;
        _currentProcess = Process.GetCurrentProcess();

        // Cache the silo address for reporting
        _siloAddressParsed = localSiloDetails.SiloAddress;
        _siloAddress = _siloAddressParsed.ToParsableString();
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

    private async void CollectAndSendMetrics(object? state)
    {
        if (_disposed || !_isRunning) return;

        try
        {
            var metrics = await CollectMetricsAsync();

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                var activationCount = metrics.GrainTypeMetrics.Values.Sum(g => g.Activations);
                _logger.LogDebug(
                    "Collected silo metrics: CPU={Cpu:F1}%, Memory={Memory}MB, Activations={Activations}, GrainTypes={GrainTypes}",
                    metrics.ClusterMetrics.CpuUsagePercent,
                    metrics.ClusterMetrics.MemoryUsageMb,
                    activationCount,
                    metrics.GrainTypeMetrics.Count);
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

    private async Task<SiloMetricsReport> CollectMetricsAsync()
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

        // Get grain activation statistics from IManagementGrain
        var grainTypeMetrics = new Dictionary<string, SiloGrainTypeMetrics>();
        long totalActivations = 0;

        try
        {
            _managementGrain ??= _grainFactory.GetGrain<IManagementGrain>(0);

            // Get detailed grain statistics for this silo only
            var stats = await _managementGrain.GetDetailedGrainStatistics(
                hostsIds: [_siloAddressParsed!]);

            // Group by grain type and count activations
            var activationCounts = new Dictionary<string, long>();
            foreach (var stat in stats)
            {
                var grainType = GetShortGrainTypeName(stat.GrainType);
                activationCounts.TryGetValue(grainType, out var count);
                activationCounts[grainType] = count + 1;
                totalActivations++;
            }

            // Create immutable SiloGrainTypeMetrics records
            foreach (var (grainType, count) in activationCounts)
            {
                grainTypeMetrics[grainType] = new SiloGrainTypeMetrics
                {
                    GrainType = grainType,
                    Activations = count,
                    LastUpdated = now
                };
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to get grain statistics from ManagementGrain");
        }

        var clusterMetrics = new SiloClusterMetrics
        {
            CpuUsagePercent = cpuUsagePercent,
            MemoryUsageMb = workingSetMb,
            AvailableMemoryMb = availableMemoryMb,
            TotalActivations = totalActivations,
            // Note: Some Orleans-specific metrics like messages, connected clients
            // are not easily accessible without hooking into internal statistics.
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
            GrainCount = totalActivations,
            SystemTargets = 0
        };

        return new SiloMetricsReport
        {
            SiloId = _siloAddress ?? _hostName,
            HostName = _hostName,
            Timestamp = now,
            ClusterMetrics = clusterMetrics,
            GrainTypeMetrics = grainTypeMetrics
        };
    }

    /// <summary>
    /// Gets the short grain type name from the full type string.
    /// Handles both Orleans GrainType format and full type names.
    /// </summary>
    private static string GetShortGrainTypeName(string grainType)
    {
        // GrainType format: "GrainReference=0000000000000000030000009a706b87+MyApp.Grains.MyGrain"
        // or just "MyApp.Grains.MyGrain"
        var plusIndex = grainType.LastIndexOf('+');
        if (plusIndex >= 0)
        {
            grainType = grainType[(plusIndex + 1)..];
        }

        // Get the simple name (after last dot)
        var dotIndex = grainType.LastIndexOf('.');
        return dotIndex >= 0 ? grainType[(dotIndex + 1)..] : grainType;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _isRunning = false;
        _timer?.Dispose();
    }
}
