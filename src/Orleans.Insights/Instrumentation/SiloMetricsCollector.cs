using Microsoft.Extensions.Logging;
using Orleans.Insights.Models;
using Orleans.Runtime;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Collects silo-level metrics and reports them to InsightsGrain.
/// </summary>
/// <remarks>
/// <para>
/// Runs on each silo and periodically sends SiloMetricsReport containing
/// cluster metrics (CPU, memory, activations, etc.).
/// </para>
/// <para>
/// This complements GrainMethodProfiler which tracks per-method metrics.
/// Together they provide complete observability:
/// </para>
/// <list type="bullet">
/// <item>SiloMetricsCollector: Silo health (CPU, memory, activations, messages)</item>
/// <item>GrainMethodProfiler: Method performance (latency, throughput, errors)</item>
/// </list>
/// <para>
/// Uses OrleansMetricsListener to capture Orleans OTel metrics (latency, messages, etc.)
/// with delta-based latency calculation for accurate display.
/// </para>
/// </remarks>
public sealed class SiloMetricsCollector : ILifecycleParticipant<ISiloLifecycle>, IDisposable
{
    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<SiloMetricsCollector> _logger;
    private readonly IOrleansMetricsListener _metricsListener;
    private readonly string _hostName;
    private readonly string _siloAddress;
    private readonly SiloAddress _siloAddressParsed;

    private Timer? _timer;
    private IInsightsGrain? _insightsGrain;
    private IManagementGrain? _managementGrain;
    private volatile bool _disposed;
    private volatile bool _isRunning;

    public SiloMetricsCollector(
        IGrainFactory grainFactory,
        ILocalSiloDetails localSiloDetails,
        IOrleansMetricsListener metricsListener,
        ILogger<SiloMetricsCollector> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(localSiloDetails);
        ArgumentNullException.ThrowIfNull(metricsListener);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _metricsListener = metricsListener;
        _logger = logger;
        _hostName = Environment.MachineName;

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

        // Start timer to collect and send metrics every 5 seconds
        _timer = new Timer(
            CollectAndSendMetrics,
            null,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(5));

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

        // Get Orleans OTel metrics from the listener (CPU, memory, latency, messages, etc.)
        // Uses delta-based latency calculation for accurate display
        var otelMetrics = _metricsListener.GetClusterMetrics();

        // Get grain activation statistics from IManagementGrain
        // IMPORTANT: Use IManagementGrain for per-silo activation counts, not OTel
        // OTel orleans-grains metric is cluster-wide and would cause double-counting
        var grainTypeMetrics = new Dictionary<string, SiloGrainTypeMetrics>();
        long totalActivations = 0;

        try
        {
            _managementGrain ??= _grainFactory.GetGrain<IManagementGrain>(0);

            // Get detailed grain statistics for this silo only
            var stats = await _managementGrain.GetDetailedGrainStatistics(
                hostsIds: [_siloAddressParsed]);

            // Group by grain type and count activations
            var activationCounts = new Dictionary<string, long>();
            foreach (var stat in stats)
            {
                var grainType = ExtractGrainTypeName(stat.GrainType);
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

        // Build cluster metrics combining OTel data with IManagementGrain activation count
        var clusterMetrics = new SiloClusterMetrics
        {
            // CPU/memory from OrleansMetricsListener (delta-based, smoothed)
            CpuUsagePercent = otelMetrics.CpuUsagePercent,
            MemoryUsageMb = otelMetrics.MemoryUsageMb,
            AvailableMemoryMb = otelMetrics.AvailableMemoryMb,
            // Activations from IManagementGrain (per-silo, not cluster-wide)
            TotalActivations = totalActivations,
            // Orleans OTel metrics (messages, latency, etc.)
            ConnectedClients = otelMetrics.ConnectedClients,
            MessagesSent = otelMetrics.MessagesSent,
            MessagesReceived = otelMetrics.MessagesReceived,
            MessagesDropped = otelMetrics.MessagesDropped,
            AverageRequestLatencyMs = otelMetrics.AverageRequestLatencyMs,
            TotalRequests = otelMetrics.TotalRequests,
            // Catalog metrics (activation lifecycle)
            ActivationWorkingSet = otelMetrics.ActivationWorkingSet,
            ActivationsCreated = otelMetrics.ActivationsCreated,
            ActivationsDestroyed = otelMetrics.ActivationsDestroyed,
            ActivationsFailedToActivate = otelMetrics.ActivationsFailedToActivate,
            ActivationCollections = otelMetrics.ActivationCollections,
            ActivationShutdowns = otelMetrics.ActivationShutdowns,
            ActivationNonExistent = otelMetrics.ActivationNonExistent,
            ConcurrentRegistrationAttempts = otelMetrics.ConcurrentRegistrationAttempts,
            // Miscellaneous grain metrics
            GrainCount = otelMetrics.GrainCount > 0 ? otelMetrics.GrainCount : totalActivations,
            SystemTargets = otelMetrics.SystemTargets
        };

        return new SiloMetricsReport
        {
            SiloId = _siloAddress,
            HostName = _hostName,
            Timestamp = now,
            ClusterMetrics = clusterMetrics,
            GrainTypeMetrics = grainTypeMetrics
        };
    }

    /// <summary>
    /// Extracts the full type name from the Orleans GrainType string.
    /// Returns "Namespace.TypeName" to match GrainTypeNameCache format (Type.FullName).
    ///
    /// Input formats from Orleans GetDetailedGrainStatistics:
    /// - "GrainReference=0000000000000000030000009a706b87+MyApp.Grains.MyGrain"
    /// - "MyApp.Grains.MyGrain"
    ///
    /// Output: "MyApp.Grains.MyGrain" (full type name without GrainReference prefix)
    /// </summary>
    private static string ExtractGrainTypeName(string grainType)
    {
        // GrainType format can have a prefix like:
        // "GrainReference=0000000000000000030000009a706b87+MyApp.Grains.MyGrain"
        var plusIndex = grainType.LastIndexOf('+');
        if (plusIndex >= 0)
        {
            grainType = grainType[(plusIndex + 1)..];
        }

        return grainType;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _isRunning = false;
        _timer?.Dispose();
    }
}
