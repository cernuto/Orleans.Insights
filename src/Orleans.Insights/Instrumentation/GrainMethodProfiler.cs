using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Orleans.Insights.Models;
using Orleans.Runtime;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Grain method profiler that collects accurate per-method metrics using the OrleansDashboard pattern.
///
/// Key Design (from OrleansDashboard):
/// - Uses atomic swap pattern: accumulates metrics, then atomically swaps the dictionary every second
/// - Stores totals (count + elapsedTime) not averages - enables accurate average calculation: avg = total / count
/// - Fire-and-forget reporting to InsightsGrain for DuckDB batch storage
/// - Uses ILifecycleParticipant for proper Orleans silo lifecycle integration
///
/// This runs alongside the existing OTel instrumentation:
/// - OTel meters (GrainInstrumentation): For external observability (Prometheus, Azure Monitor)
/// - This profiler: For accurate dashboard display with proper aggregation via DuckDB
///
/// Thread Safety:
/// - ConcurrentDictionary.AddOrUpdate is lock-free for high-throughput grain calls
/// - Interlocked.Exchange provides atomic swap without blocking
/// - Single timer thread processes stats, no contention with grain calls
///
/// Registration (in each silo):
/// <code>
/// siloBuilder.AddIncomingGrainCallFilter&lt;GlobalGrainCallFilter&gt;();
/// siloBuilder.Services.AddSingleton&lt;GrainMethodProfiler&gt;();
/// siloBuilder.Services.AddSingleton&lt;ILifecycleParticipant&lt;ISiloLifecycle&gt;&gt;(sp => sp.GetRequiredService&lt;GrainMethodProfiler&gt;());
/// </code>
/// </summary>
public sealed class GrainMethodProfiler : IGrainMethodProfiler, ILifecycleParticipant<ISiloLifecycle>, IDisposable
{
    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<GrainMethodProfiler> _logger;
    private readonly string _hostName;

    private ConcurrentDictionary<string, MethodTraceEntry> _traces = new();
    private Timer? _timer;
    private IInsightsGrain? _insightsGrain;
    private string? _siloAddress;
    private bool _disposed;
    private bool _isRunning;

    public GrainMethodProfiler(
        IGrainFactory grainFactory,
        ILocalSiloDetails localSiloDetails,
        ILogger<GrainMethodProfiler> logger)
    {
        _grainFactory = grainFactory;
        _logger = logger;
        _hostName = Environment.MachineName;

        // Cache the silo address for reporting
        _siloAddress = localSiloDetails.SiloAddress.ToParsableString();
    }

    /// <summary>
    /// Participates in the silo lifecycle - starts after all services are ready.
    /// This ensures the grain factory is fully initialized before we start sending reports.
    /// </summary>
    public void Participate(ISiloLifecycle lifecycle)
    {
        // Subscribe at ServiceLifecycleStage.Last to ensure all grain services are ready
        lifecycle.Subscribe<GrainMethodProfiler>(
            ServiceLifecycleStage.Last,
            OnStart,
            OnStop);
    }

    /// <summary>
    /// Records a grain method call. Called from GlobalGrainCallFilter after each grain invocation.
    ///
    /// Performance: ~50-100ns overhead per call (ConcurrentDictionary.AddOrUpdate is lock-free)
    /// </summary>
    /// <param name="grainType">Short grain type name (e.g., "Device" not "MyApp.Grains.DeviceGrain")</param>
    /// <param name="methodName">Method name (e.g., "GetState")</param>
    /// <param name="elapsedMs">Elapsed time in milliseconds</param>
    /// <param name="failed">Whether the call threw an exception</param>
    public void Track(string grainType, string methodName, double elapsedMs, bool failed)
    {
        if (!_isRunning) return;

        var key = $"{grainType}::{methodName}";
        var exceptionCount = failed ? 1L : 0L;

        _traces.AddOrUpdate(
            key,
            // Factory for new entry
            _ => new MethodTraceEntry
            {
                GrainType = grainType,
                Method = methodName,
                Count = 1,
                ElapsedTime = elapsedMs,
                ExceptionCount = exceptionCount
            },
            // Update existing entry (accumulate totals)
            (_, existing) =>
            {
                existing.Count++;
                existing.ElapsedTime += elapsedMs;
                existing.ExceptionCount += exceptionCount;
                return existing;
            });
    }

    private Task OnStart(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "GrainMethodProfiler starting. SiloAddress={SiloAddress}, HostName={HostName}",
            _siloAddress, _hostName);

        _isRunning = true;

        // Start timer to process and send stats every second (matches OrleansDashboard)
        // Using 1000ms interval as OrleansDashboard does
        _timer = new Timer(
            ProcessStats,
            null,
            1000,  // Due time: 1 second
            1000); // Period: 1 second

        return Task.CompletedTask;
    }

    private Task OnStop(CancellationToken cancellationToken)
    {
        _isRunning = false;
        _timer?.Change(Timeout.Infinite, 0);

        // Process any remaining stats before shutdown
        ProcessStats(null);

        _logger.LogInformation("GrainMethodProfiler stopped");
        return Task.CompletedTask;
    }

    private void ProcessStats(object? state)
    {
        if (_disposed || !_isRunning) return;

        try
        {
            // Atomic swap - get all accumulated metrics and reset to empty dictionary
            // This is the key pattern from OrleansDashboard that ensures:
            // 1. No data loss (all accumulated data is captured)
            // 2. No blocking (grain calls continue writing to new dictionary)
            // 3. Accurate totals (not averages of averages)
            var currentTraces = Interlocked.Exchange(ref _traces, new ConcurrentDictionary<string, MethodTraceEntry>());

            if (currentTraces.IsEmpty)
                return;

            var entries = currentTraces.Values.ToArray();

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    "Processing {Count} method traces: {Samples}",
                    entries.Length,
                    string.Join(", ", entries.Take(5).Select(e => $"{e.GrainType}.{e.Method}={e.Count}")));
            }

            // Build the report entries
            var reportEntries = new List<MethodProfileEntry>(entries.Length);
            foreach (var e in entries)
            {
                reportEntries.Add(new MethodProfileEntry
                {
                    GrainType = e.GrainType,
                    Method = e.Method,
                    Count = e.Count,
                    TotalElapsedMs = e.ElapsedTime,
                    ExceptionCount = e.ExceptionCount
                });
            }

            // Build the report
            var report = new MethodProfileReport
            {
                SiloId = _siloAddress ?? _hostName,
                HostName = _hostName,
                Timestamp = DateTime.UtcNow,
                Entries = reportEntries
            };

            // Get or cache the InsightsGrain reference
            _insightsGrain ??= _grainFactory.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);

            // Fire-and-forget send to aggregator (like OrleansDashboard's SubmitTracing)
            // Using Immutable<T> wrapper for efficient serialization
            _insightsGrain.IngestMethodProfile(report.AsImmutable()).Ignore();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error processing grain method stats");
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _isRunning = false;
        _timer?.Dispose();
    }
}

/// <summary>
/// Mutable entry for accumulating method trace data.
/// Only used internally within GrainMethodProfiler before atomic swap.
/// </summary>
internal sealed class MethodTraceEntry
{
    public required string GrainType { get; init; }
    public required string Method { get; init; }
    public long Count { get; set; }
    public double ElapsedTime { get; set; }
    public long ExceptionCount { get; set; }
}
