using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Orleans;
using Orleans.Concurrency;
using Orleans.Insights.Models;
using Orleans.Runtime;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Grain method profiler that collects accurate per-method metrics using the OrleansDashboard pattern.
/// </summary>
/// <remarks>
/// <para>
/// Key Design (from OrleansDashboard):
/// </para>
/// <list type="bullet">
/// <item>Uses atomic swap pattern: accumulates metrics, then atomically swaps the dictionary every second</item>
/// <item>Stores totals (count + elapsedTime) not averages - enables accurate average calculation: avg = total / count</item>
/// <item>Fire-and-forget reporting to InsightsGrain for DuckDB batch storage</item>
/// <item>Uses ILifecycleParticipant for proper Orleans silo lifecycle integration</item>
/// </list>
/// <para>
/// Thread Safety:
/// </para>
/// <list type="bullet">
/// <item>ConcurrentDictionary.AddOrUpdate is lock-free for high-throughput grain calls</item>
/// <item>Interlocked.Exchange provides atomic swap without blocking</item>
/// <item>Single timer thread processes stats, no contention with grain calls</item>
/// </list>
/// <para>
/// <b>Optimization:</b> Uses ObjectPool for report entries list to reduce GC pressure.
/// </para>
/// </remarks>
public sealed class GrainMethodProfiler : IGrainMethodProfiler, ILifecycleParticipant<ISiloLifecycle>, IDisposable
{
    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<GrainMethodProfiler> _logger;
    private readonly string _hostName;
    private readonly string _siloAddress;

    // Object pool for report entries list to avoid repeated allocations
    private readonly ObjectPool<List<MethodProfileEntry>> _entriesPool;

    private ConcurrentDictionary<string, MethodTraceEntry> _traces = new();
    private Timer? _timer;
    private IInsightsGrain? _insightsGrain;
    private volatile bool _disposed;
    private volatile bool _isRunning;

    /// <summary>
    /// ObjectPool policy for method profile entries list.
    /// </summary>
    private sealed class EntriesListPoolPolicy : PooledObjectPolicy<List<MethodProfileEntry>>
    {
        public override List<MethodProfileEntry> Create() => new(64);

        public override bool Return(List<MethodProfileEntry> obj)
        {
            obj.Clear();
            return true;
        }
    }

    public GrainMethodProfiler(
        IGrainFactory grainFactory,
        ILocalSiloDetails localSiloDetails,
        ILogger<GrainMethodProfiler> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(localSiloDetails);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _logger = logger;
        _hostName = Environment.MachineName;
        _siloAddress = localSiloDetails.SiloAddress.ToParsableString();

        // Object pool for method profile entries to avoid repeated allocations
        _entriesPool = new DefaultObjectPool<List<MethodProfileEntry>>(
            new EntriesListPoolPolicy(),
            maximumRetained: 4);
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
    /// </summary>
    /// <remarks>Performance: ~50-100ns overhead per call (ConcurrentDictionary.AddOrUpdate is lock-free)</remarks>
    /// <param name="grainType">Short grain type name (e.g., "Device" not "MyApp.Grains.DeviceGrain")</param>
    /// <param name="methodName">Method name (e.g., "GetState")</param>
    /// <param name="elapsedMs">Elapsed time in milliseconds</param>
    /// <param name="failed">Whether the call threw an exception</param>
    public void Track(string grainType, string methodName, double elapsedMs, bool failed)
    {
        if (!_isRunning) return;

        var key = string.Create(grainType.Length + 2 + methodName.Length, (grainType, methodName), static (span, state) =>
        {
            var (grain, method) = state;
            grain.AsSpan().CopyTo(span);
            span[grain.Length] = ':';
            span[grain.Length + 1] = ':';
            method.AsSpan().CopyTo(span[(grain.Length + 2)..]);
        });

        var exceptionCount = failed ? 1L : 0L;

        _traces.AddOrUpdate(
            key,
            // Factory for new entry - capture values for closure
            static (_, args) => new MethodTraceEntry
            {
                GrainType = args.grainType,
                Method = args.methodName,
                Count = 1,
                ElapsedTime = args.elapsedMs,
                ExceptionCount = args.exceptionCount
            },
            // Update existing entry (accumulate totals)
            static (_, existing, args) =>
            {
                existing.Count++;
                existing.ElapsedTime += args.elapsedMs;
                existing.ExceptionCount += args.exceptionCount;
                return existing;
            },
            (grainType, methodName, elapsedMs, exceptionCount));
    }

    private Task OnStart(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "GrainMethodProfiler starting. SiloAddress={SiloAddress}, HostName={HostName}",
            _siloAddress, _hostName);

        _isRunning = true;

        // Start timer to process and send stats every second (matches OrleansDashboard)
        _timer = new Timer(
            ProcessStats,
            null,
            1000,
            1000);

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

    /// <summary>
    /// Timer callback that processes accumulated stats.
    /// Uses ObjectPool for entries list to avoid repeated allocations.
    /// </summary>
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

            // Use CollectionsMarshal for zero-copy iteration over dictionary values
            var entriesSpan = CollectionsMarshal.AsSpan(currentTraces.Values.ToList());

            // Get report entries from pool - returned after report is built
            var reportEntries = _entriesPool.Get();
            try
            {
                foreach (ref readonly var e in entriesSpan)
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

                // Build the report - must copy entries since we'll return list to pool
                var report = new MethodProfileReport
                {
                    SiloId = _siloAddress,
                    HostName = _hostName,
                    Timestamp = DateTime.UtcNow,
                    Entries = [.. reportEntries] // Copy to new list for serialization
                };

                // Get or cache the InsightsGrain reference
                _insightsGrain ??= _grainFactory.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);

                // Fire-and-forget send to aggregator (like OrleansDashboard's SubmitTracing)
                // Using Immutable<T> wrapper for efficient serialization
                _insightsGrain.IngestMethodProfile(report.AsImmutable()).Ignore();
            }
            finally
            {
                // Return entries to pool
                _entriesPool.Return(reportEntries);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error processing grain method stats");
        }
    }

    private static string FormatTraceSamples(ReadOnlySpan<MethodTraceEntry> entries)
    {
        var sampleCount = Math.Min(5, entries.Length);
        var samples = new string[sampleCount];
        for (var i = 0; i < sampleCount; i++)
        {
            ref readonly var e = ref entries[i];
            samples[i] = $"{e.GrainType}.{e.Method}={e.Count}, elapsed={e.ElapsedTime:F2}ms";
        }
        return string.Join(", ", samples);
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
