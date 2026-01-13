using Orleans;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Global silo-level grain call filter that instruments ALL grain calls,
/// including third-party grains like SignalRConnectionCoordinatorGrain.
/// </summary>
/// <remarks>
/// <para>
/// This filter is registered at the silo level via AddIncomingGrainCallFilter,
/// which means it intercepts every grain method call in the silo.
/// </para>
/// <para>
/// Dual Instrumentation:
/// <list type="number">
/// <item>OTel meters (GrainInstrumentation): For external observability (Prometheus, Azure Monitor)</item>
/// <item>IGrainMethodProfiler: For accurate dashboard display using OrleansDashboard pattern</item>
/// </list>
/// </para>
/// <para>
/// Performance:
/// <list type="bullet">
/// <item>~100-200ns overhead per call (negligible vs typical grain work of 1-50ms)</item>
/// <item>Lock-free reads via ConcurrentDictionary caching in GrainTypeNameCache</item>
/// <item>No allocations on hot path after warmup</item>
/// </list>
/// </para>
/// <para>
/// Registration (in each silo):
/// <code>
/// siloBuilder.AddIncomingGrainCallFilter&lt;GlobalGrainCallFilter&gt;();
/// siloBuilder.Services.AddSingleton&lt;GrainMethodProfiler&gt;();
/// siloBuilder.Services.AddSingleton&lt;IGrainMethodProfiler&gt;(sp =&gt; sp.GetRequiredService&lt;GrainMethodProfiler&gt;());
/// siloBuilder.Services.AddSingleton&lt;ILifecycleParticipant&lt;ISiloLifecycle&gt;&gt;(sp =&gt; sp.GetRequiredService&lt;GrainMethodProfiler&gt;());
/// </code>
/// </para>
/// </remarks>
public sealed class GlobalGrainCallFilter : IIncomingGrainCallFilter
{
    private readonly IGrainMethodProfiler? _profiler;

    /// <summary>
    /// Constructor for DI - profiler is optional for backward compatibility.
    /// </summary>
    /// <param name="profiler">Optional profiler for dashboard metrics. If null, only OTel metrics are recorded.</param>
    public GlobalGrainCallFilter(IGrainMethodProfiler? profiler = null)
    {
        _profiler = profiler;
    }

    /// <summary>
    /// Intercepts all grain method calls and records metrics.
    /// </summary>
    public async Task Invoke(IIncomingGrainCallContext context)
    {
        var grainType = context.Grain.GetType();

        // Skip instrumentation for Orleans system grains
        if (!GrainTypeNameCache.ShouldInstrumentGrain(grainType))
        {
            await context.Invoke();
            return;
        }

        var methodName = context.ImplementationMethod?.Name ?? context.InterfaceMethod?.Name ?? "Unknown";
        var grainTypeName = GrainTypeNameCache.GetGrainTypeName(grainType);
        var grainTypeTag = GrainTypeNameCache.GetGrainTypeTag(grainType);
        var methodTag = new KeyValuePair<string, object?>("rpc.method", methodName);
        var startTime = Stopwatch.GetTimestamp();
        var failed = false;

        try
        {
            await context.Invoke();
        }
        catch
        {
            failed = true;
            throw;
        }
        finally
        {
            var elapsedMs = Stopwatch.GetElapsedTime(startTime).TotalMilliseconds;

            // 1. Record to OTel meters (for external observability)
            if (failed)
            {
                GrainInstrumentation.ErrorCounter.Add(1, grainTypeTag, methodTag);
            }
            else
            {
                GrainInstrumentation.CallCounter.Add(1, grainTypeTag, methodTag);
            }
            GrainInstrumentation.DurationHistogram.Record(elapsedMs, grainTypeTag, methodTag);

            // 2. Record to GrainMethodProfiler (for accurate dashboard display)
            // Uses OrleansDashboard pattern: accumulate totals, atomic swap every second
            _profiler?.Track(grainTypeName, methodName, elapsedMs, failed);
        }
    }
}
