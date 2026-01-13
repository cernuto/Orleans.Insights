using System.Collections.Concurrent;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Thread-safe cache for grain type metadata used in instrumentation.
/// Provides cached lookups for grain type names, OTel tags, and instrumentation decisions.
/// </summary>
/// <remarks>
/// <para>
/// This class follows Single Responsibility Principle - it handles only grain type name
/// resolution and caching, extracted from GlobalGrainCallFilter.
/// </para>
/// <para>
/// All methods are static and thread-safe via ConcurrentDictionary.
/// Cache lookups are lock-free after warmup (O(1) amortized).
/// </para>
/// </remarks>
public static class GrainTypeNameCache
{
    // Cache grain type names (full name with assembly for troubleshooting)
    private static readonly ConcurrentDictionary<Type, string> _grainTypeNames = new();

    // Cache grain type tags for OTel (includes full tag structure)
    private static readonly ConcurrentDictionary<Type, KeyValuePair<string, object?>> _grainTypeTags = new();

    // Cache whether a grain type should be instrumented (false = skip, true = instrument)
    private static readonly ConcurrentDictionary<Type, bool> _shouldInstrument = new();

    /// <summary>
    /// Gets the full display name for a grain type including namespace and assembly.
    /// Format: "Namespace.TypeName,AssemblyName" for troubleshooting purposes.
    /// </summary>
    /// <param name="grainType">The grain type</param>
    /// <returns>Full type name with assembly (e.g., "MyApp.Grains.MyGrain,MyApp.Grains")</returns>
    public static string GetGrainTypeName(Type grainType)
    {
        return _grainTypeNames.GetOrAdd(grainType, static t =>
        {
            var assemblyName = t.Assembly.GetName().Name;
            return $"{t.FullName},{assemblyName}";
        });
    }

    /// <summary>
    /// Gets the OTel tag for a grain type.
    /// </summary>
    /// <param name="grainType">The grain type</param>
    /// <returns>KeyValuePair suitable for OTel meter tags</returns>
    public static KeyValuePair<string, object?> GetGrainTypeTag(Type grainType)
    {
        return _grainTypeTags.GetOrAdd(grainType, static t =>
            new KeyValuePair<string, object?>("grain.type", GetGrainTypeName(t)));
    }

    /// <summary>
    /// Determines if a grain type should be instrumented.
    /// Currently instruments all grains without filtering.
    /// </summary>
    /// <param name="grainType">The grain type</param>
    /// <returns>Always returns true - all grains are instrumented</returns>
    public static bool ShouldInstrumentGrain(Type grainType)
    {
        // Instrument all grains - no filtering
        return true;
    }
}
