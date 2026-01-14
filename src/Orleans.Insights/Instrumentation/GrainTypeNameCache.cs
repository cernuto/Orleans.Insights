using System.Collections.Concurrent;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Thread-safe cache for grain type metadata used in instrumentation.
/// Provides cached lookups for grain type names and OTel tags.
/// </summary>
/// <remarks>
/// All methods are static and thread-safe via ConcurrentDictionary.
/// Cache lookups are lock-free after warmup (O(1) amortized).
/// </remarks>
public static class GrainTypeNameCache
{
    private static readonly ConcurrentDictionary<Type, string> _grainTypeNames = new();
    private static readonly ConcurrentDictionary<Type, KeyValuePair<string, object?>> _grainTypeTags = new();

    /// <summary>
    /// Gets the full type name for a grain type.
    /// </summary>
    public static string GetGrainTypeName(Type grainType)
    {
        return _grainTypeNames.GetOrAdd(grainType, static t => t.FullName ?? t.Name);
    }

    /// <summary>
    /// Gets the OTel tag for a grain type.
    /// </summary>
    public static KeyValuePair<string, object?> GetGrainTypeTag(Type grainType)
    {
        return _grainTypeTags.GetOrAdd(grainType, static t =>
            new KeyValuePair<string, object?>("grain.type", GetGrainTypeName(t)));
    }
}
