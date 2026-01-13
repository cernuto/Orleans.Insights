using System.Diagnostics.Metrics;

namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Shared instrumentation for all grain types. Single Meter instance ensures
/// instruments are created once and captured by MeterListener at startup.
///
/// This class is used by the GlobalGrainCallFilter to record metrics
/// for all grain method calls, including third-party grains.
/// </summary>
public static class GrainInstrumentation
{
    /// <summary>
    /// Shared meter for all grain instrumentation.
    /// </summary>
    public static readonly Meter Meter = new("Orleans.Insights.Grains", "1.0.0");

    /// <summary>
    /// Counter for successful grain method calls.
    /// </summary>
    public static readonly Counter<long> CallCounter = Meter.CreateCounter<long>(
        "grain.method.calls",
        unit: "{call}",
        description: "Number of grain method calls");

    /// <summary>
    /// Counter for grain method errors.
    /// </summary>
    public static readonly Counter<long> ErrorCounter = Meter.CreateCounter<long>(
        "grain.method.errors",
        unit: "{error}",
        description: "Number of grain method errors");

    /// <summary>
    /// Histogram for grain method call durations.
    /// </summary>
    public static readonly Histogram<double> DurationHistogram = Meter.CreateHistogram<double>(
        "grain.method.duration",
        unit: "ms",
        description: "Duration of grain method calls in milliseconds");
}
