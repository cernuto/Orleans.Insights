namespace Orleans.Insights.Instrumentation;

/// <summary>
/// Interface for grain method profiler to enable decoupling from implementation.
/// </summary>
/// <remarks>
/// This interface follows the Interface Segregation Principle - it contains only
/// the single method needed by the GlobalGrainCallFilter to record method calls.
/// </remarks>
public interface IGrainMethodProfiler
{
    /// <summary>
    /// Records a grain method call. Called from GlobalGrainCallFilter after each grain invocation.
    /// </summary>
    /// <param name="grainType">Short grain type name (e.g., "Device")</param>
    /// <param name="methodName">Method name (e.g., "GetState")</param>
    /// <param name="elapsedMs">Elapsed time in milliseconds</param>
    /// <param name="failed">Whether the call threw an exception</param>
    /// <remarks>
    /// Implementation should be thread-safe and non-blocking.
    /// Performance target: ~50-100ns overhead per call.
    /// </remarks>
    void Track(string grainType, string methodName, double elapsedMs, bool failed);
}
