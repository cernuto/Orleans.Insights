namespace Orleans.Insights.Dashboard;

/// <summary>
/// Options for configuring the dashboard observer behavior.
/// Controls health tracking, circuit breaker, and grace period buffering.
/// </summary>
public class DashboardObserverOptions
{
    /// <summary>
    /// Number of failures before marking an observer as dead.
    /// Default: 5.
    /// </summary>
    public int ObserverFailureThreshold { get; set; } = 5;

    /// <summary>
    /// Time window for counting failures. Failures outside this window are pruned.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan ObserverFailureWindow { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Enable circuit breaker pattern for observer health.
    /// When enabled, repeated failures will open the circuit, preventing dispatch
    /// until the circuit transitions to half-open and a successful delivery occurs.
    /// Default: true.
    /// </summary>
    public bool EnableCircuitBreaker { get; set; } = true;

    /// <summary>
    /// How long the circuit stays open before transitioning to half-open.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan CircuitBreakerOpenDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Interval between half-open test requests.
    /// Default: 5 seconds.
    /// </summary>
    public TimeSpan CircuitBreakerHalfOpenTestInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Grace period for buffering messages during temporary disconnects.
    /// When an observer's circuit opens, messages are buffered during this period.
    /// If the observer recovers, buffered messages are replayed.
    /// Set to Zero to disable buffering.
    /// Default: 10 seconds.
    /// </summary>
    public TimeSpan ObserverGracePeriod { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Maximum messages to buffer per observer during grace period.
    /// Older messages are dropped when this limit is exceeded.
    /// Default: 50.
    /// </summary>
    public int MaxBufferedMessagesPerObserver { get; set; } = 50;

    /// <summary>
    /// Observer subscription expiration timeout.
    /// Observers that haven't been refreshed within this period are considered stale.
    /// Default: 2 minutes.
    /// </summary>
    public TimeSpan ObserverExpiration { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Interval for refreshing observer subscriptions (heartbeat).
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan ObserverRefreshInterval { get; set; } = TimeSpan.FromSeconds(30);
}
