namespace Orleans.Insights.Dashboard;

/// <summary>
/// Centralized constants for dashboard configuration.
/// </summary>
public static class DashboardConstants
{
    /// <summary>
    /// SignalR hub route path.
    /// </summary>
    public const string HubRoute = "/insights-hub";

    /// <summary>
    /// Dashboard route prefix.
    /// </summary>
    public const string DashboardRoute = "/insights";

    /// <summary>
    /// Default polling interval for metrics collection (milliseconds).
    /// </summary>
    public const int DefaultPollingIntervalMs = 1000;

    /// <summary>
    /// Default broadcast interval for real-time updates (milliseconds).
    /// </summary>
    public const int DefaultBroadcastIntervalMs = 1000;

    /// <summary>
    /// Slow broadcast interval for less critical data (milliseconds).
    /// </summary>
    public const int SlowBroadcastIntervalMs = 5000;

    /// <summary>
    /// Default heartbeat interval for SignalR connections (milliseconds).
    /// </summary>
    public const int DefaultHeartbeatIntervalMs = 15000;

    /// <summary>
    /// Default timeout for SignalR server (milliseconds).
    /// </summary>
    public const int DefaultServerTimeoutMs = 30000;

    /// <summary>
    /// Maximum metrics sample limit for rolling windows.
    /// </summary>
    public const int MaxMetricsSampleLimit = 100;

    /// <summary>
    /// Maximum method profile sample limit (2 minutes at 1 sample/sec).
    /// </summary>
    public const int MaxMethodProfileSampleLimit = 120;

    /// <summary>
    /// Default log buffer size for log aggregation.
    /// </summary>
    public const int DefaultLogBufferSize = 1000;

    /// <summary>
    /// Page cache TTL for overview/orleans pages (milliseconds).
    /// </summary>
    public const int PageCacheTtlMs = 500;

    /// <summary>
    /// SignalR group names for page subscriptions.
    /// </summary>
    public static class Groups
    {
        public const string Overview = "overview";
        public const string Orleans = "orleans";
        public const string Insights = "insights";
        public const string Settings = "settings";
    }

    /// <summary>
    /// SignalR method names for client notifications.
    /// </summary>
    public static class Methods
    {
        public const string ReceiveOverviewData = "ReceiveOverviewData";
        public const string ReceiveOrleansData = "ReceiveOrleansData";
        public const string ReceiveInsightsData = "ReceiveInsightsData";
        public const string ReceiveSettingsData = "ReceiveSettingsData";
    }
}
