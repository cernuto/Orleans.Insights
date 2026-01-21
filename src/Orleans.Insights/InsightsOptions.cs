namespace Orleans.Insights;

/// <summary>
/// Configuration options for Orleans.Insights.
/// </summary>
public class InsightsOptions
{
    /// <summary>
    /// How long to retain historical metrics data.
    /// Default: 1 hour. Can be extended for deeper historical analysis.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// How often to run maintenance tasks (retention cleanup, vacuum).
    /// </summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How often to run vacuum/checkpoint operations.
    /// </summary>
    public TimeSpan VacuumInterval { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// How often to log database metrics (size, row count).
    /// </summary>
    public TimeSpan MetricsLogInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Default duration for queries when not specified.
    /// </summary>
    public TimeSpan DefaultQueryDuration { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Default bucket size for time-series aggregations (in seconds).
    /// </summary>
    public int DefaultBucketSeconds { get; set; } = 60;

    /// <summary>
    /// Maximum number of rows to return from custom queries.
    /// </summary>
    public int MaxQueryResults { get; set; } = 10000;

    /// <summary>
    /// Database size threshold (bytes) above which a warning is logged.
    /// Default: 100 MB
    /// </summary>
    public long DatabaseSizeWarningBytes { get; set; } = 100_000_000;

    /// <summary>
    /// Number of records to buffer before flushing to DuckDB.
    /// Higher values improve throughput but increase memory usage.
    /// Default: 1000
    /// </summary>
    public int BatchFlushThreshold { get; set; } = 1000;

    /// <summary>
    /// Maximum time to buffer records before flushing.
    /// Ensures data is persisted even with low throughput.
    /// Also controls the idle timeout for the background consumer task.
    /// Default: 1 second (reduced for faster metrics visibility)
    /// </summary>
    public TimeSpan BatchFlushInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// How often to broadcast dashboard updates to connected clients.
    /// Default: 1 second for real-time feel.
    /// </summary>
    public TimeSpan BroadcastInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Slow broadcast interval for less frequently updated data (e.g., Insights page).
    /// Default: 5 seconds.
    /// </summary>
    public TimeSpan SlowBroadcastInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of metrics samples to retain per grain type for rolling windows.
    /// Default: 100 samples (approximately 100 seconds of data at 1 sample/second).
    /// </summary>
    public int MetricsSampleLimit { get; set; } = 100;

    /// <summary>
    /// Maximum number of method profile samples to retain for time-series charts.
    /// Default: 120 samples (2 minutes of data at 1 sample/second).
    /// </summary>
    public int MethodProfileSampleLimit { get; set; } = 120;

    /// <summary>
    /// Capacity of the bounded channel for metrics buffering.
    /// When full, oldest items are dropped (backpressure handling).
    /// Default: 10,000 records.
    /// </summary>
    public int ChannelCapacity { get; set; } = 10_000;

    /// <summary>
    /// Maximum number of grain types/methods to track before LRU eviction.
    /// Prevents unbounded memory growth in long-running silos with many grain types.
    /// Default: 10,000 entries.
    /// </summary>
    public int MaxMetricsEntries { get; set; } = 10_000;

    /// <summary>
    /// How often to check for LRU eviction (to avoid per-request overhead).
    /// Default: 1 minute.
    /// </summary>
    public TimeSpan EvictionCheckInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Size of the log buffer for log aggregation sink.
    /// Default: 1000 entries.
    /// </summary>
    public int LogBufferSize { get; set; } = 1000;

    /// <summary>
    /// Cache TTL for page data to reduce repeated queries.
    /// Default: 500ms.
    /// </summary>
    public TimeSpan PageCacheTtl { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Whether to require authentication for the dashboard.
    /// Default: false (no auth required).
    /// </summary>
    public bool RequireAuthentication { get; set; } = false;
}
