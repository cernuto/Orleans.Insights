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
    /// Number of records to buffer before flushing to DuckDB.
    /// Higher values improve throughput but increase memory usage.
    /// Default: 1000
    /// </summary>
    public int BatchFlushThreshold { get; set; } = 1000;

    /// <summary>
    /// Capacity of the bounded channel for metrics buffering.
    /// When full, oldest items are dropped (backpressure handling).
    /// Default: 10,000 records.
    /// </summary>
    public int ChannelCapacity { get; set; } = 10_000;
}
