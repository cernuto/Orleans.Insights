using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Insights.Database;

/// <summary>
/// Tracks query execution times and database size metrics for Orleans Insights analytics.
/// Integrates with OpenTelemetry when a meter factory is configured.
/// </summary>
/// <remarks>
/// Uses <see cref="TimeProvider"/> for testability (.NET 8+ best practice).
/// </remarks>
public sealed class DatabaseMetrics
{
    private readonly ConcurrentDictionary<string, QueryMetrics> _queryMetrics = new();
    private readonly DatabaseTelemetry? _telemetry;
    private readonly TimeProvider _timeProvider;
    private long _estimatedSizeBytes;
    private long _rowCount;
    private DateTimeOffset _lastSizeUpdate = DateTimeOffset.MinValue;

    /// <summary>
    /// Creates a DatabaseMetrics instance without OpenTelemetry integration.
    /// </summary>
    public DatabaseMetrics() : this(TimeProvider.System)
    {
    }

    /// <summary>
    /// Creates a DatabaseMetrics instance with a custom time provider.
    /// </summary>
    public DatabaseMetrics(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _telemetry = null;
    }

    /// <summary>
    /// Creates a DatabaseMetrics instance with OpenTelemetry integration.
    /// </summary>
    public DatabaseMetrics(IMeterFactory meterFactory, string grainKey)
        : this(meterFactory, grainKey, TimeProvider.System)
    {
    }

    /// <summary>
    /// Creates a DatabaseMetrics instance with OpenTelemetry and custom time provider.
    /// </summary>
    public DatabaseMetrics(IMeterFactory meterFactory, string grainKey, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        ArgumentNullException.ThrowIfNull(grainKey);

        _timeProvider = timeProvider ?? TimeProvider.System;
        _telemetry = new DatabaseTelemetry(meterFactory, grainKey);
    }

    /// <summary>
    /// Gets the estimated database size in bytes.
    /// </summary>
    public long EstimatedSizeBytes => _estimatedSizeBytes;

    /// <summary>
    /// Gets the total row count across all tables.
    /// </summary>
    public long RowCount => _rowCount;

    /// <summary>
    /// Gets when the size was last updated.
    /// </summary>
    public DateTimeOffset LastSizeUpdate => _lastSizeUpdate;

    /// <summary>
    /// Records a query execution.
    /// </summary>
    public void RecordQueryExecution(string queryName, TimeSpan duration, int rowCount)
    {
        var metrics = _queryMetrics.GetOrAdd(queryName, name => new QueryMetrics(name, _timeProvider));
        metrics.Record(duration, rowCount);

        _telemetry?.RecordQueryExecution(queryName, duration, rowCount);
    }

    /// <summary>
    /// Updates the database size metrics.
    /// </summary>
    public void UpdateSizeMetrics(long estimatedSizeBytes, long rowCount)
    {
        _estimatedSizeBytes = estimatedSizeBytes;
        _rowCount = rowCount;
        _lastSizeUpdate = _timeProvider.GetUtcNow();

        _telemetry?.UpdateSizeMetrics(estimatedSizeBytes, rowCount);
    }

    /// <summary>
    /// Gets metrics for a specific query.
    /// </summary>
    public QueryMetrics? GetQueryMetrics(string queryName)
    {
        return _queryMetrics.TryGetValue(queryName, out var metrics) ? metrics : null;
    }

    /// <summary>
    /// Gets all query metrics.
    /// </summary>
    public IReadOnlyDictionary<string, QueryMetrics> GetAllQueryMetrics() => _queryMetrics;

    /// <summary>
    /// Gets a summary of all metrics.
    /// </summary>
    public DatabaseMetricsSummary GetSummary()
    {
        var queryStats = _queryMetrics.Values
            .Select(m => m.GetSnapshot())
            .ToList();

        return new DatabaseMetricsSummary
        {
            EstimatedSizeBytes = _estimatedSizeBytes,
            RowCount = _rowCount,
            LastSizeUpdate = _lastSizeUpdate.UtcDateTime,
            QueryStats = queryStats
        };
    }

    /// <summary>
    /// Resets all metrics.
    /// </summary>
    public void Reset()
    {
        _queryMetrics.Clear();
        _estimatedSizeBytes = 0;
        _rowCount = 0;
        _lastSizeUpdate = DateTimeOffset.MinValue;
    }
}

/// <summary>
/// Tracks metrics for a single query.
/// </summary>
/// <remarks>
/// Thread-safe using lock-based synchronization.
/// Uses <see cref="TimeProvider"/> for testability.
/// </remarks>
public sealed class QueryMetrics
{
    private readonly Lock _lock = new();
    private readonly string _queryName;
    private readonly TimeProvider _timeProvider;
    private long _executionCount;
    private long _totalDurationMs;
    private long _minDurationMs = long.MaxValue;
    private long _maxDurationMs;
    private long _totalRowsReturned;
    private DateTimeOffset _lastExecutionTime;

    /// <summary>
    /// Creates a new QueryMetrics instance.
    /// </summary>
    public QueryMetrics(string queryName) : this(queryName, TimeProvider.System)
    {
    }

    /// <summary>
    /// Creates a new QueryMetrics instance with a custom time provider.
    /// </summary>
    public QueryMetrics(string queryName, TimeProvider timeProvider)
    {
        _queryName = queryName;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// Records a query execution.
    /// </summary>
    public void Record(TimeSpan duration, int rowCount)
    {
        var durationMs = (long)duration.TotalMilliseconds;

        lock (_lock)
        {
            _executionCount++;
            _totalDurationMs += durationMs;
            _totalRowsReturned += rowCount;
            _lastExecutionTime = _timeProvider.GetUtcNow();

            if (durationMs < _minDurationMs)
                _minDurationMs = durationMs;
            if (durationMs > _maxDurationMs)
                _maxDurationMs = durationMs;
        }
    }

    /// <summary>
    /// Gets a snapshot of the current metrics.
    /// </summary>
    public QueryMetricsSnapshot GetSnapshot()
    {
        lock (_lock)
        {
            return new QueryMetricsSnapshot
            {
                QueryName = _queryName,
                ExecutionCount = _executionCount,
                TotalDurationMs = _totalDurationMs,
                AverageDurationMs = _executionCount > 0 ? _totalDurationMs / (double)_executionCount : 0,
                MinDurationMs = _minDurationMs == long.MaxValue ? 0 : _minDurationMs,
                MaxDurationMs = _maxDurationMs,
                TotalRowsReturned = _totalRowsReturned,
                LastExecutionTime = _lastExecutionTime.UtcDateTime
            };
        }
    }
}

/// <summary>
/// A point-in-time snapshot of query metrics.
/// </summary>
public sealed record QueryMetricsSnapshot
{
    public required string QueryName { get; init; }
    public long ExecutionCount { get; init; }
    public long TotalDurationMs { get; init; }
    public double AverageDurationMs { get; init; }
    public long MinDurationMs { get; init; }
    public long MaxDurationMs { get; init; }
    public long TotalRowsReturned { get; init; }
    public DateTime LastExecutionTime { get; init; }
}

/// <summary>
/// Summary of all database metrics.
/// </summary>
public sealed record DatabaseMetricsSummary
{
    public long EstimatedSizeBytes { get; init; }
    public long RowCount { get; init; }
    public DateTime LastSizeUpdate { get; init; }
    public List<QueryMetricsSnapshot> QueryStats { get; init; } = [];

    /// <summary>
    /// Gets a human-readable size string.
    /// </summary>
    public string EstimatedSizeFormatted => DatabaseFormatting.FormatBytes(EstimatedSizeBytes);
}

/// <summary>
/// Shared formatting utilities for database analytics.
/// </summary>
public static class DatabaseFormatting
{
    private const long KB = 1024;
    private const long MB = KB * 1024;
    private const long GB = MB * 1024;

    /// <summary>
    /// Formats bytes as a human-readable string (B, KB, MB, GB).
    /// </summary>
    public static string FormatBytes(long bytes) => bytes switch
    {
        < KB => $"{bytes} B",
        < MB => $"{bytes / (double)KB:F1} KB",
        < GB => $"{bytes / (double)MB:F1} MB",
        _ => $"{bytes / (double)GB:F2} GB"
    };
}

/// <summary>
/// OpenTelemetry integration for database metrics.
/// </summary>
internal sealed class DatabaseTelemetry
{
    private readonly Histogram<double> _queryDurationHistogram;
    private readonly Histogram<long> _queryRowsHistogram;
    private readonly Counter<long> _queryExecutionCounter;
    private readonly string _grainKey;
    private long _estimatedSizeBytes;
    private long _rowCount;

    public DatabaseTelemetry(IMeterFactory meterFactory, string grainKey)
    {
        _grainKey = grainKey;
        var meter = meterFactory.Create("Orleans.Insights.Database");

        _queryDurationHistogram = meter.CreateHistogram<double>(
            "insights.query.duration",
            unit: "ms",
            description: "Query execution duration in milliseconds");

        _queryRowsHistogram = meter.CreateHistogram<long>(
            "insights.query.rows",
            unit: "rows",
            description: "Number of rows returned by queries");

        _queryExecutionCounter = meter.CreateCounter<long>(
            "insights.query.executions",
            unit: "executions",
            description: "Total number of query executions");

        meter.CreateObservableGauge(
            "insights.database.size_bytes",
            () => _estimatedSizeBytes,
            unit: "By",
            description: "Estimated database size in bytes");

        meter.CreateObservableGauge(
            "insights.database.row_count",
            () => _rowCount,
            unit: "rows",
            description: "Total row count across all tables");
    }

    public void RecordQueryExecution(string queryName, TimeSpan duration, int rowCount)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("query.name", queryName),
            new("grain.key", _grainKey)
        };

        _queryDurationHistogram.Record(duration.TotalMilliseconds, tags);
        _queryRowsHistogram.Record(rowCount, tags);
        _queryExecutionCounter.Add(1, tags);
    }

    public void UpdateSizeMetrics(long estimatedSizeBytes, long rowCount)
    {
        _estimatedSizeBytes = estimatedSizeBytes;
        _rowCount = rowCount;
    }
}
