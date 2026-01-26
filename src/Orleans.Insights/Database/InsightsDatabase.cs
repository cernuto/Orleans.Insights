using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Insights.Database;

/// <summary>
/// DuckDB database wrapper for Orleans Insights analytics.
/// Uses in-memory mode with MVCC support via connection duplication.
/// </summary>
/// <remarks>
/// <para>
/// <b>MVCC Architecture:</b>
/// DuckDB supports Multi-Version Concurrency Control (MVCC) which allows concurrent reads
/// and writes without blocking. This is achieved by using <see cref="DuckDBConnection.Duplicate"/>
/// to create separate connections that share the same in-memory database:
/// <list type="bullet">
/// <item><see cref="WriteConnection"/> - Primary connection for schema and bulk writes (Appender)</item>
/// <item><see cref="CreateReadConnection"/> - Creates duplicated connections for queries</item>
/// </list>
/// </para>
/// <para>
/// <b>Thread Safety:</b>
/// Each connection should only be used from a single thread at a time. The write connection
/// is used by the background consumer thread. Read connections should be created per-query
/// or pooled with proper synchronization.
/// </para>
/// <para>
/// SOLID principles applied:
/// - SRP: Query execution, value conversion, and metadata queries are delegated to separate components
/// - OCP: New behaviors can be added via new implementations of the injected interfaces
/// - LSP: All implementations are substitutable through their interfaces
/// - ISP: Separate interfaces for query execution, value conversion, and metadata
/// - DIP: Depends on abstractions (interfaces) rather than concrete implementations
/// </para>
/// </remarks>
public sealed class InsightsDatabase : IInsightsDatabase
{
    private readonly DuckDBConnection _connection;
    private readonly ILogger _logger;
    private readonly DatabaseMetrics _metrics;
    private readonly IDuckDbQueryExecutor _queryExecutor;
    private readonly IDuckDbMetadataProvider _metadataProvider;
    private readonly TimeProvider _timeProvider;
    private readonly string _databaseId;
    private readonly ConcurrentBag<DuckDBConnection> _readConnectionPool = new();
    private const int MaxPooledConnections = 4;
    private bool _disposed;

    /// <summary>
    /// Creates an InsightsDatabase without OpenTelemetry integration (backward compatible).
    /// </summary>
    public InsightsDatabase(ILogger logger, string? grainKey = null)
        : this(logger, null, new DuckDbConnectionFactory(), new DuckDbQueryExecutor(),
               new DuckDbMetadataProvider(), TimeProvider.System, grainKey)
    {
    }

    /// <summary>
    /// Creates an InsightsDatabase with OpenTelemetry integration.
    /// </summary>
    public InsightsDatabase(ILogger logger, IMeterFactory? meterFactory, string? grainKey = null)
        : this(logger, meterFactory, new DuckDbConnectionFactory(), new DuckDbQueryExecutor(),
               new DuckDbMetadataProvider(), TimeProvider.System, grainKey)
    {
    }

    /// <summary>
    /// Creates an InsightsDatabase with full dependency injection support.
    /// </summary>
    /// <param name="logger">Logger for database operations.</param>
    /// <param name="meterFactory">Optional meter factory for OpenTelemetry metrics.</param>
    /// <param name="connectionFactory">Factory for creating DuckDB connections.</param>
    /// <param name="queryExecutor">Executor for SQL queries.</param>
    /// <param name="metadataProvider">Provider for database metadata.</param>
    /// <param name="timeProvider">Time provider for timestamps (.NET 8+ best practice).</param>
    /// <param name="grainKey">Grain key for identification.</param>
    public InsightsDatabase(
        ILogger logger,
        IMeterFactory? meterFactory,
        IDuckDbConnectionFactory connectionFactory,
        IDuckDbQueryExecutor queryExecutor,
        IDuckDbMetadataProvider metadataProvider,
        TimeProvider timeProvider,
        string? grainKey = null)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(connectionFactory);
        ArgumentNullException.ThrowIfNull(queryExecutor);
        ArgumentNullException.ThrowIfNull(metadataProvider);
        ArgumentNullException.ThrowIfNull(timeProvider);

        _logger = logger;
        _queryExecutor = queryExecutor;
        _metadataProvider = metadataProvider;
        _timeProvider = timeProvider;
        _databaseId = grainKey ?? Guid.NewGuid().ToString("N");
        _metrics = meterFactory != null
            ? new DatabaseMetrics(meterFactory, _databaseId, timeProvider)
            : new DatabaseMetrics(timeProvider);

        // Use in-memory database with single-thread mode
        // Single-thread mode is optimal for Orleans grains due to their turn-based concurrency model:
        // - Each grain executes one method at a time (no concurrent access to DuckDB)
        // - Eliminates thread synchronization overhead within queries
        // - Reduces memory footprint per grain instance
        // - Parallelism occurs at grain level (many grains), not query level
        const string connectionString = "Data Source=:memory:;threads=1";
        _connection = connectionFactory.CreateConnection(connectionString);

        _logger.LogInformation("InsightsDatabase initialized: {DatabaseId}", _databaseId);
    }

    /// <inheritdoc />
    public DatabaseMetrics Metrics => _metrics;

    /// <summary>
    /// Gets the primary connection for bulk write operations (e.g., Appender).
    /// This connection should only be used by the background consumer thread.
    /// </summary>
    public DuckDBConnection WriteConnection
    {
        get
        {
            ThrowIfDisposed();
            return _connection;
        }
    }

    /// <summary>
    /// Gets a read connection from the pool or creates a new one via <see cref="DuckDBConnection.Duplicate"/>.
    /// This enables MVCC - reads can proceed concurrently with writes.
    /// </summary>
    /// <remarks>
    /// The caller should return the connection via <see cref="ReturnReadConnection"/> when done.
    /// If not returned, the connection will still work but won't be reused.
    /// </remarks>
    /// <returns>A connection sharing the same in-memory database.</returns>
    public DuckDBConnection CreateReadConnection()
    {
        ThrowIfDisposed();

        // Try to get a pooled connection first
        if (_readConnectionPool.TryTake(out var pooledConn))
        {
            return pooledConn;
        }

        // Create a new connection
        var conn = _connection.Duplicate();
        conn.Open();
        return conn;
    }

    /// <summary>
    /// Returns a read connection to the pool for reuse.
    /// If the pool is full, the connection is disposed.
    /// </summary>
    /// <param name="connection">The connection to return.</param>
    public void ReturnReadConnection(DuckDBConnection connection)
    {
        if (_disposed || _readConnectionPool.Count >= MaxPooledConnections)
        {
            connection.Dispose();
            return;
        }

        _readConnectionPool.Add(connection);
    }

    /// <inheritdoc />
    public void Execute(string sql)
    {
        ThrowIfDisposed();
        _logger.LogTrace("Execute: {Sql}", sql);

        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        var affected = _queryExecutor.ExecuteNonQuery(command);

        _logger.LogDebug("Execute affected {RowsAffected} rows", affected);
    }

    /// <inheritdoc />
    public void Execute(string sql, params object[] parameters)
    {
        ThrowIfDisposed();
        _logger.LogTrace("Execute: {Sql} [{ParamCount} params]", sql, parameters.Length);

        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        _queryExecutor.AddParameters(command, parameters);
        var affected = _queryExecutor.ExecuteNonQuery(command);

        _logger.LogDebug("Execute affected {RowsAffected} rows", affected);
    }

    /// <inheritdoc />
    public string QueryJson(string sql)
    {
        // Use streaming - no intermediate materialization
        return QueryJsonDirect(sql, out _);
    }

    /// <inheritdoc />
    public string QueryJson(string sql, params object[] parameters)
    {
        // Use streaming with parameters
        return QueryJsonDirect(sql, out _, parameters);
    }

    /// <inheritdoc />
    public string QueryJsonDirect(string sql, out int rowCount)
    {
        return QueryJsonDirect(sql, out rowCount, []);
    }

    /// <summary>
    /// Executes a parameterized SQL query and streams results directly to JSON.
    /// </summary>
    public string QueryJsonDirect(string sql, out int rowCount, params object[] parameters)
    {
        ThrowIfDisposed();
        _logger.LogTrace("QueryJsonDirect: {Sql}", sql);

        var startTime = _timeProvider.GetTimestamp();
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        _queryExecutor.AddParameters(command, parameters);
        var json = _queryExecutor.ExecuteQueryToJson(command, out rowCount);
        var elapsed = _timeProvider.GetElapsedTime(startTime);

        _metrics.RecordQueryExecution(GetQueryName(sql), elapsed, rowCount);
        _logger.LogDebug("QueryJsonDirect returned {RowCount} rows in {ElapsedMs}ms", rowCount, elapsed.TotalMilliseconds);

        return json;
    }

    /// <inheritdoc />
    public string QueryCsv(string sql, out int rowCount, bool includeHeader = true)
    {
        ThrowIfDisposed();
        _logger.LogTrace("QueryCsv: {Sql}", sql);

        var startTime = _timeProvider.GetTimestamp();
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        var csv = _queryExecutor.ExecuteQueryToCsv(command, out rowCount, includeHeader);
        var elapsed = _timeProvider.GetElapsedTime(startTime);

        _metrics.RecordQueryExecution(GetQueryName(sql), elapsed, rowCount);
        _logger.LogDebug("QueryCsv returned {RowCount} rows in {ElapsedMs}ms", rowCount, elapsed.TotalMilliseconds);

        return csv;
    }

    /// <inheritdoc />
    public System.Data.DataTable QueryDataTable(string sql, string tableName = "Results")
    {
        ThrowIfDisposed();
        _logger.LogTrace("QueryDataTable: {Sql}", sql);

        var startTime = _timeProvider.GetTimestamp();
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        var table = _queryExecutor.ExecuteStreaming(command, new DataTableResultHandler(tableName), out var rowCount);
        var elapsed = _timeProvider.GetElapsedTime(startTime);

        _metrics.RecordQueryExecution(GetQueryName(sql), elapsed, rowCount);
        _logger.LogDebug("QueryDataTable returned {RowCount} rows in {ElapsedMs}ms", rowCount, elapsed.TotalMilliseconds);

        return table;
    }

    /// <inheritdoc />
    public List<Dictionary<string, object?>> Query(string sql)
    {
        ThrowIfDisposed();
        _logger.LogTrace("Query: {Sql}", sql);

        var startTime = _timeProvider.GetTimestamp();
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        var results = _queryExecutor.ExecuteQuery(command);
        var elapsed = _timeProvider.GetElapsedTime(startTime);

        _metrics.RecordQueryExecution(GetQueryName(sql), elapsed, results.Count);
        _logger.LogDebug("Query returned {RowCount} rows in {ElapsedMs}ms", results.Count, elapsed.TotalMilliseconds);

        return results;
    }

    /// <inheritdoc />
    public List<Dictionary<string, object?>> Query(string sql, params object[] parameters)
    {
        ThrowIfDisposed();
        _logger.LogTrace("Query: {Sql} [{ParamCount} params]", sql, parameters.Length);

        var startTime = _timeProvider.GetTimestamp();
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        _queryExecutor.AddParameters(command, parameters);
        var results = _queryExecutor.ExecuteQuery(command);
        var elapsed = _timeProvider.GetElapsedTime(startTime);

        _metrics.RecordQueryExecution(GetQueryName(sql), elapsed, results.Count);
        _logger.LogDebug("Query returned {RowCount} rows in {ElapsedMs}ms", results.Count, elapsed.TotalMilliseconds);

        return results;
    }

    /// <inheritdoc />
    public T? QueryScalar<T>(string sql)
    {
        ThrowIfDisposed();
        _logger.LogTrace("QueryScalar: {Sql}", sql);

        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        return _queryExecutor.ExecuteScalar<T>(command);
    }

    /// <inheritdoc />
    public void ApplyRetention(string tableName, string timestampColumn, TimeSpan retention)
    {
        ThrowIfDisposed();
        if (!TableExists(tableName))
            return;

        _logger.LogDebug("ApplyRetention: {Table} older than {Retention}", tableName, retention);
        var cutoffTime = _timeProvider.GetUtcNow().DateTime - retention;
        var sql = $"DELETE FROM {tableName} WHERE {timestampColumn} < $1";
        Execute(sql, cutoffTime);
    }

    /// <inheritdoc />
    public void Vacuum()
    {
        ThrowIfDisposed();
        try
        {
            Execute("CHECKPOINT");
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Vacuum/Checkpoint failed");
        }
    }

    /// <inheritdoc />
    public bool TableExists(string tableName)
    {
        ThrowIfDisposed();
        return _metadataProvider.TableExists(_connection, tableName);
    }

    /// <inheritdoc />
    public long GetEstimatedSizeBytes()
    {
        ThrowIfDisposed();
        try
        {
            return _metadataProvider.GetEstimatedSizeBytes(_connection);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to get estimated size, falling back to row-based estimate");
            return GetTotalRowCount() * 100;
        }
    }

    /// <inheritdoc />
    public long GetTotalRowCount()
    {
        ThrowIfDisposed();
        try
        {
            return _metadataProvider.GetTotalRowCount(_connection);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to get total row count");
            return 0;
        }
    }

    /// <inheritdoc />
    public void UpdateSizeMetrics()
    {
        ThrowIfDisposed();
        try
        {
            var sizeBytes = GetEstimatedSizeBytes();
            var rowCount = GetTotalRowCount();
            _metrics.UpdateSizeMetrics(sizeBytes, rowCount);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to update size metrics");
        }
    }

    /// <summary>
    /// Extracts a meaningful query name from SQL for metrics tracking.
    /// </summary>
    private static string GetQueryName(string sql)
    {
        var normalized = sql.AsSpan().Trim();
        if (normalized.Length == 0)
            return "EMPTY";

        // Use span-based parsing for performance
        if (normalized.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            return ExtractTableName(sql, "FROM", "SELECT");
        }
        if (normalized.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase))
        {
            return ExtractTableName(sql, "INTO", "INSERT");
        }
        if (normalized.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase))
        {
            var afterUpdate = sql.AsSpan()[6..].Trim();
            var endIndex = afterUpdate.IndexOfAny([' ', '\r', '\n']);
            var tableName = endIndex > 0 ? afterUpdate[..endIndex] : afterUpdate;
            return $"UPDATE:{tableName.Trim().ToString()}";
        }
        if (normalized.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase))
        {
            return ExtractTableName(sql, "FROM", "DELETE");
        }

        // Fallback: use first 30 chars
        var maxLen = Math.Min(30, sql.Length);
        return sql[..maxLen].Replace('\n', ' ').Replace('\r', ' ');
    }

    private static string ExtractTableName(string sql, string keyword, string prefix)
    {
        var keywordIndex = sql.IndexOf(keyword, StringComparison.OrdinalIgnoreCase);
        if (keywordIndex <= 0)
            return prefix;

        var afterKeyword = sql.AsSpan()[(keywordIndex + keyword.Length)..].Trim();
        var endIndex = afterKeyword.IndexOfAny([' ', '\r', '\n', '(', ',']);
        var tableName = endIndex > 0 ? afterKeyword[..endIndex] : afterKeyword;
        return $"{prefix}:{tableName.Trim().ToString()}";
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Dispose all pooled read connections
        while (_readConnectionPool.TryTake(out var pooledConn))
        {
            try
            {
                pooledConn.Close();
                pooledConn.Dispose();
            }
            catch
            {
                // Ignore errors during disposal
            }
        }

        try
        {
            _connection.Close();
            _connection.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error disposing connection");
        }

        _logger.LogInformation("InsightsDatabase disposed: {DatabaseId}", _databaseId);
    }
}
