namespace Orleans.Insights.Database;

/// <summary>
/// Interface for DuckDB database operations in Orleans Insights.
/// </summary>
public interface IInsightsDatabase : IDisposable
{
    /// <summary>
    /// Executes a SQL command that does not return results.
    /// </summary>
    void Execute(string sql);

    /// <summary>
    /// Executes a parameterized SQL command that does not return results.
    /// </summary>
    void Execute(string sql, params object[] parameters);

    /// <summary>
    /// Executes a SQL query and returns results as JSON array string.
    /// </summary>
    string QueryJson(string sql);

    /// <summary>
    /// Executes a parameterized SQL query and returns results as JSON array string.
    /// </summary>
    string QueryJson(string sql, params object[] parameters);

    /// <summary>
    /// Executes a SQL query and streams results directly to JSON, avoiding intermediate dictionary allocations.
    /// More efficient for large result sets. Use this instead of QueryJson for change detection queries.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="rowCount">Output parameter with the number of rows returned.</param>
    /// <returns>JSON array string of results.</returns>
    string QueryJsonDirect(string sql, out int rowCount);

    /// <summary>
    /// Executes a SQL query and streams results directly to CSV format.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="rowCount">Output parameter with the number of rows returned.</param>
    /// <param name="includeHeader">Whether to include column headers as the first row.</param>
    /// <returns>CSV formatted string.</returns>
    string QueryCsv(string sql, out int rowCount, bool includeHeader = true);

    /// <summary>
    /// Executes a SQL query and streams results directly to a DataTable.
    /// More efficient than Query() followed by manual DataTable conversion.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="tableName">Name for the resulting DataTable.</param>
    /// <returns>DataTable with query results.</returns>
    System.Data.DataTable QueryDataTable(string sql, string tableName = "Results");

    /// <summary>
    /// Executes a SQL query and returns results as a list of dictionaries.
    /// </summary>
    List<Dictionary<string, object?>> Query(string sql);

    /// <summary>
    /// Executes a parameterized SQL query and returns results as a list of dictionaries.
    /// </summary>
    List<Dictionary<string, object?>> Query(string sql, params object[] parameters);

    /// <summary>
    /// Executes a SQL query and returns a scalar value.
    /// </summary>
    T? QueryScalar<T>(string sql);

    /// <summary>
    /// Removes data older than the retention period for the specified table.
    /// </summary>
    void ApplyRetention(string tableName, string timestampColumn, TimeSpan retention);

    /// <summary>
    /// Runs VACUUM to reclaim space and optimize the database.
    /// </summary>
    void Vacuum();

    /// <summary>
    /// Checks if a table exists.
    /// </summary>
    bool TableExists(string tableName);

    /// <summary>
    /// Gets the metrics tracker for this database.
    /// </summary>
    DatabaseMetrics Metrics { get; }

    /// <summary>
    /// Gets the estimated size of all tables in bytes.
    /// </summary>
    long GetEstimatedSizeBytes();

    /// <summary>
    /// Gets the total row count across all tables.
    /// </summary>
    long GetTotalRowCount();

    /// <summary>
    /// Updates size metrics (estimated size and row count).
    /// </summary>
    void UpdateSizeMetrics();
}
