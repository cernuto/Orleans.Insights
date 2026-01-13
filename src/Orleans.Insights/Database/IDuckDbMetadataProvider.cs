using DuckDB.NET.Data;

namespace Orleans.Insights.Database;

/// <summary>
/// Provides metadata about DuckDB database structure.
/// Implements Interface Segregation Principle by separating metadata queries from data queries.
/// </summary>
public interface IDuckDbMetadataProvider
{
    /// <summary>
    /// Checks if a table exists in the database.
    /// </summary>
    bool TableExists(DuckDBConnection connection, string tableName);

    /// <summary>
    /// Gets all user tables in the database.
    /// </summary>
    IReadOnlyList<TableInfo> GetUserTables(DuckDBConnection connection);

    /// <summary>
    /// Gets the estimated total size of all tables in bytes.
    /// </summary>
    long GetEstimatedSizeBytes(DuckDBConnection connection);

    /// <summary>
    /// Gets the total row count across all user tables.
    /// </summary>
    long GetTotalRowCount(DuckDBConnection connection);
}

/// <summary>
/// Information about a database table.
/// </summary>
/// <param name="SchemaName">The schema name.</param>
/// <param name="TableName">The table name.</param>
/// <param name="EstimatedSize">The estimated size in bytes.</param>
public readonly record struct TableInfo(string SchemaName, string TableName, long EstimatedSize);

/// <summary>
/// Default implementation of <see cref="IDuckDbMetadataProvider"/>.
/// </summary>
public sealed class DuckDbMetadataProvider : IDuckDbMetadataProvider
{
    private readonly IDuckDbValueConverter _valueConverter;

    /// <summary>
    /// Creates a new instance with the default value converter.
    /// </summary>
    public DuckDbMetadataProvider() : this(DuckDbValueConverter.Instance)
    {
    }

    /// <summary>
    /// Creates a new instance with the specified value converter.
    /// </summary>
    public DuckDbMetadataProvider(IDuckDbValueConverter valueConverter)
    {
        _valueConverter = valueConverter;
    }

    /// <inheritdoc />
    public bool TableExists(DuckDBConnection connection, string tableName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1";
        var param = command.CreateParameter();
        param.Value = tableName;
        command.Parameters.Add(param);

        var result = command.ExecuteScalar();
        var count = _valueConverter.Convert<long>(result);
        return count > 0;
    }

    /// <inheritdoc />
    public IReadOnlyList<TableInfo> GetUserTables(DuckDBConnection connection)
    {
        var tables = new List<TableInfo>();

        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT schema_name, table_name, COALESCE(estimated_size, 0) as estimated_size
            FROM duckdb_tables()
            WHERE internal = false";

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var schemaName = reader.GetString(0);
            var tableName = reader.GetString(1);
            var estimatedSize = _valueConverter.Convert<long>(reader.GetValue(2));
            tables.Add(new TableInfo(schemaName, tableName, estimatedSize));
        }

        return tables;
    }

    /// <inheritdoc />
    public long GetEstimatedSizeBytes(DuckDBConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT COALESCE(SUM(estimated_size), 0) as total_size
            FROM duckdb_tables()
            WHERE internal = false";

        var result = command.ExecuteScalar();
        return _valueConverter.Convert<long>(result);
    }

    /// <inheritdoc />
    public long GetTotalRowCount(DuckDBConnection connection)
    {
        var tables = GetUserTables(connection);
        long totalRows = 0;

        foreach (var table in tables)
        {
            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT COUNT(*) FROM \"{table.SchemaName}\".\"{table.TableName}\"";
                var result = command.ExecuteScalar();
                totalRows += _valueConverter.Convert<long>(result);
            }
            catch
            {
                // Skip tables that can't be counted
            }
        }

        return totalRows;
    }
}
