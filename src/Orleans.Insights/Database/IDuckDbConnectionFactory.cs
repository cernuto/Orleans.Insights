using DuckDB.NET.Data;

namespace Orleans.Insights.Database;

/// <summary>
/// Factory for creating DuckDB connections.
/// Supports Dependency Inversion Principle by abstracting connection creation.
/// </summary>
public interface IDuckDbConnectionFactory
{
    /// <summary>
    /// Creates and opens a new DuckDB connection.
    /// </summary>
    /// <param name="connectionString">The connection string (e.g., "Data Source=:memory:").</param>
    /// <returns>An open DuckDB connection.</returns>
    DuckDBConnection CreateConnection(string connectionString);
}

/// <summary>
/// Default implementation of <see cref="IDuckDbConnectionFactory"/>.
/// </summary>
public sealed class DuckDbConnectionFactory : IDuckDbConnectionFactory
{
    /// <inheritdoc />
    public DuckDBConnection CreateConnection(string connectionString)
    {
        var connection = new DuckDBConnection(connectionString);
        connection.Open();
        return connection;
    }
}
