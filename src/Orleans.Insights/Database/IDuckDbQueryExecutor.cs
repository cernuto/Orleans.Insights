using DuckDB.NET.Data;
using System.Buffers;
using System.Data;
using System.Numerics;
using System.Text;
using System.Text.Json;

namespace Orleans.Insights.Database;

/// <summary>
/// Handler interface for streaming query results.
/// Implementations process rows one at a time without materializing the full result set.
/// </summary>
/// <typeparam name="TResult">The type of result produced after processing all rows.</typeparam>
public interface IQueryResultHandler<TResult>
{
    /// <summary>
    /// Called once before any rows are processed with column metadata.
    /// </summary>
    /// <param name="columnNames">Array of column names in result order.</param>
    /// <param name="fieldCount">Number of fields per row.</param>
    void OnStart(string[] columnNames, int fieldCount);

    /// <summary>
    /// Called for each row in the result set.
    /// </summary>
    /// <param name="reader">The data reader positioned at the current row.</param>
    void OnRow(IDataReader reader);

    /// <summary>
    /// Called after all rows have been processed. Returns the final result.
    /// </summary>
    /// <param name="rowCount">Total number of rows processed.</param>
    /// <returns>The accumulated result.</returns>
    TResult OnComplete(int rowCount);
}

/// <summary>
/// Executes SQL queries against DuckDB.
/// Implements Interface Segregation Principle by separating query execution from connection management.
/// </summary>
public interface IDuckDbQueryExecutor
{
    /// <summary>
    /// Executes a query and returns results as a list of dictionaries.
    /// </summary>
    List<Dictionary<string, object?>> ExecuteQuery(DuckDBCommand command);

    /// <summary>
    /// Executes a query and streams results directly to JSON, avoiding intermediate dictionary allocations.
    /// More efficient for large result sets that will be serialized to JSON.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="rowCount">Output parameter with the number of rows returned.</param>
    /// <returns>JSON array string of results.</returns>
    string ExecuteQueryToJson(DuckDBCommand command, out int rowCount);

    /// <summary>
    /// Executes a command and returns the number of affected rows.
    /// </summary>
    int ExecuteNonQuery(DuckDBCommand command);

    /// <summary>
    /// Executes a command and returns a scalar value.
    /// </summary>
    T? ExecuteScalar<T>(DuckDBCommand command);

    /// <summary>
    /// Adds parameters to a command.
    /// </summary>
    void AddParameters(DuckDBCommand command, ReadOnlySpan<object> parameters);

    /// <summary>
    /// Executes a query and streams results directly to CSV format.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="rowCount">Output parameter with the number of rows returned.</param>
    /// <param name="includeHeader">Whether to include column headers as the first row.</param>
    /// <returns>CSV formatted string.</returns>
    string ExecuteQueryToCsv(DuckDBCommand command, out int rowCount, bool includeHeader = true);

    /// <summary>
    /// Executes a query and streams results through a handler for custom processing.
    /// This is the most flexible streaming method - JSON, CSV, and other formats can be implemented as handlers.
    /// </summary>
    /// <typeparam name="TResult">The type of result produced by the handler.</typeparam>
    /// <param name="command">The command to execute.</param>
    /// <param name="handler">The handler that processes each row and produces the result.</param>
    /// <param name="rowCount">Output parameter with the number of rows returned.</param>
    /// <returns>The result produced by the handler.</returns>
    TResult ExecuteStreaming<TResult>(DuckDBCommand command, IQueryResultHandler<TResult> handler, out int rowCount);
}

/// <summary>
/// Default implementation of <see cref="IDuckDbQueryExecutor"/>.
/// </summary>
public sealed class DuckDbQueryExecutor : IDuckDbQueryExecutor
{
    private readonly IDuckDbValueConverter _valueConverter;

    /// <summary>
    /// Creates a new instance with the default value converter.
    /// </summary>
    public DuckDbQueryExecutor() : this(DuckDbValueConverter.Instance)
    {
    }

    /// <summary>
    /// Creates a new instance with the specified value converter.
    /// </summary>
    public DuckDbQueryExecutor(IDuckDbValueConverter valueConverter)
    {
        _valueConverter = valueConverter ?? throw new ArgumentNullException(nameof(valueConverter));
    }

    /// <inheritdoc />
    public List<Dictionary<string, object?>> ExecuteQuery(DuckDBCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);

        var results = new List<Dictionary<string, object?>>();
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            var row = new Dictionary<string, object?>(reader.FieldCount);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                row[name] = value;
            }
            results.Add(row);
        }

        return results;
    }

    /// <inheritdoc />
    public string ExecuteQueryToJson(DuckDBCommand command, out int rowCount)
    {
        return ExecuteStreaming(command, new JsonResultHandler(), out rowCount);
    }

    /// <inheritdoc />
    public int ExecuteNonQuery(DuckDBCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        return command.ExecuteNonQuery();
    }

    /// <inheritdoc />
    public T? ExecuteScalar<T>(DuckDBCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);

        var result = command.ExecuteScalar();
        return _valueConverter.Convert<T>(result);
    }

    /// <inheritdoc />
    public void AddParameters(DuckDBCommand command, ReadOnlySpan<object> parameters)
    {
        ArgumentNullException.ThrowIfNull(command);

        foreach (var param in parameters)
        {
            var dbParam = command.CreateParameter();
            dbParam.Value = param ?? DBNull.Value;
            command.Parameters.Add(dbParam);
        }
    }

    /// <inheritdoc />
    public string ExecuteQueryToCsv(DuckDBCommand command, out int rowCount, bool includeHeader = true)
    {
        return ExecuteStreaming(command, new CsvResultHandler(includeHeader), out rowCount);
    }

    /// <inheritdoc />
    public TResult ExecuteStreaming<TResult>(DuckDBCommand command, IQueryResultHandler<TResult> handler, out int rowCount)
    {
        ArgumentNullException.ThrowIfNull(command);
        ArgumentNullException.ThrowIfNull(handler);

        rowCount = 0;
        using var reader = command.ExecuteReader();

        var fieldCount = reader.FieldCount;
        var columnNames = new string[fieldCount];
        for (var i = 0; i < fieldCount; i++)
        {
            columnNames[i] = reader.GetName(i);
        }

        handler.OnStart(columnNames, fieldCount);

        while (reader.Read())
        {
            rowCount++;
            handler.OnRow(reader);
        }

        return handler.OnComplete(rowCount);
    }
}

/// <summary>
/// Streams query results directly to JSON format.
/// </summary>
public sealed class JsonResultHandler : IQueryResultHandler<string>, IDisposable
{
    private const int InitialBufferSize = 4096;
    private ArrayBufferWriter<byte>? _bufferWriter;
    private Utf8JsonWriter? _writer;
    private string[]? _columnNames;
    private int _fieldCount;
    private bool _disposed;

    /// <inheritdoc />
    public void OnStart(string[] columnNames, int fieldCount)
    {
        _columnNames = columnNames;
        _fieldCount = fieldCount;
        _bufferWriter = new ArrayBufferWriter<byte>(InitialBufferSize);
        _writer = new Utf8JsonWriter(_bufferWriter);
        _writer.WriteStartArray();
    }

    /// <inheritdoc />
    public void OnRow(IDataReader reader)
    {
        if (_writer == null || _columnNames == null) return;

        _writer.WriteStartObject();
        for (var i = 0; i < _fieldCount; i++)
        {
            _writer.WritePropertyName(_columnNames[i]);
            if (reader.IsDBNull(i))
            {
                _writer.WriteNullValue();
            }
            else
            {
                WriteJsonValue(_writer, reader.GetValue(i));
            }
        }
        _writer.WriteEndObject();
    }

    /// <inheritdoc />
    public string OnComplete(int rowCount)
    {
        if (_writer == null || _bufferWriter == null)
            return "[]";

        _writer.WriteEndArray();
        _writer.Flush();
        var result = Encoding.UTF8.GetString(_bufferWriter.WrittenSpan);
        Dispose();
        return result;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _writer?.Dispose();
        _writer = null;
        _bufferWriter = null;
    }

    private static void WriteJsonValue(Utf8JsonWriter writer, object value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case byte n:
                writer.WriteNumberValue(n);
                break;
            case sbyte n:
                writer.WriteNumberValue(n);
                break;
            case short n:
                writer.WriteNumberValue(n);
                break;
            case ushort n:
                writer.WriteNumberValue(n);
                break;
            case int n:
                writer.WriteNumberValue(n);
                break;
            case uint n:
                writer.WriteNumberValue(n);
                break;
            case long n:
                writer.WriteNumberValue(n);
                break;
            case ulong n:
                writer.WriteNumberValue(n);
                break;
            case float n:
                writer.WriteNumberValue(n);
                break;
            case double n:
                writer.WriteNumberValue(n);
                break;
            case decimal n:
                writer.WriteNumberValue(n);
                break;
            case BigInteger bigInt:
                if (bigInt >= long.MinValue && bigInt <= long.MaxValue)
                    writer.WriteNumberValue((long)bigInt);
                else
                    writer.WriteStringValue(bigInt.ToString());
                break;
            case DateTime dt:
                writer.WriteStringValue(dt.ToString("O"));
                break;
            case DateTimeOffset dto:
                writer.WriteStringValue(dto.ToString("O"));
                break;
            case DateOnly d:
                writer.WriteStringValue(d.ToString("O"));
                break;
            case TimeOnly t:
                writer.WriteStringValue(t.ToString("O"));
                break;
            case TimeSpan ts:
                writer.WriteStringValue(ts.ToString());
                break;
            case Guid g:
                writer.WriteStringValue(g.ToString());
                break;
            case byte[] bytes:
                writer.WriteBase64StringValue(bytes);
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            default:
                writer.WriteStringValue(value.ToString());
                break;
        }
    }
}

/// <summary>
/// Streams query results directly to CSV format.
/// </summary>
public sealed class CsvResultHandler : IQueryResultHandler<string>
{
    private const int InitialBufferSize = 4096;
    private readonly bool _includeHeader;
    private StringBuilder? _sb;
    private int _fieldCount;

    /// <summary>
    /// Creates a CSV result handler.
    /// </summary>
    /// <param name="includeHeader">Whether to include column headers as the first row.</param>
    public CsvResultHandler(bool includeHeader = true)
    {
        _includeHeader = includeHeader;
    }

    /// <inheritdoc />
    public void OnStart(string[] columnNames, int fieldCount)
    {
        _fieldCount = fieldCount;
        _sb = new StringBuilder(InitialBufferSize);

        if (_includeHeader)
        {
            for (var i = 0; i < fieldCount; i++)
            {
                if (i > 0) _sb.Append(',');
                WriteCsvValue(_sb, columnNames[i]);
            }
            _sb.AppendLine();
        }
    }

    /// <inheritdoc />
    public void OnRow(IDataReader reader)
    {
        if (_sb == null) return;

        for (var i = 0; i < _fieldCount; i++)
        {
            if (i > 0) _sb.Append(',');
            if (!reader.IsDBNull(i))
            {
                WriteCsvValue(_sb, FormatCsvValue(reader.GetValue(i)));
            }
        }
        _sb.AppendLine();
    }

    /// <inheritdoc />
    public string OnComplete(int rowCount)
    {
        return _sb?.ToString() ?? string.Empty;
    }

    private static string FormatCsvValue(object value)
    {
        return value switch
        {
            DateTime dt => dt.ToString("O"),
            DateTimeOffset dto => dto.ToString("O"),
            DateOnly d => d.ToString("O"),
            TimeOnly t => t.ToString("O"),
            bool b => b ? "true" : "false",
            byte[] bytes => Convert.ToBase64String(bytes),
            _ => value.ToString() ?? string.Empty
        };
    }

    private static void WriteCsvValue(StringBuilder sb, string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
        {
            sb.Append('"');
            sb.Append(value.Replace("\"", "\"\""));
            sb.Append('"');
        }
        else
        {
            sb.Append(value);
        }
    }
}

/// <summary>
/// Streams query results directly to a DataTable.
/// The returned DataTable is owned by the caller and should be disposed when no longer needed.
/// </summary>
#pragma warning disable CA1001 // Type owns disposable field but transfers ownership via OnComplete
public sealed class DataTableResultHandler : IQueryResultHandler<DataTable>
#pragma warning restore CA1001
{
    private readonly string _tableName;
    private DataTable? _table;
    private int _fieldCount;

    /// <summary>
    /// Creates a DataTable result handler.
    /// </summary>
    /// <param name="tableName">Name for the resulting DataTable.</param>
    public DataTableResultHandler(string tableName = "Results")
    {
        _tableName = tableName;
    }

    /// <inheritdoc />
    public void OnStart(string[] columnNames, int fieldCount)
    {
        _fieldCount = fieldCount;
        _table = new DataTable(_tableName);

        // Add columns - we'll infer types from first row values
        foreach (var name in columnNames)
        {
            _table.Columns.Add(name, typeof(object));
        }
    }

    /// <inheritdoc />
    public void OnRow(IDataReader reader)
    {
        if (_table == null) return;

        var row = _table.NewRow();
        for (var i = 0; i < _fieldCount; i++)
        {
            row[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
        }
        _table.Rows.Add(row);
    }

    /// <inheritdoc />
    public DataTable OnComplete(int rowCount)
    {
        return _table ?? new DataTable(_tableName);
    }
}
