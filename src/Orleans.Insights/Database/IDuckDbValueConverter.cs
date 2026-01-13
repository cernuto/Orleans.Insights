using System.Numerics;

namespace Orleans.Insights.Database;

/// <summary>
/// Converts DuckDB result values to .NET types.
/// Implements Single Responsibility Principle by isolating type conversion logic.
/// </summary>
public interface IDuckDbValueConverter
{
    /// <summary>
    /// Converts a DuckDB result value to the specified type.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="value">The value from DuckDB.</param>
    /// <returns>The converted value, or default if null/DBNull.</returns>
    T? Convert<T>(object? value);
}

/// <summary>
/// Default implementation of <see cref="IDuckDbValueConverter"/>.
/// Handles DuckDB-specific types like BigInteger that don't implement IConvertible.
/// </summary>
public sealed class DuckDbValueConverter : IDuckDbValueConverter
{
    /// <summary>
    /// Shared singleton instance for performance.
    /// </summary>
    public static readonly DuckDbValueConverter Instance = new();

    /// <inheritdoc />
    public T? Convert<T>(object? value)
    {
        if (value is null || value == DBNull.Value)
            return default;

        var targetType = typeof(T);
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Fast path: already the correct type
        if (value is T typedValue)
            return typedValue;

        // Handle IConvertible types directly
        if (value is IConvertible convertible)
        {
            return (T)System.Convert.ChangeType(convertible, underlyingType);
        }

        // Handle BigInteger (common in DuckDB for large numbers)
        if (value is BigInteger bigInt)
        {
            return ConvertBigInteger<T>(bigInt, underlyingType);
        }

        // Fallback: parse from string representation
        var stringValue = value.ToString();
        if (string.IsNullOrEmpty(stringValue))
            return default;

        return (T)System.Convert.ChangeType(stringValue, underlyingType);
    }

    private static T ConvertBigInteger<T>(BigInteger bigInt, Type underlyingType)
    {
        if (underlyingType == typeof(long))
            return (T)(object)(long)bigInt;
        if (underlyingType == typeof(int))
            return (T)(object)(int)bigInt;
        if (underlyingType == typeof(short))
            return (T)(object)(short)bigInt;
        if (underlyingType == typeof(byte))
            return (T)(object)(byte)bigInt;
        if (underlyingType == typeof(ulong))
            return (T)(object)(ulong)bigInt;
        if (underlyingType == typeof(uint))
            return (T)(object)(uint)bigInt;
        if (underlyingType == typeof(ushort))
            return (T)(object)(ushort)bigInt;
        if (underlyingType == typeof(decimal))
            return (T)(object)(decimal)bigInt;
        if (underlyingType == typeof(double))
            return (T)(object)(double)bigInt;
        if (underlyingType == typeof(float))
            return (T)(object)(float)bigInt;

        // Fallback to string parsing
        return (T)System.Convert.ChangeType(bigInt.ToString(), underlyingType);
    }
}
