using System.Globalization;

namespace Orleans.Insights.Dashboard.Client.Utilities;

/// <summary>
/// Zero-allocation formatting utilities for dashboard display values.
/// Uses Span-based formatting to minimize heap allocations in hot paths.
/// </summary>
public static class DashboardFormatters
{
    private static readonly CultureInfo InvariantCulture = CultureInfo.InvariantCulture;
    private const string FormatF1 = "F1";
    private const string FormatF2 = "F2";
    private const string FormatN0 = "N0";

    #region Number Formatting

    /// <summary>
    /// Formats a large number with K/M/B suffix for compact display.
    /// Uses stack-allocated buffers for zero heap allocation.
    /// </summary>
    public static string FormatCompactNumber(double value)
    {
        if (value < 1_000)
        {
            return value.ToString(FormatN0, InvariantCulture);
        }

        if (value < 1_000_000)
        {
            Span<char> buffer = stackalloc char[16];
            if ((value / 1_000.0).TryFormat(buffer, out var charsWritten, FormatF1, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], "K");
            }
            return $"{value / 1_000.0:F1}K";
        }

        if (value < 1_000_000_000)
        {
            Span<char> buffer = stackalloc char[16];
            if ((value / 1_000_000.0).TryFormat(buffer, out var charsWritten, FormatF1, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], "M");
            }
            return $"{value / 1_000_000.0:F1}M";
        }

        {
            Span<char> buffer = stackalloc char[16];
            if ((value / 1_000_000_000.0).TryFormat(buffer, out var charsWritten, FormatF1, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], "B");
            }
            return $"{value / 1_000_000_000.0:F1}B";
        }
    }

    /// <summary>
    /// Formats a byte count with KB/MB/GB suffix.
    /// </summary>
    public static string FormatBytes(long bytes)
    {
        if (bytes < 1024)
        {
            return $"{bytes} B";
        }

        if (bytes < 1024 * 1024)
        {
            Span<char> buffer = stackalloc char[16];
            var kb = bytes / 1024.0;
            if (kb.TryFormat(buffer, out var charsWritten, FormatF1, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], " KB");
            }
            return $"{kb:F1} KB";
        }

        if (bytes < 1024L * 1024 * 1024)
        {
            Span<char> buffer = stackalloc char[16];
            var mb = bytes / (1024.0 * 1024);
            if (mb.TryFormat(buffer, out var charsWritten, FormatF1, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], " MB");
            }
            return $"{mb:F1} MB";
        }

        {
            Span<char> buffer = stackalloc char[16];
            var gb = bytes / (1024.0 * 1024 * 1024);
            if (gb.TryFormat(buffer, out var charsWritten, FormatF2, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], " GB");
            }
            return $"{gb:F2} GB";
        }
    }

    /// <summary>
    /// Formats a percentage value.
    /// </summary>
    public static string FormatPercent(double value)
    {
        Span<char> buffer = stackalloc char[16];
        if (value.TryFormat(buffer, out var charsWritten, FormatF1, InvariantCulture))
        {
            return string.Concat(buffer[..charsWritten], "%");
        }
        return $"{value:F1}%";
    }

    /// <summary>
    /// Formats latency in milliseconds with appropriate precision.
    /// </summary>
    public static string FormatLatencyMs(double latencyMs)
    {
        if (latencyMs < 0.01)
            return "< 0.01 ms";

        if (latencyMs < 1)
        {
            Span<char> buffer = stackalloc char[16];
            if (latencyMs.TryFormat(buffer, out var charsWritten, FormatF2, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], " ms");
            }
            return $"{latencyMs:F2} ms";
        }

        if (latencyMs < 1000)
        {
            Span<char> buffer = stackalloc char[16];
            if (latencyMs.TryFormat(buffer, out var charsWritten, FormatF1, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], " ms");
            }
            return $"{latencyMs:F1} ms";
        }

        // Convert to seconds for large values
        Span<char> secBuffer = stackalloc char[16];
        var seconds = latencyMs / 1000.0;
        if (seconds.TryFormat(secBuffer, out var secCharsWritten, FormatF2, InvariantCulture))
        {
            return string.Concat(secBuffer[..secCharsWritten], " s");
        }
        return $"{seconds:F2} s";
    }

    /// <summary>
    /// Formats throughput (requests per second).
    /// </summary>
    public static string FormatThroughput(double requestsPerSecond)
    {
        if (requestsPerSecond < 1)
        {
            Span<char> buffer = stackalloc char[16];
            if (requestsPerSecond.TryFormat(buffer, out var charsWritten, FormatF2, InvariantCulture))
            {
                return string.Concat(buffer[..charsWritten], "/s");
            }
            return $"{requestsPerSecond:F2}/s";
        }

        return string.Concat(FormatCompactNumber(requestsPerSecond), "/s");
    }

    #endregion

    #region String Formatting

    /// <summary>
    /// Extracts the short grain type name from a fully qualified type name.
    /// Uses span-based parsing for efficiency.
    /// </summary>
    /// <example>
    /// "MyApp.Grains.UserGrain, MyApp.Grains" -> "UserGrain"
    /// "MyApp.Grains.GenericGrain`1[[System.Int32]]" -> "GenericGrain&lt;Int32&gt;"
    /// </example>
    public static string FormatGrainTypeName(string? fullTypeName)
    {
        if (string.IsNullOrEmpty(fullTypeName))
            return "Unknown";

        var span = fullTypeName.AsSpan();

        // Remove assembly suffix (everything after comma)
        var commaIndex = span.IndexOf(',');
        if (commaIndex >= 0)
            span = span[..commaIndex];

        // Find the last dot to get just the type name
        var lastDotIndex = span.LastIndexOf('.');
        if (lastDotIndex >= 0)
            span = span[(lastDotIndex + 1)..];

        // Handle generic types (e.g., "GenericGrain`1[[System.Int32]]")
        var backtickIndex = span.IndexOf('`');
        if (backtickIndex >= 0)
        {
            var baseName = span[..backtickIndex];

            // Extract generic arguments
            var bracketStart = span.IndexOf('[');
            var bracketEnd = span.LastIndexOf(']');

            if (bracketStart >= 0 && bracketEnd > bracketStart)
            {
                var argsSpan = span[(bracketStart + 1)..bracketEnd];
                var genericArgs = ExtractGenericTypeNames(argsSpan);
                return $"{baseName}<{genericArgs}>";
            }

            return baseName.ToString();
        }

        return span.ToString();
    }

    /// <summary>
    /// Extracts generic type names from a generic arguments string.
    /// </summary>
    private static string ExtractGenericTypeNames(ReadOnlySpan<char> argsSpan)
    {
        // Handle nested brackets [[Type1, Assembly], [Type2, Assembly]]
        var depth = 0;
        var startIndex = 0;
        var typeNames = new List<string>();

        for (var i = 0; i < argsSpan.Length; i++)
        {
            var c = argsSpan[i];
            if (c == '[')
            {
                if (depth == 0)
                    startIndex = i + 1;
                depth++;
            }
            else if (c == ']')
            {
                depth--;
                if (depth == 0)
                {
                    var typeSpan = argsSpan[startIndex..i];
                    var commaIdx = typeSpan.IndexOf(',');
                    if (commaIdx >= 0)
                        typeSpan = typeSpan[..commaIdx];

                    var lastDot = typeSpan.LastIndexOf('.');
                    if (lastDot >= 0)
                        typeSpan = typeSpan[(lastDot + 1)..];

                    typeNames.Add(typeSpan.ToString());
                }
            }
        }

        return string.Join(", ", typeNames);
    }

    /// <summary>
    /// Extracts the short method name, removing interface prefixes.
    /// </summary>
    /// <example>
    /// "IUserGrain.GetUser" -> "GetUser"
    /// "GetUser" -> "GetUser"
    /// </example>
    public static string FormatMethodName(string? fullMethodName)
    {
        if (string.IsNullOrEmpty(fullMethodName))
            return "Unknown";

        var span = fullMethodName.AsSpan();

        // Remove interface prefix (IInterface.Method -> Method)
        var dotIndex = span.LastIndexOf('.');
        if (dotIndex >= 0)
            span = span[(dotIndex + 1)..];

        return span.ToString();
    }

    /// <summary>
    /// Extracts the silo name from a silo address.
    /// Uses span-based parsing for efficiency.
    /// </summary>
    /// <example>
    /// "S127.0.0.1:11111:12345678" -> "S127.0.0.1:11111"
    /// </example>
    public static string FormatSiloAddress(string? siloAddress)
    {
        if (string.IsNullOrEmpty(siloAddress))
            return "Unknown";

        var span = siloAddress.AsSpan();

        // Find last colon (generation separator)
        var lastColonIndex = span.LastIndexOf(':');
        if (lastColonIndex > 0)
        {
            // Check if this is the generation (all digits after the colon)
            var afterColon = span[(lastColonIndex + 1)..];
            var isGeneration = true;
            foreach (var c in afterColon)
            {
                if (!char.IsDigit(c))
                {
                    isGeneration = false;
                    break;
                }
            }

            if (isGeneration && afterColon.Length >= 6) // Generation is typically 8+ digits
            {
                span = span[..lastColonIndex];
            }
        }

        return span.ToString();
    }

    /// <summary>
    /// Truncates a string to the specified maximum length with ellipsis.
    /// </summary>
    public static string Truncate(string? value, int maxLength = 50)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;

        if (value.Length <= maxLength)
            return value;

        // Leave room for ellipsis
        return string.Concat(value.AsSpan(0, maxLength - 3), "...");
    }

    #endregion

    #region Time Formatting

    /// <summary>
    /// Formats a duration as a human-readable string.
    /// </summary>
    public static string FormatDuration(TimeSpan duration)
    {
        if (duration.TotalSeconds < 60)
            return $"{duration.TotalSeconds:F0}s";

        if (duration.TotalMinutes < 60)
            return $"{duration.TotalMinutes:F0}m";

        if (duration.TotalHours < 24)
            return $"{duration.TotalHours:F1}h";

        return $"{duration.TotalDays:F1}d";
    }

    /// <summary>
    /// Formats a DateTime as relative time (e.g., "2 minutes ago").
    /// </summary>
    public static string FormatRelativeTime(DateTime timestamp)
    {
        var elapsed = DateTime.UtcNow - timestamp;

        if (elapsed.TotalSeconds < 5)
            return "just now";

        if (elapsed.TotalSeconds < 60)
            return $"{elapsed.TotalSeconds:F0}s ago";

        if (elapsed.TotalMinutes < 60)
            return $"{elapsed.TotalMinutes:F0}m ago";

        if (elapsed.TotalHours < 24)
            return $"{elapsed.TotalHours:F0}h ago";

        return $"{elapsed.TotalDays:F0}d ago";
    }

    /// <summary>
    /// Formats a timestamp for display (local time).
    /// </summary>
    public static string FormatTimestamp(DateTime timestamp)
    {
        var local = timestamp.Kind == DateTimeKind.Utc
            ? timestamp.ToLocalTime()
            : timestamp;

        return local.ToString("HH:mm:ss", InvariantCulture);
    }

    /// <summary>
    /// Formats a full timestamp with date for display.
    /// </summary>
    public static string FormatFullTimestamp(DateTime timestamp)
    {
        var local = timestamp.Kind == DateTimeKind.Utc
            ? timestamp.ToLocalTime()
            : timestamp;

        return local.ToString("yyyy-MM-dd HH:mm:ss", InvariantCulture);
    }

    #endregion

    #region Status Formatting

    /// <summary>
    /// Gets a CSS class name for a health status percentage.
    /// </summary>
    public static string GetHealthStatusClass(double healthPercent)
    {
        return healthPercent switch
        {
            >= 95 => "status-healthy",
            >= 80 => "status-warning",
            >= 50 => "status-degraded",
            _ => "status-critical"
        };
    }

    /// <summary>
    /// Gets a CSS class name for latency severity.
    /// </summary>
    public static string GetLatencyStatusClass(double latencyMs)
    {
        return latencyMs switch
        {
            < 10 => "latency-excellent",
            < 50 => "latency-good",
            < 200 => "latency-moderate",
            < 1000 => "latency-slow",
            _ => "latency-critical"
        };
    }

    /// <summary>
    /// Gets a CSS class name for error rate severity.
    /// </summary>
    public static string GetErrorRateStatusClass(double errorRatePercent)
    {
        return errorRatePercent switch
        {
            < 0.1 => "error-none",
            < 1 => "error-low",
            < 5 => "error-moderate",
            < 10 => "error-high",
            _ => "error-critical"
        };
    }

    #endregion
}
