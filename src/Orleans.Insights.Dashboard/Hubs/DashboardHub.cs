using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans.Insights.Models;
using System.Collections.Frozen;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Insights.Dashboard.Hubs;

/// <summary>
/// SignalR hub for dashboard real-time updates.
/// </summary>
/// <remarks>
/// <para>
/// WASM clients connect to this hub to:
/// </para>
/// <list type="bullet">
/// <item>Subscribe to page-specific groups (receive updates only for pages they're viewing)</item>
/// <item>Receive pushed data when grain observers notify of changes</item>
/// <item>Request immediate data fetch (initial load or manual refresh)</item>
/// </list>
/// </remarks>
public sealed class DashboardHub : Hub
{
    private readonly IClusterClient _clusterClient;
    private readonly IConfiguration _configuration;
    private readonly ILogger<DashboardHub> _logger;

    // Cached JSON options for serialization
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter() }
    };

    // Frozen set of sensitive configuration keys (fast lookup, thread-safe, allocated once)
    private static readonly FrozenSet<string> SensitiveKeys = new[]
    {
        "ConnectionStrings",
        "Secret",
        "Password",
        "Token",
        "ApiKey",
        "ConnectionString"
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    public DashboardHub(
        IClusterClient clusterClient,
        IConfiguration configuration,
        ILogger<DashboardHub> logger)
    {
        _clusterClient = clusterClient;
        _configuration = configuration;
        _logger = logger;
    }

    #region Connection Lifecycle

    public override Task OnConnectedAsync()
    {
        _logger.LogInformation("Dashboard client connected: {ConnectionId}", Context.ConnectionId);
        return base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation(
            "Dashboard client disconnected: {ConnectionId}, Exception: {Exception}",
            Context.ConnectionId,
            exception?.Message);
        return base.OnDisconnectedAsync(exception);
    }

    #endregion

    #region Page Subscription

    /// <summary>
    /// Subscribe to updates for a specific page.
    /// </summary>
    /// <param name="pageName">Page name (health, overview, orleans, insights)</param>
    public async Task SubscribeToPage(string pageName)
    {
        var normalizedPage = pageName.ToLowerInvariant();
        await Groups.AddToGroupAsync(Context.ConnectionId, normalizedPage);
        _logger.LogDebug("Client {ConnectionId} subscribed to: {Page}", Context.ConnectionId, normalizedPage);

        // Send initial data immediately
        try
        {
            var data = await GetPageDataAsync(normalizedPage);
            if (data.ValueKind != JsonValueKind.Undefined)
            {
                await Clients.Caller.SendAsync($"Receive{CapitalizeFirstLetter(normalizedPage)}Data", data);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send initial data for page: {Page}", normalizedPage);
        }
    }

    /// <summary>
    /// Unsubscribe from updates for a specific page.
    /// </summary>
    public async Task UnsubscribeFromPage(string pageName)
    {
        var normalizedPage = pageName.ToLowerInvariant();
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, normalizedPage);
        _logger.LogDebug("Client {ConnectionId} unsubscribed from: {Page}", Context.ConnectionId, normalizedPage);
    }

    #endregion

    #region Data Fetch Methods

    /// <summary>Get Health page data immediately.</summary>
    public async Task<JsonElement> GetHealthPageData()
    {
        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var data = await grain.GetHealthPageData();
        return JsonSerializer.SerializeToElement(data, JsonOptions);
    }

    /// <summary>Get Overview page data immediately.</summary>
    public async Task<JsonElement> GetOverviewPageData()
    {
        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var data = await grain.GetOverviewPageData();
        return JsonSerializer.SerializeToElement(data, JsonOptions);
    }

    /// <summary>Get Orleans page data immediately.</summary>
    public async Task<JsonElement> GetOrleansPageData()
    {
        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var data = await grain.GetOrleansPageData();
        return JsonSerializer.SerializeToElement(data, JsonOptions);
    }

    /// <summary>Get Insights page data immediately.</summary>
    public async Task<JsonElement> GetInsightsPageData()
    {
        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var data = await grain.GetInsightsPageData();
        return JsonSerializer.SerializeToElement(data, JsonOptions);
    }

    /// <summary>Get Settings page data - server configuration.</summary>
    public Task<JsonElement> GetSettingsPageData()
    {
        var configDict = new Dictionary<string, object?>();
        BuildConfigDictionary(_configuration, configDict);
        return Task.FromResult(JsonSerializer.SerializeToElement(configDict, JsonOptions));
    }

    /// <summary>Get method profile trend data for charts.</summary>
    public async Task<JsonElement> GetMethodProfileTrend(
        string? grainType = null,
        string? methodName = null,
        double durationSeconds = 300,
        int bucketSeconds = 1)
    {
        _logger.LogDebug(
            "GetMethodProfileTrend: grainType={GrainType}, method={MethodName}, duration={Duration}s",
            grainType, methodName, durationSeconds);

        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var effectiveDuration = TimeSpan.FromSeconds(durationSeconds);

        // Route to the appropriate method based on parameters
        object data = (grainType, methodName) switch
        {
            (not null, not null) => await grain.GetMethodProfileTrendForMethod(grainType, methodName, effectiveDuration, bucketSeconds),
            (not null, null) => await grain.GetMethodProfileTrendForGrain(grainType, effectiveDuration, bucketSeconds),
            _ => await grain.GetMethodProfileTrend(effectiveDuration, bucketSeconds)
        };

        return JsonSerializer.SerializeToElement(data, JsonOptions);
    }

    #endregion

    #region Helper Methods

    private async Task<JsonElement> GetPageDataAsync(string pageName) => pageName switch
    {
        DashboardPages.Health => await GetHealthPageData(),
        DashboardPages.Overview => await GetOverviewPageData(),
        DashboardPages.Orleans => await GetOrleansPageData(),
        DashboardPages.Insights => await GetInsightsPageData(),
        DashboardPages.Settings => await GetSettingsPageData(),
        _ => default
    };

    private static string CapitalizeFirstLetter(ReadOnlySpan<char> input)
    {
        if (input.IsEmpty) return string.Empty;
        return string.Create(input.Length, input.ToString(), (span, str) =>
        {
            span[0] = char.ToUpper(str[0]);
            str.AsSpan(1).ToLower(span[1..], null);
        });
    }

    private static void BuildConfigDictionary(IConfiguration config, Dictionary<string, object?> dict)
    {
        foreach (var section in config.GetChildren())
        {
            // Skip sensitive sections
            if (IsSensitiveKey(section.Key))
            {
                dict[section.Key] = "[REDACTED]";
                continue;
            }

            if (section.GetChildren().Any())
            {
                var childDict = new Dictionary<string, object?>();
                BuildConfigDictionary(section, childDict);
                dict[section.Key] = childDict;
            }
            else
            {
                dict[section.Key] = section.Value;
            }
        }
    }

    private static bool IsSensitiveKey(string key)
        => SensitiveKeys.Any(sensitive => key.Contains(sensitive, StringComparison.OrdinalIgnoreCase));

    #endregion
}

/// <summary>
/// Page name constants for SignalR groups.
/// </summary>
public static class DashboardPages
{
    public const string Health = "health";
    public const string Overview = "overview";
    public const string Orleans = "orleans";
    public const string Insights = "insights";
    public const string Settings = "settings";
}
