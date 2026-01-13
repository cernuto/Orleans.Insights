using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Insights.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Orleans.Insights.Dashboard.Hubs;

/// <summary>
/// SignalR hub for dashboard real-time updates.
///
/// WASM clients connect to this hub to:
/// - Subscribe to page-specific groups (receive updates only for pages they're viewing)
/// - Receive pushed data when grain observers notify of changes
/// - Request immediate data fetch (initial load or manual refresh)
/// </summary>
public class DashboardHub : Hub
{
    private readonly IClusterClient _clusterClient;
    private readonly IConfiguration _configuration;
    private readonly ILogger<DashboardHub> _logger;
    private readonly JsonSerializerOptions _jsonOptions;

    public DashboardHub(
        IClusterClient clusterClient,
        IConfiguration configuration,
        ILogger<DashboardHub> logger)
    {
        _clusterClient = clusterClient;
        _configuration = configuration;
        _logger = logger;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            Converters = { new JsonStringEnumConverter() }
        };
    }

    #region Connection Lifecycle

    public override async Task OnConnectedAsync()
    {
        _logger.LogInformation(
            "Dashboard client connected: {ConnectionId}",
            Context.ConnectionId);
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation(
            "Dashboard client disconnected: {ConnectionId}, Exception: {Exception}",
            Context.ConnectionId,
            exception?.Message);
        await base.OnDisconnectedAsync(exception);
    }

    #endregion

    #region Page Subscription

    /// <summary>
    /// Subscribe to updates for a specific page.
    /// Client joins a SignalR group named after the page.
    /// Immediately sends current page data to the caller.
    /// </summary>
    /// <param name="pageName">Page name (health, overview, orleans, insights)</param>
    public async Task SubscribeToPage(string pageName)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, pageName.ToLowerInvariant());
        _logger.LogDebug("Client {ConnectionId} subscribed to page: {Page}", Context.ConnectionId, pageName);

        // Send initial data immediately so the client doesn't wait for first push
        try
        {
            var data = await GetPageDataAsync(pageName);
            if (data.ValueKind != JsonValueKind.Undefined)
            {
                await Clients.Caller.SendAsync($"Receive{CapitalizeFirstLetter(pageName)}Data", data);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send initial data for page: {Page}", pageName);
        }
    }

    /// <summary>
    /// Unsubscribe from updates for a specific page.
    /// Client leaves the SignalR group.
    /// </summary>
    /// <param name="pageName">Page name to unsubscribe from</param>
    public async Task UnsubscribeFromPage(string pageName)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, pageName.ToLowerInvariant());
        _logger.LogDebug("Client {ConnectionId} unsubscribed from page: {Page}", Context.ConnectionId, pageName);
    }

    #endregion

    #region Data Fetch Methods

    /// <summary>
    /// Get Health page data immediately.
    /// </summary>
    public async Task<JsonElement> GetHealthPageData()
    {
        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var data = await grain.GetHealthPageData();
        return JsonSerializer.SerializeToElement(data, _jsonOptions);
    }

    /// <summary>
    /// Get Overview page data immediately.
    /// </summary>
    public async Task<JsonElement> GetOverviewPageData()
    {
        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var data = await grain.GetOverviewPageData();
        return JsonSerializer.SerializeToElement(data, _jsonOptions);
    }

    /// <summary>
    /// Get Orleans page data immediately.
    /// </summary>
    public async Task<JsonElement> GetOrleansPageData()
    {
        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var data = await grain.GetOrleansPageData();
        return JsonSerializer.SerializeToElement(data, _jsonOptions);
    }

    /// <summary>
    /// Get Insights page data immediately.
    /// </summary>
    public async Task<JsonElement> GetInsightsPageData()
    {
        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var data = await grain.GetInsightsPageData();
        return JsonSerializer.SerializeToElement(data, _jsonOptions);
    }

    /// <summary>
    /// Get Settings page data - server configuration.
    /// </summary>
    public Task<JsonElement> GetSettingsPageData()
    {
        var configDict = new Dictionary<string, object?>();
        BuildConfigDictionary(_configuration, configDict);
        return Task.FromResult(JsonSerializer.SerializeToElement(configDict, _jsonOptions));
    }

    private static void BuildConfigDictionary(IConfiguration config, Dictionary<string, object?> dict)
    {
        foreach (var section in config.GetChildren())
        {
            // Skip sensitive sections
            if (section.Key.Equals("ConnectionStrings", StringComparison.OrdinalIgnoreCase) ||
                section.Key.Contains("Secret", StringComparison.OrdinalIgnoreCase) ||
                section.Key.Contains("Password", StringComparison.OrdinalIgnoreCase) ||
                section.Key.Contains("Key", StringComparison.OrdinalIgnoreCase) && section.Key.Contains("Api", StringComparison.OrdinalIgnoreCase))
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
                // Redact individual sensitive values
                var value = section.Value;
                if (IsSensitiveKey(section.Key))
                {
                    value = "[REDACTED]";
                }
                dict[section.Key] = value;
            }
        }
    }

    private static bool IsSensitiveKey(string key) =>
        key.Contains("Secret", StringComparison.OrdinalIgnoreCase) ||
        key.Contains("Password", StringComparison.OrdinalIgnoreCase) ||
        key.Contains("Token", StringComparison.OrdinalIgnoreCase) ||
        key.Contains("ApiKey", StringComparison.OrdinalIgnoreCase) ||
        key.Contains("ConnectionString", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Get method profile trend data for charts.
    /// </summary>
    /// <param name="grainType">Grain type name</param>
    /// <param name="methodName">Method name</param>
    /// <param name="durationSeconds">Duration in seconds (default 300 = 5 minutes)</param>
    /// <param name="bucketSeconds">Bucket size in seconds</param>
    public async Task<JsonElement> GetMethodProfileTrend(
        string? grainType = null,
        string? methodName = null,
        double durationSeconds = 300,
        int bucketSeconds = 1)
    {
        _logger.LogDebug("GetMethodProfileTrend called: grainType={GrainType}, methodName={MethodName}, duration={Duration}s, bucket={Bucket}s",
            grainType, methodName, durationSeconds, bucketSeconds);

        var grain = _clusterClient.GetGrain<IInsightsGrain>(IInsightsGrain.SingletonKey);
        var effectiveDuration = TimeSpan.FromSeconds(durationSeconds);

        // Route to the appropriate method based on parameters
        object data;
        if (!string.IsNullOrEmpty(methodName) && !string.IsNullOrEmpty(grainType))
        {
            data = await grain.GetMethodProfileTrendForMethod(grainType, methodName, effectiveDuration, bucketSeconds);
            _logger.LogDebug("GetMethodProfileTrendForMethod returned {Count} points", ((System.Collections.IList)data).Count);
        }
        else if (!string.IsNullOrEmpty(grainType))
        {
            data = await grain.GetMethodProfileTrendForGrain(grainType, effectiveDuration, bucketSeconds);
            _logger.LogDebug("GetMethodProfileTrendForGrain returned {Count} points", ((System.Collections.IList)data).Count);
        }
        else
        {
            data = await grain.GetMethodProfileTrend(effectiveDuration, bucketSeconds);
            _logger.LogDebug("GetMethodProfileTrend returned {Count} points", ((System.Collections.IList)data).Count);
        }

        return JsonSerializer.SerializeToElement(data, _jsonOptions);
    }

    #endregion

    #region Helper Methods

    private async Task<JsonElement> GetPageDataAsync(string pageName)
    {
        return pageName.ToLowerInvariant() switch
        {
            "health" => await GetHealthPageData(),
            "overview" => await GetOverviewPageData(),
            "orleans" => await GetOrleansPageData(),
            "insights" => await GetInsightsPageData(),
            "settings" => await GetSettingsPageData(),
            _ => default
        };
    }

    private static string CapitalizeFirstLetter(string input)
    {
        if (string.IsNullOrEmpty(input)) return input;
        return char.ToUpper(input[0]) + input[1..].ToLower();
    }

    #endregion
}

/// <summary>
/// Page name constants for SignalR groups.
/// Must match the page names used in SubscribeToPage/UnsubscribeFromPage.
/// </summary>
public static class DashboardPages
{
    public const string Health = "health";
    public const string Overview = "overview";
    public const string Orleans = "orleans";
    public const string Insights = "insights";
    public const string Settings = "settings";
}
