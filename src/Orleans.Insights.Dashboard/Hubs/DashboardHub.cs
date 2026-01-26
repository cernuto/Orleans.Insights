using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Insights.Dashboard.Observers;
using Orleans.Insights.Models;
using System.Collections.Frozen;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Insights.Dashboard.Hubs;

/// <summary>
/// SignalR hub for dashboard real-time updates using Orleans observer pattern.
/// </summary>
/// <remarks>
/// <para>
/// WASM clients connect to this hub to:
/// </para>
/// <list type="bullet">
/// <item>Subscribe to page-specific updates via Orleans observers</item>
/// <item>Receive pushed data when the broadcast grain dispatches to observers</item>
/// <item>Request immediate data fetch (initial load or manual refresh)</item>
/// </list>
/// <para>
/// Uses Orleans IGrainObserver pattern for push-based notifications with:
/// - Per-connection observer creation via CreateObjectReference
/// - Subscription tracking in DashboardBroadcastGrain
/// - Automatic cleanup on disconnect
/// </para>
/// <para>
/// IMPORTANT: The observer callbacks use IHubContext (not Hub.Clients) because:
/// - Hub instances are transient and disposed after each request
/// - Observers are long-lived and called outside of hub method invocations
/// - IHubContext is a singleton service that survives beyond the hub's lifetime
/// </para>
/// </remarks>
public sealed class DashboardHub : Hub
{
    private readonly IClusterClient _clusterClient;
    private readonly IConfiguration _configuration;
    private readonly ILogger<DashboardHub> _logger;
    private readonly DashboardObserverOptions _options;
    private readonly IHubContext<DashboardHub> _hubContext;

    // Cached JSON options for serialization
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter() }
    };

    // Frozen set of sensitive configuration keys (fast lookup, thread-safe, allocated once)
    private static readonly FrozenSet<string> SensitiveKeys = FrozenSet.ToFrozenSet(
    [
        "ConnectionStrings",
        "Secret",
        "Password",
        "Token",
        "ApiKey",
        "ConnectionString"
    ], StringComparer.OrdinalIgnoreCase);

    public DashboardHub(
        IClusterClient clusterClient,
        IConfiguration configuration,
        ILogger<DashboardHub> logger,
        IOptions<DashboardObserverOptions> options,
        IHubContext<DashboardHub> hubContext)
    {
        _clusterClient = clusterClient;
        _configuration = configuration;
        _logger = logger;
        _options = options.Value;
        _hubContext = hubContext;
    }

    #region Connection Lifecycle

    public override async Task OnConnectedAsync()
    {
        _logger.LogInformation("Dashboard client connected: {ConnectionId}", Context.ConnectionId);

        // Capture these values for use in observer callbacks
        // CRITICAL: We must capture the IHubContext and connectionId directly, NOT use 'this' or any instance methods
        // This is because the Hub instance is transient and will be disposed after OnConnectedAsync completes
        // But the observer callbacks will be invoked later by Orleans when data needs to be pushed
        var connectionId = Context.ConnectionId;
        var hubContext = _hubContext;

        // Create observer for this connection using captured IHubContext (singleton)
        // The lambdas capture hubContext and connectionId by value, not 'this'
        var observer = new DashboardObserver(
            onOverview: data => SendToConnection(hubContext, connectionId, "ReceiveOverviewData", data),
            onOrleans: data => SendToConnection(hubContext, connectionId, "ReceiveOrleansData", data),
            onInsights: data => SendToConnection(hubContext, connectionId, "ReceiveInsightsData", data));

        // Create grain object reference
        var reference = _clusterClient.CreateObjectReference<IDashboardObserver>(observer);

        // Create subscription and store in connection features
        var subscription = new DashboardSubscription(observer);
        subscription.SetReference(reference);
        Context.Items["Subscription"] = subscription;

        await base.OnConnectedAsync();
    }

    /// <summary>
    /// Static helper to send data to a connection via IHubContext.
    /// This is static to ensure observer callbacks don't accidentally capture 'this'.
    /// </summary>
    private static Task SendToConnection<T>(IHubContext<DashboardHub> hubContext, string connectionId, string method, T data)
    {
        var json = JsonSerializer.SerializeToElement(data, JsonOptions);
        return hubContext.Clients.Client(connectionId).SendAsync(method, json);
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation(
            "Dashboard client disconnected: {ConnectionId}, Exception: {Exception}",
            Context.ConnectionId,
            exception?.Message);

        // Cleanup subscription
        if (Context.Items.TryGetValue("Subscription", out var obj) && obj is DashboardSubscription subscription)
        {
            try
            {
                // Unsubscribe from all pages in the broadcast grain
                var grain = _clusterClient.GetGrain<IDashboardBroadcastGrain>(0);
                await grain.UnsubscribeAll(Context.ConnectionId);

                // Delete the object reference
                _clusterClient.DeleteObjectReference<IDashboardObserver>(subscription.Reference);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error cleaning up subscription for connection {ConnectionId}", Context.ConnectionId);
            }
            finally
            {
                subscription.Dispose();
                Context.Items.Remove("Subscription");
            }
        }

        await base.OnDisconnectedAsync(exception);
    }

    #endregion

    #region Page Subscription

    /// <summary>
    /// Subscribe to updates for a specific page.
    /// </summary>
    /// <param name="pageName">Page name (overview, orleans, insights)</param>
    public async Task SubscribeToPage(string pageName)
    {
        var normalizedPage = pageName.ToLowerInvariant();

        var subscription = GetSubscription();
        if (subscription is null)
        {
            _logger.LogWarning("No subscription found for connection {ConnectionId}", Context.ConnectionId);
            return;
        }

        // Add to local tracking
        subscription.AddPage(normalizedPage);

        // Subscribe with the broadcast grain
        var grain = _clusterClient.GetGrain<IDashboardBroadcastGrain>(0);
        await grain.Subscribe(Context.ConnectionId, normalizedPage, subscription.Reference);

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

        var subscription = GetSubscription();
        subscription?.RemovePage(normalizedPage);

        // Unsubscribe from the broadcast grain
        var grain = _clusterClient.GetGrain<IDashboardBroadcastGrain>(0);
        await grain.Unsubscribe(Context.ConnectionId, normalizedPage);

        _logger.LogDebug("Client {ConnectionId} unsubscribed from: {Page}", Context.ConnectionId, normalizedPage);
    }

    /// <summary>
    /// Heartbeat to refresh the observer subscription and prevent expiration.
    /// Clients should call this periodically (recommended: every 30 seconds).
    /// </summary>
    public async Task Heartbeat()
    {
        var subscription = GetSubscription();
        if (subscription is null)
        {
            _logger.LogWarning("Heartbeat: No subscription found for connection {ConnectionId}", Context.ConnectionId);
            return;
        }

        // Touch the subscription in the broadcast grain to refresh expiration
        var grain = _clusterClient.GetGrain<IDashboardBroadcastGrain>(0);
        await grain.Touch(Context.ConnectionId, subscription.Reference);

        _logger.LogDebug("Heartbeat received from connection {ConnectionId}", Context.ConnectionId);
    }

    #endregion

    #region Data Fetch Methods

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

    private DashboardSubscription? GetSubscription()
    {
        if (Context.Items.TryGetValue("Subscription", out var obj) && obj is DashboardSubscription subscription)
        {
            return subscription;
        }
        return null;
    }

    private async Task<JsonElement> GetPageDataAsync(string pageName) => pageName switch
    {
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
/// Page name constants for SignalR groups and observer subscriptions.
/// </summary>
public static class DashboardPages
{
    public const string Overview = "overview";
    public const string Orleans = "orleans";
    public const string Insights = "insights";
    public const string Settings = "settings";
}
