using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

namespace Orleans.Insights.Dashboard.Client.Services;

/// <summary>
/// Interface for dashboard data access.
/// Uses SignalR for WASM client communication with the dashboard hub.
///
/// Uses JsonElement for data to avoid coupling to Orleans-specific types.
/// The actual type casting happens in the implementation.
/// </summary>
public interface IDashboardDataService : IAsyncDisposable
{
    #region Connection State

    /// <summary>
    /// Gets whether the service is connected and ready.
    /// True when SignalR hub is connected.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Raised when connection state changes.
    /// </summary>
    event Action<bool>? OnConnectionStateChanged;

    /// <summary>
    /// Starts the data service.
    /// Connects to SignalR hub.
    /// </summary>
    Task StartAsync();

    /// <summary>
    /// Waits for the connection to be established with a timeout.
    /// </summary>
    /// <param name="timeout">Maximum time to wait for connection. Default: 10 seconds.</param>
    /// <returns>True if connected within timeout, false otherwise.</returns>
    Task<bool> WaitForConnectionAsync(TimeSpan? timeout = null);

    #endregion

    #region Page Subscription

    /// <summary>
    /// Subscribe to updates for a specific page.
    /// Called when a page component initializes.
    /// </summary>
    /// <param name="pageName">Page name: health, overview, orleans, insights</param>
    Task SubscribeToPage(string pageName);

    /// <summary>
    /// Unsubscribe from updates for a specific page.
    /// Called when a page component disposes.
    /// </summary>
    /// <param name="pageName">Page name to unsubscribe from</param>
    Task UnsubscribeFromPage(string pageName);

    #endregion

    #region Data Events - Using JsonElement for cross-platform compatibility

    /// <summary>Raised when Health page data is received.</summary>
    event Action<JsonElement>? OnHealthDataReceived;

    /// <summary>Raised when Overview page data is received.</summary>
    event Action<JsonElement>? OnOverviewDataReceived;

    /// <summary>Raised when Orleans page data is received.</summary>
    event Action<JsonElement>? OnOrleansDataReceived;

    /// <summary>Raised when Insights page data is received.</summary>
    event Action<JsonElement>? OnInsightsDataReceived;

    #endregion

    #region Data Fetch (for initial load and on-demand refresh)

    /// <summary>Fetches Health page data immediately.</summary>
    Task<JsonElement> GetHealthPageDataAsync();

    /// <summary>Fetches Overview page data immediately.</summary>
    Task<JsonElement> GetOverviewPageDataAsync();

    /// <summary>Fetches Orleans page data immediately.</summary>
    Task<JsonElement> GetOrleansPageDataAsync();

    /// <summary>Fetches Insights page data immediately.</summary>
    Task<JsonElement> GetInsightsPageDataAsync();

    /// <summary>Fetches Settings page data (server configuration) immediately.</summary>
    Task<JsonElement> GetSettingsPageDataAsync();

    /// <summary>Fetches method profile trend data for charts.</summary>
    Task<JsonElement> GetMethodProfileTrendAsync(
        string? grainType = null,
        string? methodName = null,
        TimeSpan? duration = null,
        int bucketSeconds = 1);

    #endregion
}

/// <summary>
/// Page name constants for subscription/unsubscription.
/// </summary>
public static class ClientDashboardPages
{
    public const string Health = "health";
    public const string Overview = "overview";
    public const string Orleans = "orleans";
    public const string Insights = "insights";
    public const string Settings = "settings";
}
