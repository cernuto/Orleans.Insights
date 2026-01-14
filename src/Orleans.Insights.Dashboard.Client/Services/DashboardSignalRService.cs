using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Orleans.Insights.Dashboard.Client.Services;

/// <summary>
/// SignalR-based implementation of IDashboardDataService for Blazor WASM clients.
/// </summary>
/// <remarks>
/// <para>
/// This service manages the SignalR connection to the dashboard hub and provides:
/// </para>
/// <list type="bullet">
/// <item>Automatic reconnection with exponential backoff</item>
/// <item>Page subscription management (joins SignalR groups)</item>
/// <item>Push-based updates from grain observers</item>
/// <item>Periodic heartbeat to keep Orleans observer references alive</item>
/// </list>
/// </remarks>
public sealed class DashboardSignalRService : IDashboardDataService
{
    private readonly NavigationManager _navigationManager;
    private readonly ILogger<DashboardSignalRService> _logger;
    private HubConnection? _hubConnection;
    private volatile bool _disposed;

    // .NET 9+: Use Lock class for better performance than object lock
    private readonly Lock _subscribedPagesLock = new();
    private readonly HashSet<string> _subscribedPages = new(StringComparer.OrdinalIgnoreCase);

    // Heartbeat timer - keeps Orleans observer references alive (15s < Orleans 30s timeout)
    private Timer? _heartbeatTimer;
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(15);

    public DashboardSignalRService(
        NavigationManager navigationManager,
        ILogger<DashboardSignalRService> logger)
    {
        _navigationManager = navigationManager;
        _logger = logger;
    }

    #region Connection State

    public bool IsConnected => _hubConnection?.State == HubConnectionState.Connected;

    public event Action<bool>? OnConnectionStateChanged;

    // Retry delays for initial connection (exponential backoff: 0s, 1s, 2s, 5s, 10s)
    private static readonly TimeSpan[] InitialConnectionRetryDelays =
    [
        TimeSpan.Zero,
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(10)
    ];

    public async Task StartAsync()
    {
        if (_hubConnection is not null)
            return;

        // Build hub URL from NavigationManager base URI
        var hubUrl = new Uri(new Uri(_navigationManager.BaseUri), "hub");

        // Configure SignalR with automatic reconnection using infinite instant retry
        // This ensures the dashboard stays connected even with intermittent network issues
        _hubConnection = new HubConnectionBuilder()
            .WithUrl(hubUrl)
            .WithAutomaticReconnect(new InfiniteInstantRetryPolicy())
            .Build();

        // Register handlers for server-pushed page data
        RegisterHandlers();

        // Track connection state changes
        _hubConnection.Closed += error =>
        {
            _logger.LogWarning(error, "SignalR connection closed");
            StopHeartbeatTimer();
            OnConnectionStateChanged?.Invoke(false);
            return Task.CompletedTask;
        };

        _hubConnection.Reconnecting += error =>
        {
            _logger.LogInformation(error, "SignalR reconnecting...");
            StopHeartbeatTimer();
            OnConnectionStateChanged?.Invoke(false);
            return Task.CompletedTask;
        };

        _hubConnection.Reconnected += async connectionId =>
        {
            _logger.LogInformation("SignalR reconnected: {ConnectionId}", connectionId);
            StartHeartbeatTimer();
            await ResubscribeAllPagesAsync();
            OnConnectionStateChanged?.Invoke(true);
        };

        // Retry initial connection with exponential backoff
        // WithAutomaticReconnect only works AFTER a successful connection
        for (var attempt = 0; attempt < InitialConnectionRetryDelays.Length; attempt++)
        {
            var delay = InitialConnectionRetryDelays[attempt];
            if (delay > TimeSpan.Zero)
            {
                _logger.LogInformation("Retrying SignalR connection in {DelayMs}ms (attempt {Attempt}/{Max})",
                    delay.TotalMilliseconds, attempt + 1, InitialConnectionRetryDelays.Length);
                await Task.Delay(delay);
            }

            try
            {
                await _hubConnection.StartAsync();
                _logger.LogInformation("SignalR connected to dashboard hub");
                StartHeartbeatTimer();
                OnConnectionStateChanged?.Invoke(true);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "SignalR connection attempt {Attempt}/{Max} failed",
                    attempt + 1, InitialConnectionRetryDelays.Length);
            }
        }

        _logger.LogError("Failed to connect to dashboard hub after {Attempts} attempts",
            InitialConnectionRetryDelays.Length);
        OnConnectionStateChanged?.Invoke(false);
    }

    /// <summary>
    /// Waits for the SignalR connection to be established.
    /// Useful for pages that need to ensure connection before subscribing.
    /// </summary>
    /// <param name="timeout">Maximum time to wait. Default: 10 seconds.</param>
    /// <returns>True if connected within timeout, false otherwise.</returns>
    public async Task<bool> WaitForConnectionAsync(TimeSpan? timeout = null)
    {
        if (IsConnected)
            return true;

        var effectiveTimeout = timeout ?? TimeSpan.FromSeconds(10);
        var deadline = DateTime.UtcNow.Add(effectiveTimeout);
        const int pollIntervalMs = 100;

        while (DateTime.UtcNow < deadline)
        {
            if (IsConnected)
                return true;

            await Task.Delay(pollIntervalMs);
        }

        _logger.LogWarning("Timed out waiting for SignalR connection after {Timeout}ms", effectiveTimeout.TotalMilliseconds);
        return false;
    }

    private void RegisterHandlers()
    {
        if (_hubConnection is null) return;

        _hubConnection.On<JsonElement>("ReceiveOverviewData", data =>
        {
            _logger.LogDebug("Received Overview data");
            OnOverviewDataReceived?.Invoke(data);
        });

        _hubConnection.On<JsonElement>("ReceiveOrleansData", data =>
        {
            _logger.LogDebug("Received Orleans data");
            OnOrleansDataReceived?.Invoke(data);
        });

        _hubConnection.On<JsonElement>("ReceiveInsightsData", data =>
        {
            _logger.LogDebug("Received Insights data");
            OnInsightsDataReceived?.Invoke(data);
        });
    }

    #endregion

    #region Page Subscription

    public async Task SubscribeToPage(string pageName)
    {
        if (_hubConnection is null || !IsConnected)
        {
            _logger.LogWarning("Cannot subscribe to {Page}: not connected", pageName);
            return;
        }

        try
        {
            await _hubConnection.InvokeAsync("SubscribeToPage", pageName);

            lock (_subscribedPagesLock)
            {
                _subscribedPages.Add(pageName);
            }

            _logger.LogDebug("Subscribed to {Page}", pageName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to subscribe to {Page}", pageName);
        }
    }

    public async Task UnsubscribeFromPage(string pageName)
    {
        if (_hubConnection is null || !IsConnected)
            return;

        try
        {
            await _hubConnection.InvokeAsync("UnsubscribeFromPage", pageName);

            lock (_subscribedPagesLock)
            {
                _subscribedPages.Remove(pageName);
            }

            _logger.LogDebug("Unsubscribed from {Page}", pageName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to unsubscribe from {Page}", pageName);
        }
    }

    #endregion

    #region Data Events

    public event Action<JsonElement>? OnOverviewDataReceived;
    public event Action<JsonElement>? OnOrleansDataReceived;
    public event Action<JsonElement>? OnInsightsDataReceived;

    #endregion

    #region Data Fetch

    public Task<JsonElement> GetOverviewPageDataAsync()
        => InvokeHubMethodAsync("GetOverviewPageData");

    public Task<JsonElement> GetOrleansPageDataAsync()
        => InvokeHubMethodAsync("GetOrleansPageData");

    public Task<JsonElement> GetInsightsPageDataAsync()
        => InvokeHubMethodAsync("GetInsightsPageData");

    public Task<JsonElement> GetSettingsPageDataAsync()
        => InvokeHubMethodAsync("GetSettingsPageData");

    public Task<JsonElement> GetMethodProfileTrendAsync(
        string? grainType = null,
        string? methodName = null,
        TimeSpan? duration = null,
        int bucketSeconds = 1)
    {
        var durationSeconds = (duration ?? TimeSpan.FromMinutes(5)).TotalSeconds;
        _logger.LogDebug(
            "GetMethodProfileTrendAsync: grainType={GrainType}, method={Method}, duration={Duration}s",
            grainType, methodName, durationSeconds);

        return InvokeHubMethodAsync("GetMethodProfileTrend", grainType, methodName, durationSeconds, bucketSeconds);
    }

    private async Task<JsonElement> InvokeHubMethodAsync(string method, params object?[] args)
    {
        if (_hubConnection is null || !IsConnected)
        {
            _logger.LogWarning("Cannot invoke {Method}: not connected", method);
            return default;
        }

        try
        {
            return await _hubConnection.InvokeCoreAsync<JsonElement>(method, args);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to invoke {Method}", method);
            return default;
        }
    }

    #endregion

    #region Heartbeat

    private void StartHeartbeatTimer()
    {
        StopHeartbeatTimer();
        _heartbeatTimer = new Timer(
            _ => _ = HeartbeatAsync(),
            null,
            HeartbeatInterval,
            HeartbeatInterval);
        _logger.LogDebug("Heartbeat started (interval: {Interval}s)", HeartbeatInterval.TotalSeconds);
    }

    private void StopHeartbeatTimer()
    {
        _heartbeatTimer?.Dispose();
        _heartbeatTimer = null;
    }

    private async Task HeartbeatAsync()
    {
        if (_hubConnection is null || !IsConnected || _disposed)
            return;

        string[] pagesToRefresh;
        lock (_subscribedPagesLock)
        {
            if (_subscribedPages.Count == 0)
                return;
            pagesToRefresh = [.. _subscribedPages];
        }

        _logger.LogDebug("Heartbeat: refreshing {Count} subscriptions", pagesToRefresh.Length);

        foreach (var page in pagesToRefresh)
        {
            try
            {
                await _hubConnection.InvokeAsync("SubscribeToPage", page);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Heartbeat: failed to refresh {Page}", page);
            }
        }
    }

    private async Task ResubscribeAllPagesAsync()
    {
        string[] pagesToResubscribe;
        lock (_subscribedPagesLock)
        {
            if (_subscribedPages.Count == 0)
                return;
            pagesToResubscribe = [.. _subscribedPages];
        }

        _logger.LogInformation("Re-subscribing to {Count} pages", pagesToResubscribe.Length);

        foreach (var page in pagesToResubscribe)
        {
            try
            {
                await _hubConnection!.InvokeAsync("SubscribeToPage", page);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to re-subscribe to {Page}", page);
            }
        }
    }

    #endregion

    #region IAsyncDisposable

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        StopHeartbeatTimer();

        lock (_subscribedPagesLock)
        {
            _subscribedPages.Clear();
        }

        if (_hubConnection is not null)
        {
            await _hubConnection.DisposeAsync();
            _hubConnection = null;
        }
    }

    #endregion
}

/// <summary>
/// Retry policy that retries immediately and infinitely.
/// Always returns TimeSpan.Zero to retry instantly without giving up.
/// Better UX for monitoring dashboards that need to stay connected.
/// </summary>
file class InfiniteInstantRetryPolicy : IRetryPolicy
{
    public TimeSpan? NextRetryDelay(RetryContext retryContext) => TimeSpan.Zero;
}
