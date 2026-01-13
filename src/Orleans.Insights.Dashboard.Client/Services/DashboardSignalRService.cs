using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Orleans.Insights.Dashboard.Client.Services;

/// <summary>
/// SignalR-based implementation of IDashboardDataService for WASM clients.
///
/// Connects to DashboardHub on the server and:
/// - Subscribes to page updates (joins SignalR groups)
/// - Receives pushed data from grain observers
/// - Fires events for page components to update UI
///
/// Uses automatic reconnection with exponential backoff.
/// Includes periodic heartbeat to keep Orleans SignalR observer references alive.
/// </summary>
public sealed class DashboardSignalRService : IDashboardDataService
{
    private readonly NavigationManager _navigationManager;
    private readonly ILogger<DashboardSignalRService> _logger;
    private HubConnection? _hubConnection;
    private bool _disposed;

    /// <summary>
    /// Pages currently subscribed to. Used for heartbeat refresh.
    /// </summary>
    private readonly HashSet<string> _subscribedPages = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _subscribedPagesLock = new();

    /// <summary>
    /// Timer for periodic heartbeat to keep Orleans observer references alive.
    /// </summary>
    private System.Threading.Timer? _heartbeatTimer;

    /// <summary>
    /// Heartbeat interval - must be less than Orleans ClientTimeoutInterval (default 30s).
    /// Using 15 seconds provides comfortable margin.
    /// </summary>
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

    /// <summary>
    /// Retry delays for initial connection attempts.
    /// Uses exponential backoff: 0s, 1s, 2s, 5s, 10s (total ~18s of retries).
    /// </summary>
    private static readonly int[] InitialConnectionRetryDelaysMs = [0, 1000, 2000, 5000, 10000];

    public async Task StartAsync()
    {
        if (_hubConnection != null)
            return;

        // Hub URL - BaseUri already includes the base href (e.g., https://localhost:5001/dashboard/)
        // So we just need to append "hub" to get the full hub URL
        var hubUrl = new Uri(new Uri(_navigationManager.BaseUri), "hub");

        // Configure SignalR with automatic reconnection
        _hubConnection = new HubConnectionBuilder()
            .WithUrl(hubUrl)
            .WithAutomaticReconnect(new[]
            {
                TimeSpan.FromSeconds(0),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(5),
                TimeSpan.FromSeconds(10),
                TimeSpan.FromSeconds(30)
            })
            .Build();

        // Register handlers for server-pushed page data
        RegisterHandlers();

        // Track connection state changes
        _hubConnection.Closed += async (error) =>
        {
            _logger.LogWarning(error, "SignalR connection closed");
            StopHeartbeatTimer();
            OnConnectionStateChanged?.Invoke(false);
            await Task.CompletedTask;
        };

        _hubConnection.Reconnecting += (error) =>
        {
            _logger.LogInformation(error, "SignalR reconnecting...");
            StopHeartbeatTimer(); // Stop heartbeat during reconnection
            OnConnectionStateChanged?.Invoke(false);
            return Task.CompletedTask;
        };

        _hubConnection.Reconnected += async (connectionId) =>
        {
            _logger.LogInformation("SignalR reconnected: {ConnectionId}", connectionId);
            StartHeartbeatTimer();

            // Re-subscribe to all pages after reconnection to refresh Orleans observer references
            await ResubscribeAllPagesAsync();

            OnConnectionStateChanged?.Invoke(true);
        };

        // Retry initial connection with exponential backoff
        // WithAutomaticReconnect only works AFTER a successful connection
        for (var attempt = 0; attempt < InitialConnectionRetryDelaysMs.Length; attempt++)
        {
            var delay = InitialConnectionRetryDelaysMs[attempt];
            if (delay > 0)
            {
                _logger.LogInformation("Retrying SignalR connection in {DelayMs}ms (attempt {Attempt}/{Max})",
                    delay, attempt + 1, InitialConnectionRetryDelaysMs.Length);
                await Task.Delay(delay);
            }

            try
            {
                await _hubConnection.StartAsync();
                _logger.LogInformation("SignalR connected to dashboard hub");
                StartHeartbeatTimer();
                OnConnectionStateChanged?.Invoke(true);
                return; // Success - exit retry loop
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "SignalR connection attempt {Attempt}/{Max} failed",
                    attempt + 1, InitialConnectionRetryDelaysMs.Length);
            }
        }

        // All retries exhausted
        _logger.LogError("Failed to connect to dashboard hub after {Attempts} attempts",
            InitialConnectionRetryDelaysMs.Length);
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
        if (_hubConnection == null) return;

        _hubConnection.On<JsonElement>("ReceiveHealthData", data =>
        {
            _logger.LogDebug("Received Health data");
            OnHealthDataReceived?.Invoke(data);
        });

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
        if (_hubConnection == null || !IsConnected)
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
        if (_hubConnection == null || !IsConnected)
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

    public event Action<JsonElement>? OnHealthDataReceived;
    public event Action<JsonElement>? OnOverviewDataReceived;
    public event Action<JsonElement>? OnOrleansDataReceived;
    public event Action<JsonElement>? OnInsightsDataReceived;

    #endregion

    #region Data Fetch (via Hub)

    public async Task<JsonElement> GetHealthPageDataAsync()
    {
        return await InvokeAsync("GetHealthPageData");
    }

    public async Task<JsonElement> GetOverviewPageDataAsync()
    {
        return await InvokeAsync("GetOverviewPageData");
    }

    public async Task<JsonElement> GetOrleansPageDataAsync()
    {
        return await InvokeAsync("GetOrleansPageData");
    }

    public async Task<JsonElement> GetInsightsPageDataAsync()
    {
        return await InvokeAsync("GetInsightsPageData");
    }

    public async Task<JsonElement> GetSettingsPageDataAsync()
    {
        return await InvokeAsync("GetSettingsPageData");
    }

    public async Task<JsonElement> GetMethodProfileTrendAsync(
        string? grainType = null,
        string? methodName = null,
        TimeSpan? duration = null,
        int bucketSeconds = 1)
    {
        var durationSeconds = (duration ?? TimeSpan.FromMinutes(5)).TotalSeconds;
        _logger.LogDebug("GetMethodProfileTrendAsync: grainType={GrainType}, methodName={MethodName}, duration={Duration}s, bucket={Bucket}s",
            grainType, methodName, durationSeconds, bucketSeconds);
        return await InvokeAsync(
            "GetMethodProfileTrend",
            grainType,
            methodName,
            durationSeconds,
            bucketSeconds);
    }

    private async Task<JsonElement> InvokeAsync(string method, params object?[] args)
    {
        if (_hubConnection == null || !IsConnected)
        {
            _logger.LogWarning("Cannot invoke {Method}: not connected", method);
            return default;
        }

        try
        {
            // Use InvokeCoreAsync to pass args as individual parameters, not as a single array
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

    /// <summary>
    /// Starts the heartbeat timer to periodically refresh Orleans observer references.
    /// This prevents observer expiration in long-running sessions.
    /// </summary>
    private void StartHeartbeatTimer()
    {
        StopHeartbeatTimer();
        _heartbeatTimer = new System.Threading.Timer(
            _ => _ = HeartbeatAsync(),
            null,
            HeartbeatInterval,
            HeartbeatInterval);
        _logger.LogInformation("SignalR heartbeat started (interval: {Interval}s)", HeartbeatInterval.TotalSeconds);
    }

    /// <summary>
    /// Stops the heartbeat timer.
    /// </summary>
    private void StopHeartbeatTimer()
    {
        _heartbeatTimer?.Dispose();
        _heartbeatTimer = null;
    }

    /// <summary>
    /// Periodic heartbeat that re-subscribes to all current pages.
    /// This refreshes the Orleans observer references to prevent expiration.
    /// </summary>
    private async Task HeartbeatAsync()
    {
        if (_hubConnection == null || !IsConnected || _disposed)
        {
            _logger.LogDebug("Heartbeat skipped: connected={Connected}, disposed={Disposed}",
                IsConnected, _disposed);
            return;
        }

        string[] pagesToRefresh;
        lock (_subscribedPagesLock)
        {
            if (_subscribedPages.Count == 0)
            {
                _logger.LogDebug("Heartbeat: no subscribed pages");
                return;
            }
            pagesToRefresh = _subscribedPages.ToArray();
        }

        _logger.LogDebug("Heartbeat: refreshing {Count} page subscriptions", pagesToRefresh.Length);

        foreach (var page in pagesToRefresh)
        {
            try
            {
                // Re-subscribing refreshes the observer reference in the Orleans group grain
                await _hubConnection.InvokeAsync("SubscribeToPage", page);
                _logger.LogDebug("Heartbeat: refreshed subscription to {Page}", page);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Heartbeat: failed to refresh subscription to {Page}", page);
            }
        }
    }

    /// <summary>
    /// Re-subscribes to all tracked pages. Used after reconnection.
    /// </summary>
    private async Task ResubscribeAllPagesAsync()
    {
        string[] pagesToResubscribe;
        lock (_subscribedPagesLock)
        {
            if (_subscribedPages.Count == 0)
                return;
            pagesToResubscribe = _subscribedPages.ToArray();
        }

        _logger.LogInformation("Re-subscribing to {Count} pages after reconnection", pagesToResubscribe.Length);

        foreach (var page in pagesToResubscribe)
        {
            try
            {
                await _hubConnection!.InvokeAsync("SubscribeToPage", page);
                _logger.LogDebug("Re-subscribed to {Page}", page);
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

        if (_hubConnection != null)
        {
            await _hubConnection.DisposeAsync();
            _hubConnection = null;
        }
    }

    #endregion
}
