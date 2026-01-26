using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Insights.Dashboard.Hubs;
using Orleans.Insights.Dashboard.Observers;
using Orleans.Insights.Models;
using Orleans.Utilities;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Insights.Dashboard.Grains;

/// <summary>
/// Grain that broadcasts dashboard data to SignalR clients via Orleans observers.
///
/// Uses Orleans IGrainObserver pattern for push-based notifications with:
/// - ObserverManager for subscription tracking with automatic expiration
/// - ObserverHealthTracker for circuit breaker and failure detection
/// - Connection-to-observer mapping for targeted dispatch
///
/// Architecture:
///   InsightsGrain --[OneWay]--> DashboardBroadcastGrain --> ObserverManager --> IDashboardObserver --> SignalR --> Client
///
/// Note: This grain is NOT a StatelessWorker because it maintains observer state.
/// A single instance per silo handles all subscriptions.
/// </summary>
public sealed class DashboardBroadcastGrain : Grain, IDashboardBroadcastGrain
{
    private readonly ILogger<DashboardBroadcastGrain> _logger;
    private readonly DashboardObserverOptions _options;
    private readonly JsonSerializerOptions _jsonOptions;

    // Observer management infrastructure
    private readonly Dictionary<string, IDashboardObserver> _liveObservers = new(StringComparer.Ordinal);
    private readonly Dictionary<IDashboardObserver, string> _observerToConnectionId = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<string, HashSet<string>> _connectionToPages = new(StringComparer.Ordinal);

    private ObserverManager<IDashboardObserver>? _observerManager;
    private ObserverHealthTracker? _healthTracker;
    private IDisposable? _refreshTimer;
    private bool _gracePeriodEnabled;

    public DashboardBroadcastGrain(
        ILogger<DashboardBroadcastGrain> logger,
        IOptions<DashboardObserverOptions> options)
    {
        _logger = logger;
        _options = options.Value;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("DashboardBroadcastGrain activating");

        // Initialize observer manager with expiration
        _observerManager = new ObserverManager<IDashboardObserver>(
            _options.ObserverExpiration,
            _logger);

        // Initialize health tracker with circuit breaker
        _healthTracker = new ObserverHealthTracker(
            _options.ObserverFailureThreshold,
            _options.ObserverFailureWindow,
            _options.EnableCircuitBreaker,
            _options.CircuitBreakerOpenDuration,
            _options.CircuitBreakerHalfOpenTestInterval,
            _options.ObserverGracePeriod,
            _options.MaxBufferedMessagesPerObserver);

        _gracePeriodEnabled = _options.ObserverGracePeriod > TimeSpan.Zero;

        // Start refresh timer for observer maintenance
        if (_options.ObserverRefreshInterval > TimeSpan.Zero)
        {
            _refreshTimer = this.RegisterGrainTimer(
                RefreshObserversAsync,
                new GrainTimerCreationOptions(_options.ObserverRefreshInterval, _options.ObserverRefreshInterval)
                {
                    Interleave = true,
                    KeepAlive = false
                });
        }

        return base.OnActivateAsync(cancellationToken);
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _logger.LogInformation("DashboardBroadcastGrain deactivating: {Reason}", reason);

        _refreshTimer?.Dispose();
        _refreshTimer = null;

        _liveObservers.Clear();
        _observerToConnectionId.Clear();
        _connectionToPages.Clear();
        _healthTracker?.Clear();

        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    #region Observer Subscription

    /// <inheritdoc/>
    public Task Subscribe(string connectionId, string page, IDashboardObserver observer)
    {
        _logger.LogDebug("Subscribe: connectionId={ConnectionId}, page={Page}", connectionId, page);

        // Track observer in manager
        _observerManager!.Subscribe(observer, observer);

        // Track connection to observer mapping
        if (_liveObservers.TryGetValue(connectionId, out var existingObserver) && !ReferenceEquals(existingObserver, observer))
        {
            // Remove old mapping if observer changed
            _observerToConnectionId.Remove(existingObserver);
        }

        _liveObservers[connectionId] = observer;
        _observerToConnectionId[observer] = connectionId;

        // Track page subscriptions
        if (!_connectionToPages.TryGetValue(connectionId, out var pages))
        {
            pages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _connectionToPages[connectionId] = pages;
        }
        pages.Add(page);

        _logger.LogInformation(
            "Subscribed connection {ConnectionId} to page {Page}. Total observers: {Count}",
            connectionId, page, _liveObservers.Count);

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task Unsubscribe(string connectionId, string page)
    {
        _logger.LogDebug("Unsubscribe: connectionId={ConnectionId}, page={Page}", connectionId, page);

        if (_connectionToPages.TryGetValue(connectionId, out var pages))
        {
            pages.Remove(page);

            // If no more pages, untrack the connection entirely
            if (pages.Count == 0)
            {
                UntrackConnection(connectionId);
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task UnsubscribeAll(string connectionId)
    {
        _logger.LogDebug("UnsubscribeAll: connectionId={ConnectionId}", connectionId);
        UntrackConnection(connectionId);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task Touch(string connectionId, IDashboardObserver observer)
    {
        if (_liveObservers.ContainsKey(connectionId))
        {
            _observerManager!.Subscribe(observer, observer);
        }
        return Task.CompletedTask;
    }

    private void UntrackConnection(string connectionId)
    {
        if (_liveObservers.TryGetValue(connectionId, out var observer))
        {
            _observerManager!.Unsubscribe(observer);
            _liveObservers.Remove(connectionId);
            _observerToConnectionId.Remove(observer);
        }

        _connectionToPages.Remove(connectionId);
        _healthTracker!.RemoveConnection(connectionId);

        _logger.LogInformation(
            "Untracked connection {ConnectionId}. Total observers: {Count}",
            connectionId, _liveObservers.Count);
    }

    #endregion

    #region Broadcasting

    /// <inheritdoc/>
    public Task BroadcastOverviewData(OverviewPageData data)
    {
        DispatchToObservers(DashboardPages.Overview, observer => observer.OnOverviewData(data), data);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task BroadcastOrleansData(OrleansPageData data)
    {
        DispatchToObservers(DashboardPages.Orleans, observer => observer.OnOrleansData(data), data);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task BroadcastInsightsData(InsightsPageData data)
    {
        DispatchToObservers(DashboardPages.Insights, observer => observer.OnInsightsData(data), data);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Dispatches data to all observers subscribed to a specific page.
    /// Respects circuit breaker state and buffers messages during grace period.
    /// </summary>
    private void DispatchToObservers(string page, Func<IDashboardObserver, Task> dispatch, object data)
    {
        var dispatchCount = 0;

        foreach (var (connectionId, observer) in _liveObservers)
        {
            // Check if subscribed to this page
            if (!_connectionToPages.TryGetValue(connectionId, out var pages) || !pages.Contains(page))
            {
                continue;
            }

            // Check circuit breaker
            if (!_healthTracker!.AllowRequest(connectionId))
            {
                var state = _healthTracker.GetCircuitState(connectionId);
                if (state == CircuitState.Open)
                {
                    // Try to buffer if in grace period
                    if (_healthTracker.IsInGracePeriod(connectionId))
                    {
                        if (_healthTracker.BufferMessage(connectionId, data))
                        {
                            _logger.LogDebug("Buffered {Page} data for connection {ConnectionId} in grace period", page, connectionId);
                        }
                    }
                    continue;
                }
            }

            // Dispatch to observer
            var pending = dispatch(observer);
            _ = ObserveDispatchAsync(pending, connectionId, observer);
            dispatchCount++;
        }

        if (dispatchCount > 0)
        {
            _logger.LogDebug("Dispatched {Page} data to {Count} observers", page, dispatchCount);
        }
    }

    /// <summary>
    /// Observes the result of dispatching to an observer for health tracking.
    /// </summary>
    private async Task ObserveDispatchAsync(Task pending, string connectionId, IDashboardObserver observer)
    {
        try
        {
            await pending;
            _healthTracker!.RecordSuccess(connectionId);
        }
        catch (Exception ex)
        {
            var result = _healthTracker!.RecordFailure(connectionId, ex);

            switch (result)
            {
                case FailureResult.Dead:
                    _logger.LogWarning(
                        ex,
                        "Observer for connection {ConnectionId} exceeded failure threshold, marking as dead",
                        connectionId);
                    OnObserverDead(connectionId, observer);
                    break;

                case FailureResult.CircuitOpened:
                    _logger.LogWarning(
                        ex,
                        "Circuit breaker opened for connection {ConnectionId}",
                        connectionId);
                    OnCircuitOpened(connectionId);
                    break;

                case FailureResult.Healthy:
                default:
                    _logger.LogDebug(ex, "Observer dispatch failed for connection {ConnectionId}", connectionId);
                    break;
            }
        }
    }

    private void OnObserverDead(string connectionId, IDashboardObserver observer)
    {
        // Remove dead observer
        _liveObservers.Remove(connectionId);
        _observerToConnectionId.Remove(observer);
        _observerManager!.Unsubscribe(observer);
        _connectionToPages.Remove(connectionId);

        _logger.LogWarning("Removed dead observer for connection {ConnectionId}", connectionId);
    }

    private void OnCircuitOpened(string connectionId)
    {
        // Start grace period if enabled
        if (_gracePeriodEnabled && _healthTracker!.StartGracePeriod(connectionId))
        {
            _logger.LogDebug("Started grace period for connection {ConnectionId}", connectionId);
        }
    }

    #endregion

    #region Maintenance

    private Task RefreshObserversAsync()
    {
        if (_liveObservers.Count == 0)
        {
            return Task.CompletedTask;
        }

        // Clear expired from ObserverManager and synchronize with our tracking dictionaries
        _observerManager!.ClearExpired();
        SynchronizeExpiredObservers();

        // Cleanup expired grace periods
        if (_gracePeriodEnabled)
        {
            var expiredConnections = _healthTracker!.CleanupExpiredGracePeriods();
            if (expiredConnections.Count > 0)
            {
                _logger.LogInformation(
                    "Grace periods expired for {Count} connections. Buffered messages discarded.",
                    expiredConnections.Count);
            }
        }

        // Log statistics periodically
        if (_logger.IsEnabled(LogLevel.Debug))
        {
            var stats = _healthTracker!.GetStatistics();
            _logger.LogDebug(
                "Observer stats: Total={Total}, Closed={Closed}, Open={Open}, HalfOpen={HalfOpen}, Dead={Dead}, InGrace={InGrace}, Buffered={Buffered}",
                stats.TotalTracked, stats.ClosedCircuits, stats.OpenCircuits, stats.HalfOpenCircuits,
                stats.DeadObservers, stats.ObserversInGracePeriod, stats.TotalBufferedMessages);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Synchronizes our tracking dictionaries with ObserverManager after ClearExpired().
    /// Removes any connections whose observers have been marked as dead by health tracking.
    /// </summary>
    private void SynchronizeExpiredObservers()
    {
        // Use health tracker's dead observers list - these need cleanup from all tracking structures
        var deadObservers = _healthTracker!.GetDeadObservers();
        if (deadObservers.Count > 0)
        {
            foreach (var connectionId in deadObservers)
            {
                if (_liveObservers.TryGetValue(connectionId, out var observer))
                {
                    _liveObservers.Remove(connectionId);
                    _observerToConnectionId.Remove(observer);
                    _connectionToPages.Remove(connectionId);
                    _observerManager!.Unsubscribe(observer);

                    _logger.LogDebug("Cleaned up expired/dead observer for connection {ConnectionId}", connectionId);
                }
            }

            _logger.LogInformation(
                "Synchronized {Count} expired observers. Total observers: {Total}",
                deadObservers.Count, _liveObservers.Count);
        }
    }

    #endregion
}
