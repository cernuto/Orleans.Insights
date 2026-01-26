using Orleans.Insights.Models;

namespace Orleans.Insights.Dashboard.Observers;

// Note: IDashboardObserver is defined in Orleans.Insights.Models.GrainInterfaces

/// <summary>
/// Concrete observer implementation that wraps SignalR callback functions.
/// Created per-connection in the hub and registered with the broadcast grain.
/// </summary>
public sealed class DashboardObserver : IDashboardObserver, IDisposable
{
    private Func<OverviewPageData, Task>? _onOverview;
    private Func<OrleansPageData, Task>? _onOrleans;
    private Func<InsightsPageData, Task>? _onInsights;

    public DashboardObserver(
        Func<OverviewPageData, Task>? onOverview = null,
        Func<OrleansPageData, Task>? onOrleans = null,
        Func<InsightsPageData, Task>? onInsights = null)
    {
        _onOverview = onOverview;
        _onOrleans = onOrleans;
        _onInsights = onInsights;
    }

    /// <inheritdoc/>
    public async Task OnOverviewData(OverviewPageData data)
    {
        if (_onOverview != null)
        {
            await _onOverview.Invoke(data);
        }
    }

    /// <inheritdoc/>
    public async Task OnOrleansData(OrleansPageData data)
    {
        if (_onOrleans != null)
        {
            await _onOrleans.Invoke(data);
        }
    }

    /// <inheritdoc/>
    public async Task OnInsightsData(InsightsPageData data)
    {
        if (_onInsights != null)
        {
            await _onInsights.Invoke(data);
        }
    }

    /// <summary>
    /// Gets whether the observer has any active callbacks.
    /// </summary>
    public bool IsActive => _onOverview != null || _onOrleans != null || _onInsights != null;

    /// <inheritdoc/>
    public void Dispose()
    {
        _onOverview = null;
        _onOrleans = null;
        _onInsights = null;
    }
}
