using Orleans.Insights.Models;

namespace Orleans.Insights.Dashboard.Observers;

/// <summary>
/// Subscription metadata wrapper for a dashboard observer connection.
/// Tracks which pages a connection is subscribed to and holds the observer reference.
/// </summary>
public sealed class DashboardSubscription : IDisposable
{
    private readonly DashboardObserver _observer;
    private readonly HashSet<string> _subscribedPages = new(StringComparer.OrdinalIgnoreCase);
    private bool _disposed;

    public DashboardSubscription(DashboardObserver observer)
    {
        _observer = observer;
    }

    /// <summary>
    /// Gets the grain object reference for this observer.
    /// Set after creating the observer reference via ClusterClient.CreateObjectReference.
    /// </summary>
    public IDashboardObserver Reference { get; private set; } = default!;

    /// <summary>
    /// Gets the set of pages this subscription is subscribed to.
    /// </summary>
    public IReadOnlySet<string> SubscribedPages => _subscribedPages;

    /// <summary>
    /// Sets the grain object reference for this observer.
    /// </summary>
    public void SetReference(IDashboardObserver reference) => Reference = reference;

    /// <summary>
    /// Adds a page subscription.
    /// </summary>
    public void AddPage(string page) => _subscribedPages.Add(page);

    /// <summary>
    /// Removes a page subscription.
    /// </summary>
    public void RemovePage(string page) => _subscribedPages.Remove(page);

    /// <summary>
    /// Checks if this subscription is subscribed to a specific page.
    /// </summary>
    public bool IsSubscribedToPage(string page) => _subscribedPages.Contains(page);

    /// <summary>
    /// Gets the underlying observer instance.
    /// </summary>
    public DashboardObserver GetObserver() => _observer;

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _observer.Dispose();
        _subscribedPages.Clear();
        Reference = null!;
    }
}
