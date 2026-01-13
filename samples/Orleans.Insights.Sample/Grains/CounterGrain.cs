namespace Orleans.Insights.Sample.Grains;

/// <summary>
/// Simple counter grain implementation.
/// </summary>
public class CounterGrain : Grain, ICounterGrain
{
    private int _count;

    public Task<int> IncrementAsync()
    {
        _count++;
        return Task.FromResult(_count);
    }

    public Task<int> GetCountAsync()
    {
        return Task.FromResult(_count);
    }

    public Task ResetAsync()
    {
        _count = 0;
        return Task.CompletedTask;
    }
}
