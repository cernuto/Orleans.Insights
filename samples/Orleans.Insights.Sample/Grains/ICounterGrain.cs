namespace Orleans.Insights.Sample.Grains;

/// <summary>
/// Simple counter grain for demonstrating Orleans Insights monitoring.
/// </summary>
public interface ICounterGrain : IGrainWithStringKey
{
    Task<int> IncrementAsync();
    Task<int> GetCountAsync();
    Task ResetAsync();
}
