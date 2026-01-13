namespace Orleans.Insights.Sample.Grains;

/// <summary>
/// Order processor grain for demonstrating Orleans Insights monitoring.
/// Simulates order processing with variable latency.
/// </summary>
public interface IOrderProcessorGrain : IGrainWithGuidKey
{
    Task<bool> ProcessOrderAsync(string product, int quantity);
    Task<string> GetStatusAsync();
}
