namespace Orleans.Insights.Sample.Grains;

/// <summary>
/// Order processor grain implementation.
/// Simulates order processing with variable latency to show in monitoring.
/// </summary>
public class OrderProcessorGrain : Grain, IOrderProcessorGrain
{
    private static readonly Random _random = new();
    private string _status = "Pending";
    private string _product = "";
    private int _quantity;

    public async Task<bool> ProcessOrderAsync(string product, int quantity)
    {
        _product = product;
        _quantity = quantity;
        _status = "Processing";

        // Simulate variable processing time (10-100ms)
        var delay = _random.Next(10, 100);
        await Task.Delay(delay);

        // Simulate occasional failures (5% chance)
        if (_random.Next(100) < 5)
        {
            _status = "Failed";
            throw new InvalidOperationException($"Failed to process order for {quantity}x {product}");
        }

        _status = "Completed";
        return true;
    }

    public Task<string> GetStatusAsync()
    {
        return Task.FromResult($"{_status}: {_quantity}x {_product}");
    }
}
