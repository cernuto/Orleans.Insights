using Orleans.Insights.Sample.Grains;

namespace Orleans.Insights.Sample.Services;

/// <summary>
/// Background service that simulates grain activity for demonstrating Orleans Insights dashboard.
/// </summary>
public class GrainActivitySimulator : BackgroundService
{
    private readonly IClusterClient _clusterClient;
    private readonly ILogger<GrainActivitySimulator> _logger;
    private static readonly Random _random = new();

    private static readonly string[] SensorIds = ["sensor-1", "sensor-2", "sensor-3", "sensor-4", "sensor-5"];
    private static readonly string[] CounterIds = ["page-views", "api-calls", "orders", "users", "sessions"];
    private static readonly string[] Products = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"];

    public GrainActivitySimulator(IClusterClient clusterClient, ILogger<GrainActivitySimulator> logger)
    {
        _clusterClient = clusterClient;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Grain activity simulator starting...");

        // Wait for silo to be ready
        await Task.Delay(2000, stoppingToken);

        _logger.LogInformation("Grain activity simulator running - generating traffic for dashboard");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Run multiple grain operations concurrently
                var tasks = new List<Task>
                {
                    SimulateCounterActivityAsync(),
                    SimulateTemperatureReadingsAsync(),
                    SimulateOrderProcessingAsync()
                };

                await Task.WhenAll(tasks);

                // Small delay between batches
                await Task.Delay(_random.Next(100, 500), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error in grain activity simulation");
                await Task.Delay(1000, stoppingToken);
            }
        }

        _logger.LogInformation("Grain activity simulator stopped");
    }

    private async Task SimulateCounterActivityAsync()
    {
        // Increment random counters
        var counterId = CounterIds[_random.Next(CounterIds.Length)];
        var counter = _clusterClient.GetGrain<ICounterGrain>(counterId);

        var count = await counter.IncrementAsync();

        // Occasionally read the count
        if (_random.Next(10) < 3)
        {
            await counter.GetCountAsync();
        }

        // Very occasionally reset
        if (_random.Next(100) < 1)
        {
            await counter.ResetAsync();
        }
    }

    private async Task SimulateTemperatureReadingsAsync()
    {
        // Update random sensors with temperature readings
        var sensorId = SensorIds[_random.Next(SensorIds.Length)];
        var sensor = _clusterClient.GetGrain<ITemperatureSensorGrain>(sensorId);

        // Generate realistic temperature (15-30°C with some variation)
        var temperature = 22.5 + (_random.NextDouble() - 0.5) * 10;
        await sensor.SetTemperatureAsync(temperature);

        // Occasionally get statistics
        if (_random.Next(10) < 2)
        {
            await sensor.GetStatisticsAsync();
        }

        // Sometimes just read current temperature
        if (_random.Next(10) < 4)
        {
            await sensor.GetTemperatureAsync();
        }
    }

    // Counter for order IDs - grows slowly over time
    private static int _orderCounter;

    private async Task SimulateOrderProcessingAsync()
    {
        // 80% chance to reuse an existing order ID, 20% chance to create a new one
        // This creates realistic growth that slows as more orders exist
        Guid orderId;
        if (_orderCounter > 0 && _random.Next(100) < 80)
        {
            // Reuse an existing order ID
            orderId = Guid.Parse($"00000000-0000-0000-0000-{_random.Next(_orderCounter):D12}");
        }
        else
        {
            // Create a new order (slow growth)
            orderId = Guid.Parse($"00000000-0000-0000-0000-{Interlocked.Increment(ref _orderCounter):D12}");
        }
        var processor = _clusterClient.GetGrain<IOrderProcessorGrain>(orderId);

        var product = Products[_random.Next(Products.Length)];
        var quantity = _random.Next(1, 10);

        try
        {
            await processor.ProcessOrderAsync(product, quantity);
        }
        catch (InvalidOperationException)
        {
            // Expected occasional failures - these will show up in the dashboard
        }

        // Check status
        await processor.GetStatusAsync();
    }
}
