namespace Orleans.Insights.Sample.Grains;

/// <summary>
/// Simulated temperature sensor grain implementation.
/// </summary>
public class TemperatureSensorGrain : Grain, ITemperatureSensorGrain
{
    private readonly List<double> _readings = new();
    private double _currentTemperature = 20.0;

    public Task<double> GetTemperatureAsync()
    {
        return Task.FromResult(_currentTemperature);
    }

    public Task SetTemperatureAsync(double temperature)
    {
        _currentTemperature = temperature;
        _readings.Add(temperature);

        // Keep only last 100 readings
        if (_readings.Count > 100)
            _readings.RemoveAt(0);

        return Task.CompletedTask;
    }

    public Task<(double Min, double Max, double Avg)> GetStatisticsAsync()
    {
        if (_readings.Count == 0)
            return Task.FromResult((_currentTemperature, _currentTemperature, _currentTemperature));

        return Task.FromResult((_readings.Min(), _readings.Max(), _readings.Average()));
    }
}
