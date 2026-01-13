namespace Orleans.Insights.Sample.Grains;

/// <summary>
/// Simulated temperature sensor grain for demonstrating Orleans Insights monitoring.
/// </summary>
public interface ITemperatureSensorGrain : IGrainWithStringKey
{
    Task<double> GetTemperatureAsync();
    Task SetTemperatureAsync(double temperature);
    Task<(double Min, double Max, double Avg)> GetStatisticsAsync();
}
