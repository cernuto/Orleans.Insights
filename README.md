# Orleans.Insights

A modern, high-performance dashboard for Microsoft Orleans with DuckDB-powered time-series analytics.

## Features

- **Real-time monitoring** - SignalR-based push updates for live cluster visibility
- **DuckDB analytics** - In-memory time-series database for historical queries and trend analysis
- **Accurate method profiling** - True average calculations (not average of averages)
- **Anomaly detection** - Automatic detection of latency and error rate anomalies
- **Modern UI** - Blazor WebAssembly dashboard with responsive design
- **Trend analysis** - Historical trends for grains, methods, and cluster metrics

## Comparison with OrleansDashboard

| Feature | Orleans.Insights | OrleansDashboard |
|---------|-----------------|------------------|
| Storage | DuckDB (time-series) | In-memory only |
| Historical queries | Full SQL analytics | None |
| Method profiling | Accurate totals | Average of averages |
| Real-time updates | SignalR push | Polling |
| UI Framework | Blazor WASM | jQuery/Razor |
| Anomaly detection | Built-in | None |
| Trend analysis | Built-in | None |

## Installation

### NuGet Packages

```bash
dotnet add package Orleans.Insights
dotnet add package Orleans.Insights.Dashboard
```

### Silo Configuration

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.UseOrleans(siloBuilder =>
{
    siloBuilder
        .UseLocalhostClustering()
        .AddOrleansInsights(options =>
        {
            options.RetentionPeriod = TimeSpan.FromHours(24);
            options.BroadcastInterval = TimeSpan.FromSeconds(1);
        });
});
```

### Web Host Configuration

```csharp
var builder = WebApplication.CreateBuilder(args);

// Add dashboard services
builder.Services.AddOrleansInsightsDashboard();

var app = builder.Build();

// Map dashboard endpoints
app.MapOrleansInsightsDashboard("/dashboard");

app.Run();
```

## Architecture

```
Silos (Metrics Collection)
    │
    ▼
InsightsGrain (Singleton)
    │ ├── DuckDB (time-series storage)
    │ ├── In-memory aggregations
    │ └── Change detection
    ▼
DashboardBroadcastGrain (StatelessWorker)
    │
    ▼
SignalR Hub
    │
    ▼
Blazor WASM Dashboard
```

## Dashboard Pages

- **Overview** - High-level cluster summary with silo status
- **Orleans** - Detailed grain monitoring with method profiling
- **Health** - Health check status across silos
- **Insights** - Trend analysis and anomaly detection

## Configuration Options

```csharp
public class InsightsOptions
{
    // How long to retain historical data (default: 1 hour)
    public TimeSpan RetentionPeriod { get; set; }

    // Dashboard update frequency (default: 1 second)
    public TimeSpan BroadcastInterval { get; set; }

    // Batch flush threshold for DuckDB writes (default: 1000)
    public int BatchFlushThreshold { get; set; }

    // Require authentication for dashboard (default: false)
    public bool RequireAuthentication { get; set; }
}
```

## Requirements

- .NET 10.0+
- Microsoft Orleans 9.0+
- DuckDB.NET 1.2+

## License

MIT License - See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
