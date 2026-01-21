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
    │ ├── Channel-based buffer (non-blocking ingestion)
    │ ├── DuckDB (time-series storage with MVCC)
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

## Performance Design

Orleans.Insights is designed to minimize impact on your Orleans cluster:

### Non-Blocking Ingestion
Metrics are ingested via a lock-free `Channel<T>` with `BoundedChannelFullMode.DropOldest`. Producers never block - if the channel is full, the oldest metrics are dropped rather than causing backpressure. This ensures grain method calls are never delayed by metrics collection.

### MVCC Reads
DuckDB connections are duplicated via `Duplicate()` for read operations, enabling true MVCC (Multi-Version Concurrency Control). Queries run on the thread pool with their own connection while writes continue uninterrupted on the main connection.

### Cooperative Yielding
The background consumer yields control every 500 records, 50ms, or 3 batch flushes to prevent thread pool starvation. This ensures the silo remains responsive even under heavy metrics load.

### Bounded Memory Growth
In-memory aggregations use LRU eviction with configurable limits (`MaxMetricsEntries`). This prevents unbounded memory growth in long-running silos with many grain types.

## Projects

### Orleans.Insights

Core library that provides metrics collection and storage. Install this on every silo in your cluster.

**Key Components:**
- **GrainMethodProfiler** - Intercepts grain method calls to collect latency, throughput, and error metrics
- **SiloMetricsCollector** - Collects silo-level metrics (CPU, memory, activation counts) using `Process` APIs and `IManagementGrain.GetDetailedGrainStatistics()`
- **InsightsGrain** - Singleton grain that aggregates metrics from all silos and stores them in DuckDB
- **InsightsDatabase** - DuckDB wrapper for time-series storage with automatic retention cleanup
- **GrainTypeNameCache** - Caches grain type names using `Type.FullName` for consistent identification

**Metrics Collected:**
- Per-method: call count, latency (avg/min/max), error count, requests per second
- Per-silo: CPU usage, memory usage, activation counts per grain type
- Per-grain-type: total activations across cluster, aggregated method metrics

### Orleans.Insights.Dashboard

Server-side dashboard components. Install this on the host that serves the dashboard UI.

**Key Components:**
- **DashboardHub** - SignalR hub for real-time push updates to connected clients
- **DashboardBroadcastGrain** - StatelessWorker that broadcasts page data to SignalR groups
- **DashboardExtensions** - Extension methods for `IServiceCollection` and `IEndpointRouteBuilder` configuration

**Responsibilities:**
- Serves the Blazor WebAssembly client static files
- Manages SignalR connections and group subscriptions (one group per dashboard page)
- Routes page data from InsightsGrain to connected dashboard clients

### Orleans.Insights.Dashboard.Client

Blazor WebAssembly client application for the dashboard UI.

**Key Components:**
- **DashboardService** - SignalR client that subscribes to page-specific data streams
- **Dashboard Pages:**
  - **Overview** - Cluster summary with silo status cards
  - **Orleans** - Grain type list with method profiling charts
  - **Insights** - Slowest/busiest grains, trend analysis

**UI Features:**
- Real-time updates via SignalR (no polling)
- Interactive charts using a lightweight charting library
- Responsive grid layout for silo and grain cards

### Orleans.Insights.Sample

Sample application demonstrating Orleans.Insights integration.

**Contents:**
- Sample grains: `CounterGrain`, `TemperatureSensorGrain`, `OrderProcessorGrain`
- `GrainActivitySimulator` - Background service that generates grain traffic for demo purposes
- Serilog configuration for file-based logging

## Dashboard Pages

- **Overview** - High-level cluster summary with silo status
- **Orleans** - Detailed grain monitoring with method profiling
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

    // Capacity of the bounded channel for metrics buffering (default: 10,000)
    // When full, oldest items are dropped (backpressure handling)
    public int ChannelCapacity { get; set; }

    // Maximum grain types/methods to track before LRU eviction (default: 10,000)
    public int MaxMetricsEntries { get; set; }

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
