using Orleans.Insights.Database;
using Orleans.Insights.Models;
using Orleans.Insights.Schema;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Insights.Grains;

/// <summary>
/// Singleton grain providing deep historical analytics for Orleans cluster metrics.
/// Uses DuckDB for time-series storage with batch ingestion via Appender.
/// All aggregation queries are performed against DuckDB for persistence and consistency.
/// </summary>
/// <remarks>
/// <para>
/// This grain follows SOLID principles with extracted responsibilities:
/// - <see cref="InsightsMetricsBuffer"/>: Batch buffering and flush operations (SRP)
/// - <see cref="InsightsSchemaManager"/>: Schema initialization and retention (SRP)
/// - <see cref="InsightsQueryHelper"/>: Value extraction helpers (SRP)
/// </para>
/// <para>
/// Schema tables:
/// - cluster_metrics: Per-silo cluster snapshots (activations, CPU, memory, latency)
/// - grain_metrics: Per-grain-type performance metrics
/// - method_metrics: Per-method performance metrics
/// - method_profile: Per-method accurate totals (call count, elapsed time, exceptions)
/// </para>
/// <para>
/// Marked [Reentrant] to handle concurrent metric submissions from multiple silos
/// without blocking. This matches the OrleansDashboard pattern where submissions
/// are fire-and-forget and should not queue behind each other.
/// </para>
/// </remarks>
[Reentrant]
public partial class InsightsGrain : Grain, IInsightsGrain, IDisposable
{
    /// <summary>
    /// Frozen lookup table for InsightMetric to SQL ORDER BY clause mapping.
    /// Using FrozenDictionary for O(1) lookup with minimal overhead.
    /// </summary>
    private static readonly FrozenDictionary<InsightMetric, string> OrderByMapping =
        new Dictionary<InsightMetric, string>
        {
            [InsightMetric.Latency] = "avg_latency DESC",
            [InsightMetric.Requests] = "total_requests DESC",
            [InsightMetric.Errors] = "failed_requests DESC",
            [InsightMetric.Throughput] = "rps DESC",
            [InsightMetric.ErrorRate] = "error_rate DESC"
        }.ToFrozenDictionary();

    private readonly ILogger<InsightsGrain> _logger;
    private readonly IOptions<InsightsOptions> _settings;
    private readonly Stopwatch _vacuumStopwatch = new();
    private readonly Stopwatch _metricsStopwatch = new();

    private InsightsDatabase? _database;
    private InsightsMetricsBuffer? _buffer;
    private InsightsSchemaManager? _schemaManager;
    private IDisposable? _maintenanceTimer;
    private bool _disposed;

    public InsightsGrain(
        ILogger<InsightsGrain> logger,
        IOptions<InsightsOptions> settings)
    {
        _logger = logger;
        _settings = settings;
    }

    private InsightsDatabase Database => _database ?? throw new InvalidOperationException("Database not initialized");
    private InsightsMetricsBuffer Buffer => _buffer ?? throw new InvalidOperationException("Buffer not initialized");
    private InsightsOptions Settings => _settings.Value;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("InsightsGrain activated");

        // Create DuckDB database
        _database = new InsightsDatabase(_logger, "Insights");

        // Initialize schema using dedicated manager
        _schemaManager = new InsightsSchemaManager(_logger, _database);
        _schemaManager.InitializeSchema();

        // Create buffer for batch ingestion
        _buffer = new InsightsMetricsBuffer(_logger, Settings);

        // Start maintenance timer
        _vacuumStopwatch.Start();
        _metricsStopwatch.Start();

        _maintenanceTimer = this.RegisterGrainTimer(
            MaintenanceCallbackAsync,
            new GrainTimerCreationOptions(Settings.MaintenanceInterval, Settings.MaintenanceInterval)
            {
                Interleave = true,
                KeepAlive = false
            });

        // Start broadcast timer for real-time dashboard updates via SignalR
        StartBroadcastTimer();

        return base.OnActivateAsync(cancellationToken);
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _logger.LogInformation("InsightsGrain deactivating: {Reason}", reason);

        // Flush any remaining buffered data
        try
        {
            _buffer?.FlushTo(Database);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to flush buffers during deactivation");
        }

        Dispose();
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    #region Ingestion Methods

    /// <inheritdoc/>
    public Task IngestMetrics(SiloMetricsReport metrics)
    {
        try
        {
            Buffer.BufferSiloMetrics(metrics);

            if (Buffer.ShouldFlush())
            {
                Buffer.FlushTo(Database);
            }

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    "Buffered metrics from {SiloId}: {GrainTypes} grain types, {Methods} methods",
                    metrics.SiloId, metrics.GrainTypeMetrics.Count, metrics.MethodMetrics.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to buffer metrics from {SiloId}", metrics.SiloId);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task IngestMethodProfile(Immutable<MethodProfileReport> report)
    {
        var reportValue = report.Value;
        try
        {
            Buffer.BufferMethodProfile(reportValue);

            if (Buffer.ShouldFlush())
            {
                Buffer.FlushTo(Database);
            }

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    "IngestMethodProfile: Received {Methods} methods from {SiloId}",
                    reportValue.Entries.Count, reportValue.SiloId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to buffer method profile from {SiloId}", reportValue.SiloId);
        }

        return Task.CompletedTask;
    }

    #endregion

    #region Orleans Metrics Queries

    /// <inheritdoc/>
    public Task<List<ClusterMetricsTrend>> GetClusterTrend(TimeSpan duration, int bucketSeconds = 60)
    {
        var cutoff = DateTime.UtcNow - duration;

        var sql = $"""
            SELECT
                time_bucket(INTERVAL '{bucketSeconds} seconds', timestamp) as bucket,
                COUNT(DISTINCT silo_id) as silo_count,
                CAST(AVG(total_activations) AS BIGINT) as avg_activations,
                SUM(total_requests) as total_requests,
                AVG(avg_latency_ms) as avg_latency,
                AVG(cpu_usage_percent) as avg_cpu,
                SUM(memory_usage_mb) as total_memory,
                SUM(messages_dropped) as messages_dropped
            FROM cluster_metrics
            WHERE timestamp >= $1
            GROUP BY bucket
            ORDER BY bucket
            """;

        var results = Database.Query(sql, cutoff);

        var trends = results.Select(r => new ClusterMetricsTrend
        {
            Timestamp = InsightsQueryHelper.GetDateTime(r, "bucket"),
            SiloCount = InsightsQueryHelper.GetInt(r, "silo_count"),
            TotalActivations = InsightsQueryHelper.GetLong(r, "avg_activations"),
            TotalRequests = InsightsQueryHelper.GetLong(r, "total_requests"),
            AvgLatencyMs = InsightsQueryHelper.GetDouble(r, "avg_latency"),
            AvgCpuPercent = InsightsQueryHelper.GetDouble(r, "avg_cpu"),
            TotalMemoryMb = InsightsQueryHelper.GetLong(r, "total_memory"),
            MessagesDropped = InsightsQueryHelper.GetLong(r, "messages_dropped"),
            RequestsPerSecond = bucketSeconds > 0 ? InsightsQueryHelper.GetLong(r, "total_requests") / (double)bucketSeconds : 0
        }).ToList();

        return Task.FromResult(trends);
    }

    /// <inheritdoc/>
    public Task<List<GrainTypeInsight>> GetTopGrainTypes(InsightMetric metric, int count = 10, TimeSpan? duration = null)
    {
        var cutoff = DateTime.UtcNow - (duration ?? Settings.DefaultQueryDuration);
        var windowSeconds = (duration ?? Settings.DefaultQueryDuration).TotalSeconds;

        // Map metric to ORDER BY clause for method_profile table
        var orderBy = metric switch
        {
            InsightMetric.Latency => "avg_latency DESC",
            InsightMetric.Requests => "total_requests DESC",
            InsightMetric.Errors => "failed_requests DESC",
            InsightMetric.Throughput => "rps DESC",
            InsightMetric.ErrorRate => "error_rate DESC",
            _ => "avg_latency DESC"
        };

        // Query from method_profile table (populated by GrainMethodProfiler) instead of grain_metrics
        // This is where actual grain call data is stored
        var sql = $"""
            SELECT
                grain_type,
                SUM(call_count) as total_requests,
                SUM(exception_count) as failed_requests,
                CASE WHEN SUM(call_count) > 0
                     THEN SUM(total_elapsed_ms) / SUM(call_count)
                     ELSE 0 END as avg_latency,
                MIN(CASE WHEN call_count > 0 THEN total_elapsed_ms / call_count ELSE NULL END) as min_latency,
                MAX(CASE WHEN call_count > 0 THEN total_elapsed_ms / call_count ELSE NULL END) as max_latency,
                SUM(call_count) / {windowSeconds:F2} as rps,
                CASE WHEN SUM(call_count) > 0
                     THEN (SUM(exception_count)::DOUBLE / SUM(call_count)) * 100
                     ELSE 0 END as error_rate,
                COUNT(DISTINCT silo_id) as silo_count
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type
            ORDER BY {orderBy}
            LIMIT {count}
            """;

        var results = Database.Query(sql, cutoff);

        var resultsSpan = CollectionsMarshal.AsSpan(results);
        var insights = new List<GrainTypeInsight>(resultsSpan.Length);

        foreach (ref readonly var r in resultsSpan)
        {
            insights.Add(new GrainTypeInsight
            {
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                TotalRequests = InsightsQueryHelper.GetLong(r, "total_requests"),
                FailedRequests = InsightsQueryHelper.GetLong(r, "failed_requests"),
                AvgLatencyMs = InsightsQueryHelper.GetDouble(r, "avg_latency"),
                MinLatencyMs = InsightsQueryHelper.GetDouble(r, "min_latency"),
                MaxLatencyMs = InsightsQueryHelper.GetDouble(r, "max_latency"),
                RequestsPerSecond = InsightsQueryHelper.GetDouble(r, "rps"),
                ErrorRate = InsightsQueryHelper.GetDouble(r, "error_rate"),
                SiloCount = InsightsQueryHelper.GetInt(r, "silo_count")
            });
        }

        return Task.FromResult(insights);
    }

    /// <inheritdoc/>
    public Task<List<MethodInsight>> GetTopMethods(InsightMetric metric, int count = 10, TimeSpan? duration = null)
    {
        var cutoff = DateTime.UtcNow - (duration ?? Settings.DefaultQueryDuration);
        var windowSeconds = (duration ?? Settings.DefaultQueryDuration).TotalSeconds;

        // Map metric to ORDER BY clause for method_profile table
        var orderBy = metric switch
        {
            InsightMetric.Latency => "avg_latency DESC",
            InsightMetric.Requests => "total_requests DESC",
            InsightMetric.Errors => "failed_requests DESC",
            InsightMetric.Throughput => "rps DESC",
            InsightMetric.ErrorRate => "error_rate DESC",
            _ => "avg_latency DESC"
        };

        // Query from method_profile table (populated by GrainMethodProfiler) instead of method_metrics
        var sql = $"""
            SELECT
                grain_type,
                method_name,
                SUM(call_count) as total_requests,
                SUM(exception_count) as failed_requests,
                CASE WHEN SUM(call_count) > 0
                     THEN SUM(total_elapsed_ms) / SUM(call_count)
                     ELSE 0 END as avg_latency,
                SUM(call_count) / {windowSeconds:F2} as rps,
                CASE WHEN SUM(call_count) > 0
                     THEN (SUM(exception_count)::DOUBLE / SUM(call_count)) * 100
                     ELSE 0 END as error_rate,
                COUNT(DISTINCT silo_id) as silo_count
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type, method_name
            ORDER BY {orderBy}
            LIMIT {count}
            """;

        var results = Database.Query(sql, cutoff);

        var resultsSpan = CollectionsMarshal.AsSpan(results);
        var insights = new List<MethodInsight>(resultsSpan.Length);

        foreach (ref readonly var r in resultsSpan)
        {
            insights.Add(new MethodInsight
            {
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                MethodName = InsightsQueryHelper.GetString(r, "method_name"),
                TotalRequests = InsightsQueryHelper.GetLong(r, "total_requests"),
                FailedRequests = InsightsQueryHelper.GetLong(r, "failed_requests"),
                AvgLatencyMs = InsightsQueryHelper.GetDouble(r, "avg_latency"),
                RequestsPerSecond = InsightsQueryHelper.GetDouble(r, "rps"),
                ErrorRate = InsightsQueryHelper.GetDouble(r, "error_rate"),
                SiloCount = InsightsQueryHelper.GetInt(r, "silo_count")
            });
        }

        return Task.FromResult(insights);
    }

    /// <inheritdoc/>
    public Task<List<LatencyTrendPoint>> GetGrainLatencyTrend(string grainType, TimeSpan duration, int bucketSeconds = 60)
    {
        var cutoff = DateTime.UtcNow - duration;

        // Query from method_profile table using accurate totals calculation
        var sql = $"""
            SELECT
                time_bucket(INTERVAL '{bucketSeconds} seconds', timestamp) as bucket,
                CASE WHEN SUM(call_count) > 0
                     THEN SUM(total_elapsed_ms) / SUM(call_count)
                     ELSE 0 END as avg_latency,
                MIN(CASE WHEN call_count > 0 THEN total_elapsed_ms / call_count ELSE NULL END) as min_latency,
                MAX(CASE WHEN call_count > 0 THEN total_elapsed_ms / call_count ELSE NULL END) as max_latency,
                SUM(call_count) as request_count,
                SUM(call_count) / {bucketSeconds}.0 as rps
            FROM method_profile
            WHERE timestamp >= $1 AND grain_type = $2
            GROUP BY bucket
            ORDER BY bucket
            """;

        var results = Database.Query(sql, cutoff, grainType);

        var points = results.Select(r => new LatencyTrendPoint
        {
            Timestamp = InsightsQueryHelper.GetDateTime(r, "bucket"),
            AvgLatencyMs = InsightsQueryHelper.GetDouble(r, "avg_latency"),
            MinLatencyMs = InsightsQueryHelper.GetDouble(r, "min_latency"),
            MaxLatencyMs = InsightsQueryHelper.GetDouble(r, "max_latency"),
            RequestCount = InsightsQueryHelper.GetLong(r, "request_count"),
            RequestsPerSecond = InsightsQueryHelper.GetDouble(r, "rps")
        }).ToList();

        return Task.FromResult(points);
    }

    /// <inheritdoc/>
    public Task<List<LatencyTrendPoint>> GetMethodLatencyTrend(string grainType, string methodName, TimeSpan duration, int bucketSeconds = 60)
    {
        var cutoff = DateTime.UtcNow - duration;

        // Query from method_profile table using accurate totals calculation
        var sql = $"""
            SELECT
                time_bucket(INTERVAL '{bucketSeconds} seconds', timestamp) as bucket,
                CASE WHEN SUM(call_count) > 0
                     THEN SUM(total_elapsed_ms) / SUM(call_count)
                     ELSE 0 END as avg_latency,
                MIN(CASE WHEN call_count > 0 THEN total_elapsed_ms / call_count ELSE NULL END) as min_latency,
                MAX(CASE WHEN call_count > 0 THEN total_elapsed_ms / call_count ELSE NULL END) as max_latency,
                SUM(call_count) as request_count,
                SUM(call_count) / {bucketSeconds}.0 as rps
            FROM method_profile
            WHERE timestamp >= $1 AND grain_type = $2 AND method_name = $3
            GROUP BY bucket
            ORDER BY bucket
            """;

        var results = Database.Query(sql, cutoff, grainType, methodName);

        var points = results.Select(r => new LatencyTrendPoint
        {
            Timestamp = InsightsQueryHelper.GetDateTime(r, "bucket"),
            AvgLatencyMs = InsightsQueryHelper.GetDouble(r, "avg_latency"),
            MinLatencyMs = InsightsQueryHelper.GetDouble(r, "min_latency"),
            MaxLatencyMs = InsightsQueryHelper.GetDouble(r, "max_latency"),
            RequestCount = InsightsQueryHelper.GetLong(r, "request_count"),
            RequestsPerSecond = InsightsQueryHelper.GetDouble(r, "rps")
        }).ToList();

        return Task.FromResult(points);
    }

    /// <inheritdoc/>
    public Task<AnomalyReport> DetectAnomalies(TimeSpan recentWindow, TimeSpan baselineWindow, double thresholdMultiplier = 2.0)
    {
        var now = DateTime.UtcNow;
        var recentCutoff = now - recentWindow;
        var baselineCutoff = now - baselineWindow;

        // Detect latency anomalies for grain types using method_profile table
        var latencyAnomalySql = """
            WITH recent AS (
                SELECT grain_type,
                       CASE WHEN SUM(call_count) > 0
                            THEN SUM(total_elapsed_ms) / SUM(call_count)
                            ELSE 0 END as current_latency
                FROM method_profile
                WHERE timestamp >= $1
                GROUP BY grain_type
            ),
            baseline AS (
                SELECT grain_type,
                       CASE WHEN SUM(call_count) > 0
                            THEN SUM(total_elapsed_ms) / SUM(call_count)
                            ELSE 0 END as baseline_latency
                FROM method_profile
                WHERE timestamp >= $2 AND timestamp < $1
                GROUP BY grain_type
            )
            SELECT
                r.grain_type,
                r.current_latency,
                b.baseline_latency,
                r.current_latency / NULLIF(b.baseline_latency, 0) as deviation
            FROM recent r
            JOIN baseline b ON r.grain_type = b.grain_type
            WHERE b.baseline_latency > 0
              AND r.current_latency > b.baseline_latency * $3
            ORDER BY deviation DESC
            """;

        var latencyResults = Database.Query(latencyAnomalySql, recentCutoff, baselineCutoff, thresholdMultiplier);

        var latencyAnomalies = latencyResults.Select(r => new LatencyAnomaly
        {
            GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
            MethodName = null,
            CurrentLatencyMs = InsightsQueryHelper.GetDouble(r, "current_latency"),
            BaselineLatencyMs = InsightsQueryHelper.GetDouble(r, "baseline_latency"),
            DeviationMultiplier = InsightsQueryHelper.GetDouble(r, "deviation")
        }).ToList();

        // Detect error rate anomalies using method_profile table
        var errorAnomalySql = """
            WITH recent AS (
                SELECT grain_type,
                       CASE WHEN SUM(call_count) > 0
                            THEN (SUM(exception_count)::DOUBLE / SUM(call_count)) * 100
                            ELSE 0 END as current_error_rate
                FROM method_profile
                WHERE timestamp >= $1
                GROUP BY grain_type
            ),
            baseline AS (
                SELECT grain_type,
                       CASE WHEN SUM(call_count) > 0
                            THEN (SUM(exception_count)::DOUBLE / SUM(call_count)) * 100
                            ELSE 0 END as baseline_error_rate
                FROM method_profile
                WHERE timestamp >= $2 AND timestamp < $1
                GROUP BY grain_type
            )
            SELECT
                r.grain_type,
                r.current_error_rate,
                b.baseline_error_rate,
                CASE WHEN b.baseline_error_rate > 0
                     THEN r.current_error_rate / b.baseline_error_rate
                     ELSE 0 END as deviation
            FROM recent r
            JOIN baseline b ON r.grain_type = b.grain_type
            WHERE r.current_error_rate > 1
              AND (b.baseline_error_rate = 0 OR r.current_error_rate > b.baseline_error_rate * $3)
            ORDER BY r.current_error_rate DESC
            """;

        var errorResults = Database.Query(errorAnomalySql, recentCutoff, baselineCutoff, thresholdMultiplier);

        var errorAnomalies = errorResults.Select(r => new ErrorRateAnomaly
        {
            GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
            MethodName = null,
            CurrentErrorRate = InsightsQueryHelper.GetDouble(r, "current_error_rate"),
            BaselineErrorRate = InsightsQueryHelper.GetDouble(r, "baseline_error_rate"),
            DeviationMultiplier = InsightsQueryHelper.GetDouble(r, "deviation")
        }).ToList();

        var report = new AnomalyReport
        {
            GeneratedAt = now,
            RecentWindow = recentWindow,
            BaselineWindow = baselineWindow,
            ThresholdMultiplier = thresholdMultiplier,
            LatencyAnomalies = latencyAnomalies,
            ErrorRateAnomalies = errorAnomalies
        };

        return Task.FromResult(report);
    }

    /// <inheritdoc/>
    public Task<List<SiloComparison>> CompareSilos(TimeSpan duration)
    {
        var cutoff = DateTime.UtcNow - duration;

        // Combine cluster_metrics (CPU, memory) with method_profile (latency, requests)
        var sql = """
            WITH cluster_stats AS (
                SELECT
                    silo_id,
                    MAX(host_name) as host_name,
                    AVG(cpu_usage_percent) as avg_cpu,
                    CAST(AVG(memory_usage_mb) AS BIGINT) as avg_memory,
                    SUM(messages_dropped) as messages_dropped
                FROM cluster_metrics
                WHERE timestamp >= $1
                GROUP BY silo_id
            ),
            method_stats AS (
                SELECT
                    silo_id,
                    CASE WHEN SUM(call_count) > 0
                         THEN SUM(total_elapsed_ms) / SUM(call_count)
                         ELSE 0 END as avg_latency,
                    SUM(call_count) as total_requests
                FROM method_profile
                WHERE timestamp >= $1
                GROUP BY silo_id
            ),
            silo_stats AS (
                SELECT
                    COALESCE(c.silo_id, m.silo_id) as silo_id,
                    COALESCE(c.host_name, '') as host_name,
                    COALESCE(c.avg_cpu, 0) as avg_cpu,
                    COALESCE(c.avg_memory, 0) as avg_memory,
                    COALESCE(m.avg_latency, 0) as avg_latency,
                    COALESCE(m.total_requests, 0) as total_requests,
                    COALESCE(c.messages_dropped, 0) as messages_dropped
                FROM cluster_stats c
                FULL OUTER JOIN method_stats m ON c.silo_id = m.silo_id
            ),
            cluster_avg AS (
                SELECT
                    AVG(avg_latency) as cluster_latency,
                    AVG(avg_cpu) as cluster_cpu
                FROM silo_stats
                WHERE avg_latency > 0
            )
            SELECT
                s.silo_id,
                s.host_name,
                s.avg_cpu,
                s.avg_memory,
                s.avg_latency,
                s.total_requests,
                s.messages_dropped,
                CASE WHEN c.cluster_latency > 0 AND s.avg_latency > 0
                     THEN s.avg_latency / c.cluster_latency
                     ELSE 1 END as performance_score
            FROM silo_stats s, cluster_avg c
            ORDER BY performance_score DESC
            """;

        var results = Database.Query(sql, cutoff);

        var durationSeconds = duration.TotalSeconds;

        var comparisons = results.Select(r => new SiloComparison
        {
            SiloId = InsightsQueryHelper.GetString(r, "silo_id"),
            HostName = InsightsQueryHelper.GetString(r, "host_name"),
            AvgCpuPercent = InsightsQueryHelper.GetDouble(r, "avg_cpu"),
            AvgMemoryMb = InsightsQueryHelper.GetLong(r, "avg_memory"),
            AvgLatencyMs = InsightsQueryHelper.GetDouble(r, "avg_latency"),
            TotalRequests = InsightsQueryHelper.GetLong(r, "total_requests"),
            RequestsPerSecond = durationSeconds > 0 ? InsightsQueryHelper.GetLong(r, "total_requests") / durationSeconds : 0,
            MessagesDropped = InsightsQueryHelper.GetLong(r, "messages_dropped"),
            PerformanceScore = InsightsQueryHelper.GetDouble(r, "performance_score")
        }).ToList();

        return Task.FromResult(comparisons);
    }

    #endregion

    #region Real-Time Cluster Aggregation

    /// <inheritdoc/>
    public Task<AggregatedClusterMetrics> GetAggregatedMetrics()
    {
        // Query the most recent cluster metrics from each silo (last 30 seconds)
        var cutoff = DateTime.UtcNow.AddSeconds(-30);

        // Get latest metrics per silo
        var siloMetricsSql = """
            WITH latest AS (
                SELECT silo_id, MAX(timestamp) as max_ts
                FROM cluster_metrics
                WHERE timestamp >= $1
                GROUP BY silo_id
            )
            SELECT c.*
            FROM cluster_metrics c
            INNER JOIN latest l ON c.silo_id = l.silo_id AND c.timestamp = l.max_ts
            """;

        var siloResults = Database.Query(siloMetricsSql, cutoff);
        var siloCount = siloResults.Count;

        if (siloCount == 0)
        {
            return Task.FromResult(new AggregatedClusterMetrics
            {
                ReportingSiloCount = 0,
                Timestamp = DateTime.UtcNow,
                ClusterMetrics = new AggregatedOrleansMetrics()
            });
        }

        // Aggregate cluster metrics
        long totalActivations = 0;
        int totalConnectedClients = 0;
        long totalMessagesSent = 0;
        long totalMessagesReceived = 0;
        long totalMessagesDropped = 0;
        double totalCpuUsage = 0;
        long totalMemoryUsage = 0;
        long totalAvailableMemory = 0;
        long totalRequests = 0;
        double weightedLatencySum = 0;
        // Catalog metrics
        long totalActivationWorkingSet = 0;
        long totalActivationsCreated = 0;
        long totalActivationsDestroyed = 0;
        long totalActivationsFailedToActivate = 0;
        long totalActivationCollections = 0;
        long totalActivationShutdowns = 0;
        long totalActivationNonExistent = 0;
        long totalConcurrentRegistrationAttempts = 0;
        // Miscellaneous grain metrics
        long totalGrainCount = 0;
        long totalSystemTargets = 0;

        foreach (var row in siloResults)
        {
            totalActivations += InsightsQueryHelper.GetLong(row, "total_activations");
            totalConnectedClients += InsightsQueryHelper.GetInt(row, "connected_clients");
            totalMessagesSent += InsightsQueryHelper.GetLong(row, "messages_sent");
            totalMessagesReceived += InsightsQueryHelper.GetLong(row, "messages_received");
            totalMessagesDropped += InsightsQueryHelper.GetLong(row, "messages_dropped");
            totalCpuUsage += InsightsQueryHelper.GetDouble(row, "cpu_usage_percent");
            totalMemoryUsage += InsightsQueryHelper.GetLong(row, "memory_usage_mb");
            totalAvailableMemory += InsightsQueryHelper.GetLong(row, "available_memory_mb");

            var requests = InsightsQueryHelper.GetLong(row, "total_requests");
            var latency = InsightsQueryHelper.GetDouble(row, "avg_latency_ms");
            totalRequests += requests;
            weightedLatencySum += latency * requests;

            // Catalog metrics (sum across all silos)
            totalActivationWorkingSet += InsightsQueryHelper.GetLong(row, "activation_working_set");
            totalActivationsCreated += InsightsQueryHelper.GetLong(row, "activations_created");
            totalActivationsDestroyed += InsightsQueryHelper.GetLong(row, "activations_destroyed");
            totalActivationsFailedToActivate += InsightsQueryHelper.GetLong(row, "activations_failed_to_activate");
            totalActivationCollections += InsightsQueryHelper.GetLong(row, "activation_collections");
            totalActivationShutdowns += InsightsQueryHelper.GetLong(row, "activation_shutdowns");
            totalActivationNonExistent += InsightsQueryHelper.GetLong(row, "activation_non_existent");
            totalConcurrentRegistrationAttempts += InsightsQueryHelper.GetLong(row, "concurrent_registration_attempts");
            // Miscellaneous grain metrics
            totalGrainCount += InsightsQueryHelper.GetLong(row, "grain_count");
            totalSystemTargets += InsightsQueryHelper.GetLong(row, "system_targets");
        }

        var avgLatency = totalRequests > 0 ? weightedLatencySum / totalRequests : 0;
        var avgCpu = siloCount > 0 ? totalCpuUsage / siloCount : 0;

        var clusterMetrics = new AggregatedOrleansMetrics
        {
            TotalActivations = totalActivations,
            TotalConnectedClients = totalConnectedClients,
            TotalMessagesSent = totalMessagesSent,
            TotalMessagesReceived = totalMessagesReceived,
            TotalMessagesDropped = totalMessagesDropped,
            AverageCpuUsagePercent = avgCpu,
            TotalMemoryUsageMb = totalMemoryUsage,
            TotalAvailableMemoryMb = totalAvailableMemory,
            AverageRequestLatencyMs = avgLatency,
            TotalRequests = totalRequests,
            // Catalog metrics
            TotalActivationWorkingSet = totalActivationWorkingSet,
            TotalActivationsCreated = totalActivationsCreated,
            TotalActivationsDestroyed = totalActivationsDestroyed,
            TotalActivationsFailedToActivate = totalActivationsFailedToActivate,
            TotalActivationCollections = totalActivationCollections,
            TotalActivationShutdowns = totalActivationShutdowns,
            TotalActivationNonExistent = totalActivationNonExistent,
            TotalConcurrentRegistrationAttempts = totalConcurrentRegistrationAttempts,
            // Miscellaneous grain metrics
            TotalGrainCount = totalGrainCount,
            TotalSystemTargets = totalSystemTargets
        };

        // Aggregate grain type metrics
        var grainTypeMetrics = AggregateGrainTypeMetrics(cutoff);

        // Aggregate method metrics from database
        var methodMetrics = AggregateMethodMetrics(cutoff);

        var result = new AggregatedClusterMetrics
        {
            ReportingSiloCount = siloCount,
            Timestamp = DateTime.UtcNow,
            ClusterMetrics = clusterMetrics,
            GrainTypeMetrics = grainTypeMetrics,
            MethodMetrics = methodMetrics
        };

        return Task.FromResult(result);
    }

    /// <inheritdoc/>
    public Task<SiloMetricsInfo[]> GetReportingSilos()
    {
        var now = DateTime.UtcNow;
        var cutoff = now.AddSeconds(-60); // Consider silos from last minute
        var staleThreshold = now.AddSeconds(-30); // Stale if no report in 30 seconds

        var sql = """
            SELECT
                silo_id,
                MAX(host_name) as host_name,
                MAX(timestamp) as last_report_time
            FROM cluster_metrics
            WHERE timestamp >= $1
            GROUP BY silo_id
            ORDER BY last_report_time DESC
            """;

        var results = Database.Query(sql, cutoff);

        var silos = results.Select(row =>
        {
            var lastReport = InsightsQueryHelper.GetDateTime(row, "last_report_time");
            return new SiloMetricsInfo
            {
                SiloId = InsightsQueryHelper.GetString(row, "silo_id"),
                HostName = InsightsQueryHelper.GetString(row, "host_name"),
                LastReportTime = lastReport,
                IsStale = lastReport < staleThreshold
            };
        }).ToArray();

        return Task.FromResult(silos);
    }

    private Dictionary<string, AggregatedGrainTypeMetrics> AggregateGrainTypeMetrics(DateTime cutoff)
    {
        // Query DuckDB method_profile table and aggregate by grain type
        var now = DateTime.UtcNow;
        var windowSeconds = (now - cutoff).TotalSeconds;

        var sql = """
            SELECT
                grain_type,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions,
                MIN(CASE WHEN call_count > 0 THEN total_elapsed_ms / call_count ELSE NULL END) as min_latency,
                MAX(CASE WHEN call_count > 0 THEN total_elapsed_ms / call_count ELSE NULL END) as max_latency,
                COUNT(DISTINCT silo_id) as silo_count,
                MAX(timestamp) as last_updated
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type
            """;

        var results = Database.Query(sql, cutoff);
        var dict = new Dictionary<string, AggregatedGrainTypeMetrics>(results.Count);

        foreach (var row in results)
        {
            var grainType = InsightsQueryHelper.GetString(row, "grain_type");
            var totalCalls = InsightsQueryHelper.GetLong(row, "total_calls");
            var totalElapsed = InsightsQueryHelper.GetDouble(row, "total_elapsed");
            var totalExceptions = InsightsQueryHelper.GetLong(row, "total_exceptions");
            var minLatency = InsightsQueryHelper.GetDouble(row, "min_latency");
            var maxLatency = InsightsQueryHelper.GetDouble(row, "max_latency");
            var siloCount = InsightsQueryHelper.GetInt(row, "silo_count");
            var lastUpdated = InsightsQueryHelper.GetDateTime(row, "last_updated");

            var avgLatency = totalCalls > 0 ? totalElapsed / totalCalls : 0;
            var exceptionRate = totalCalls > 0 ? (totalExceptions / (double)totalCalls) * 100 : 0;
            var rps = windowSeconds > 0 ? totalCalls / windowSeconds : 0;

            dict[grainType] = new AggregatedGrainTypeMetrics
            {
                GrainType = grainType,
                TotalRequests = totalCalls,
                FailedRequests = totalExceptions,
                AverageLatencyMs = avgLatency,
                MinLatencyMs = minLatency,
                MaxLatencyMs = maxLatency,
                RequestsPerSecond = rps,
                ExceptionRate = exceptionRate,
                ReportingSiloCount = siloCount,
                LastUpdated = lastUpdated,
                TotalActivations = 0 // Will be populated from grain_type_activations
            };
        }

        // Query per-grain-type total activations (sum across all silos)
        // Note: grain_type_activations stores "Namespace.TypeName,AssemblyName" format
        // while method_profile stores "Namespace.TypeName" format (Type.FullName without assembly)
        var activationsSql = """
            WITH latest AS (
                SELECT grain_type, silo_id, MAX(timestamp) as max_ts
                FROM grain_type_activations
                WHERE timestamp >= $1
                GROUP BY grain_type, silo_id
            )
            SELECT a.grain_type, SUM(a.activations) as total_activations
            FROM grain_type_activations a
            INNER JOIN latest l ON a.grain_type = l.grain_type AND a.silo_id = l.silo_id AND a.timestamp = l.max_ts
            GROUP BY a.grain_type
            """;

        var activationResults = Database.Query(activationsSql, cutoff);

        // Match activation grain types to method_profile grain types
        // grain_type_activations has "Namespace.TypeName,AssemblyName" format
        // method_profile has "Namespace.TypeName" format
        foreach (var row in activationResults)
        {
            var activationGrainType = InsightsQueryHelper.GetString(row, "grain_type");
            var totalActivations = InsightsQueryHelper.GetLong(row, "total_activations");

            // Strip assembly suffix to get just "Namespace.TypeName"
            var normalizedType = activationGrainType;
            var commaIndex = activationGrainType.LastIndexOf(',');
            if (commaIndex > 0)
            {
                normalizedType = activationGrainType[..commaIndex];
            }

            if (dict.TryGetValue(normalizedType, out var existing))
            {
                dict[normalizedType] = existing with { TotalActivations = totalActivations };
            }
            else
            {
                // Grain type has activations but no method calls yet - use the normalized type as key
                dict[normalizedType] = new AggregatedGrainTypeMetrics
                {
                    GrainType = normalizedType,
                    TotalActivations = totalActivations,
                    LastUpdated = DateTime.UtcNow
                };
            }
        }

        _logger.LogDebug("AggregateGrainTypeMetrics: Found {GrainCount} grain types from DuckDB", dict.Count);
        return dict;
    }

    private Dictionary<string, AggregatedMethodMetrics> AggregateMethodMetrics(DateTime cutoff)
    {
        // Query DuckDB method_profile table for method metrics
        var now = DateTime.UtcNow;
        var windowSeconds = (now - cutoff).TotalSeconds;

        // Query 1: Get aggregated metrics per method with silo breakdown
        var siloSql = """
            SELECT
                grain_type,
                method_name,
                silo_id,
                MAX(host_name) as host_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions,
                MAX(timestamp) as last_updated
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type, method_name, silo_id
            ORDER BY grain_type, method_name, total_calls DESC
            """;

        var siloResults = Database.Query(siloSql, cutoff);

        // Query 2: Get time-series samples (1-second buckets) for charts
        var timeSeriesSql = """
            SELECT
                grain_type,
                method_name,
                time_bucket(INTERVAL '1 second', timestamp) as bucket,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type, method_name, bucket
            ORDER BY grain_type, method_name, bucket
            """;

        var timeSeriesResults = Database.Query(timeSeriesSql, cutoff);

        // Build method data structure
        var methodData = new Dictionary<string, (
            string grainType,
            string methodName,
            List<SiloMethodBreakdown> siloBreakdown,
            List<MethodSample> samples,
            DateTime lastUpdated
        )>();

        // Process silo breakdown results
        foreach (var row in siloResults)
        {
            var grainType = InsightsQueryHelper.GetString(row, "grain_type");
            var methodName = InsightsQueryHelper.GetString(row, "method_name");
            var key = $"{grainType}::{methodName}";

            if (!methodData.TryGetValue(key, out var data))
            {
                data = (grainType, methodName, new List<SiloMethodBreakdown>(), new List<MethodSample>(), DateTime.MinValue);
                methodData[key] = data;
            }

            var calls = InsightsQueryHelper.GetLong(row, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(row, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(row, "total_exceptions");
            var lastUpdated = InsightsQueryHelper.GetDateTime(row, "last_updated");

            data.siloBreakdown.Add(new SiloMethodBreakdown
            {
                SiloId = InsightsQueryHelper.GetString(row, "silo_id"),
                HostName = InsightsQueryHelper.GetString(row, "host_name"),
                TotalRequests = calls,
                RequestsPerSecond = windowSeconds > 0 ? calls / windowSeconds : 0,
                AverageLatencyMs = calls > 0 ? elapsed / calls : 0,
                FailedRequests = exceptions
            });

            if (lastUpdated > data.lastUpdated)
            {
                methodData[key] = (data.grainType, data.methodName, data.siloBreakdown, data.samples, lastUpdated);
            }
        }

        // Process time-series results
        foreach (var row in timeSeriesResults)
        {
            var grainType = InsightsQueryHelper.GetString(row, "grain_type");
            var methodName = InsightsQueryHelper.GetString(row, "method_name");
            var key = $"{grainType}::{methodName}";

            if (!methodData.TryGetValue(key, out var data))
            {
                data = (grainType, methodName, new List<SiloMethodBreakdown>(), new List<MethodSample>(), DateTime.MinValue);
                methodData[key] = data;
            }

            var calls = InsightsQueryHelper.GetLong(row, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(row, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(row, "total_exceptions");

            data.samples.Add(new MethodSample
            {
                Timestamp = InsightsQueryHelper.GetDateTime(row, "bucket"),
                RequestsPerSecond = calls,
                LatencyMs = calls > 0 ? elapsed / calls : 0,
                FailedCount = exceptions
            });
        }

        _logger.LogDebug("AggregateMethodMetrics: Found {MethodCount} methods from DuckDB", methodData.Count);

        // Build final dictionary
        var finalDict = new Dictionary<string, AggregatedMethodMetrics>(methodData.Count);

        foreach (var (key, data) in methodData)
        {
            var siloBreakdown = data.siloBreakdown;
            var totalRequests = siloBreakdown.Sum(s => s.TotalRequests);
            var failedRequests = siloBreakdown.Sum(s => s.FailedRequests);
            var requestsPerSecond = siloBreakdown.Sum(s => s.RequestsPerSecond);

            var avgLatency = 0.0;
            if (totalRequests > 0)
            {
                var weightedLatency = siloBreakdown.Sum(s => s.AverageLatencyMs * s.TotalRequests);
                avgLatency = weightedLatency / totalRequests;
            }

            // Keep only last 60 samples for chart display
            var samples = data.samples;
            if (samples.Count > 60)
                samples = samples.Skip(samples.Count - 60).ToList();

            finalDict[key] = new AggregatedMethodMetrics
            {
                GrainType = data.grainType,
                MethodName = data.methodName,
                TotalRequests = totalRequests,
                FailedRequests = failedRequests,
                AverageLatencyMs = avgLatency,
                RequestsPerSecond = requestsPerSecond,
                ReportingSiloCount = siloBreakdown.Count,
                LastUpdated = data.lastUpdated,
                RecentSamples = samples,
                SiloBreakdown = siloBreakdown
            };
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            var sampleCounts = finalDict.Take(5).Select(kv => $"{kv.Key}={kv.Value.RecentSamples.Count}");
            _logger.LogDebug("AggregateMethodMetrics: Final sample counts: {Counts}", string.Join(", ", sampleCounts));
        }

        return finalDict;
    }

    /// <summary>
    /// Gets per-silo grain type statistics for the Orleans page silo filtering.
    /// Combines method profile data with per-grain-type activation counts.
    /// </summary>
    private Dictionary<string, Dictionary<string, SiloGrainStats>> GetPerSiloGrainStats(DateTime cutoff)
    {
        var windowSeconds = (DateTime.UtcNow - cutoff).TotalSeconds;

        // Query method profile data for request metrics
        var methodSql = """
            SELECT
                grain_type,
                silo_id,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type, silo_id
            """;

        var methodResults = Database.Query(methodSql, cutoff);
        var perSiloStats = new Dictionary<string, Dictionary<string, SiloGrainStats>>();

        foreach (var row in methodResults)
        {
            var grainType = InsightsQueryHelper.GetString(row, "grain_type");
            var siloId = InsightsQueryHelper.GetString(row, "silo_id");
            var totalCalls = InsightsQueryHelper.GetLong(row, "total_calls");
            var totalElapsed = InsightsQueryHelper.GetDouble(row, "total_elapsed");
            var totalExceptions = InsightsQueryHelper.GetLong(row, "total_exceptions");

            var avgLatency = totalCalls > 0 ? totalElapsed / totalCalls : 0;
            var rps = windowSeconds > 0 ? totalCalls / windowSeconds : 0;

            if (!perSiloStats.TryGetValue(grainType, out var siloDict))
            {
                siloDict = new Dictionary<string, SiloGrainStats>();
                perSiloStats[grainType] = siloDict;
            }

            siloDict[siloId] = new SiloGrainStats
            {
                Activations = 0, // Will be populated from grain_type_activations
                RequestsPerSecond = rps,
                AverageLatencyMs = avgLatency,
                TotalRequests = totalCalls,
                FailedRequests = totalExceptions
            };
        }

        // Query per-grain-type activations from the grain_type_activations table
        // Get the latest activation count per grain type per silo
        // Note: grain_type_activations stores "Namespace.TypeName,AssemblyName" format
        // while method_profile stores "Namespace.TypeName" format
        var activationsSql = """
            WITH latest AS (
                SELECT grain_type, silo_id, MAX(timestamp) as max_ts
                FROM grain_type_activations
                WHERE timestamp >= $1
                GROUP BY grain_type, silo_id
            )
            SELECT a.grain_type, a.silo_id, a.activations
            FROM grain_type_activations a
            INNER JOIN latest l ON a.grain_type = l.grain_type AND a.silo_id = l.silo_id AND a.timestamp = l.max_ts
            """;

        var activationResults = Database.Query(activationsSql, cutoff);

        // Match activation grain types to method_profile grain types by stripping assembly suffix
        foreach (var row in activationResults)
        {
            var activationGrainType = InsightsQueryHelper.GetString(row, "grain_type");
            var siloId = InsightsQueryHelper.GetString(row, "silo_id");
            var activations = (int)InsightsQueryHelper.GetLong(row, "activations");

            // Strip assembly suffix to get just "Namespace.TypeName"
            var normalizedType = activationGrainType;
            var commaIndex = activationGrainType.LastIndexOf(',');
            if (commaIndex > 0)
            {
                normalizedType = activationGrainType[..commaIndex];
            }

            if (!perSiloStats.TryGetValue(normalizedType, out var siloDict))
            {
                siloDict = new Dictionary<string, SiloGrainStats>();
                perSiloStats[normalizedType] = siloDict;
            }

            if (siloDict.TryGetValue(siloId, out var existingStats))
            {
                // Update existing stats with activation count
                siloDict[siloId] = existingStats with { Activations = activations };
            }
            else
            {
                // Create new stats entry with only activation data
                siloDict[siloId] = new SiloGrainStats
                {
                    Activations = activations,
                    RequestsPerSecond = 0,
                    AverageLatencyMs = 0,
                    TotalRequests = 0,
                    FailedRequests = 0
                };
            }
        }

        return perSiloStats;
    }

    /// <summary>
    /// Gets per-silo cluster metrics (activations, CPU, memory) for the Orleans page.
    /// </summary>
    private Dictionary<string, (long activations, double cpu, long memoryMb)> GetPerSiloClusterMetrics(DateTime cutoff)
    {
        var sql = """
            SELECT
                silo_id,
                CAST(AVG(total_activations) AS BIGINT) as avg_activations,
                AVG(cpu_usage_percent) as avg_cpu,
                CAST(AVG(memory_usage_mb) AS BIGINT) as avg_memory
            FROM cluster_metrics
            WHERE timestamp >= $1
            GROUP BY silo_id
            """;

        var metrics = new Dictionary<string, (long activations, double cpu, long memoryMb)>();

        try
        {
            var results = Database.Query(sql, cutoff);

            foreach (var row in results)
            {
                var siloId = InsightsQueryHelper.GetString(row, "silo_id");
                var activations = InsightsQueryHelper.GetLong(row, "avg_activations");
                var cpu = InsightsQueryHelper.GetDouble(row, "avg_cpu");
                var memoryMb = InsightsQueryHelper.GetLong(row, "avg_memory");

                metrics[siloId] = (activations, cpu, memoryMb);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get per-silo cluster metrics");
        }

        return metrics;
    }

    /// <summary>
    /// Gets the total activations for a grain type by summing activations from all silos.
    /// Returns 0 if no per-silo stats are available for this grain type.
    /// </summary>
    private static int GetGrainTypeActivations(
        string grainType, Dictionary<string, Dictionary<string, SiloGrainStats>> perSiloStats)
    {
        if (!perSiloStats.TryGetValue(grainType, out var siloStats))
            return 0;

        return siloStats.Values.Sum(s => s.Activations);
    }

    /// <summary>
    /// Gets all method profiles for the Orleans page grain detail view.
    /// Returns all methods grouped by grain type (limited to 500 total to prevent excessive data).
    /// </summary>
    private Task<List<MethodProfileSummary>> GetAllMethodProfiles(TimeSpan? duration = null)
    {
        var cutoff = DateTime.UtcNow - (duration ?? Settings.DefaultQueryDuration);
        var durationSeconds = (duration ?? Settings.DefaultQueryDuration).TotalSeconds;

        var sql = """
            SELECT
                grain_type,
                method_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions,
                COUNT(DISTINCT silo_id) as silo_count,
                MAX(timestamp) as last_updated
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type, method_name
            HAVING SUM(call_count) > 0
            ORDER BY grain_type, total_calls DESC
            LIMIT 500
            """;

        var results = Database.Query(sql, cutoff);

        var summaries = results.Select(r =>
        {
            var calls = InsightsQueryHelper.GetLong(r, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(r, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(r, "total_exceptions");

            return new MethodProfileSummary
            {
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                MethodName = InsightsQueryHelper.GetString(r, "method_name"),
                TotalCalls = calls,
                TotalElapsedMs = elapsed,
                TotalExceptions = exceptions,
                AvgLatencyMs = calls > 0 ? elapsed / calls : 0,
                CallsPerSecond = durationSeconds > 0 ? calls / durationSeconds : 0,
                ExceptionRate = calls > 0 ? (exceptions / (double)calls) * 100 : 0,
                SiloCount = InsightsQueryHelper.GetInt(r, "silo_count"),
                LastUpdated = InsightsQueryHelper.GetDateTime(r, "last_updated")
            };
        }).ToList();

        return Task.FromResult(summaries);
    }

    #endregion

    #region Method Profile Queries (Accurate)

    /// <inheritdoc/>
    public Task<List<MethodProfileTrendPoint>> GetMethodProfileTrend(TimeSpan duration, int bucketSeconds = 1)
    {
        var cutoff = DateTime.UtcNow - duration;

        var sql = $"""
            SELECT
                time_bucket(INTERVAL '{bucketSeconds} seconds', timestamp) as bucket,
                grain_type,
                method_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY bucket, grain_type, method_name
            ORDER BY bucket, total_calls DESC
            """;

        var results = Database.Query(sql, cutoff);

        var points = results.Select(r =>
        {
            var calls = InsightsQueryHelper.GetLong(r, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(r, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(r, "total_exceptions");

            return new MethodProfileTrendPoint
            {
                Timestamp = InsightsQueryHelper.GetDateTime(r, "bucket"),
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                MethodName = InsightsQueryHelper.GetString(r, "method_name"),
                CallCount = calls,
                TotalElapsedMs = elapsed,
                ExceptionCount = exceptions,
                AvgLatencyMs = calls > 0 ? elapsed / calls : 0,
                CallsPerSecond = bucketSeconds > 0 ? calls / (double)bucketSeconds : 0,
                ExceptionRate = calls > 0 ? (exceptions / (double)calls) * 100 : 0
            };
        }).ToList();

        return Task.FromResult(points);
    }

    /// <inheritdoc/>
    public Task<List<MethodProfileTrendPoint>> GetMethodProfileTrendForGrain(string grainType, TimeSpan duration, int bucketSeconds = 1)
    {
        var cutoff = DateTime.UtcNow - duration;

        var sql = $"""
            SELECT
                time_bucket(INTERVAL '{bucketSeconds} seconds', timestamp) as bucket,
                grain_type,
                method_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions
            FROM method_profile
            WHERE timestamp >= $1 AND grain_type = $2
            GROUP BY bucket, grain_type, method_name
            ORDER BY bucket, total_calls DESC
            """;

        var results = Database.Query(sql, cutoff, grainType);

        var points = results.Select(r =>
        {
            var calls = InsightsQueryHelper.GetLong(r, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(r, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(r, "total_exceptions");

            return new MethodProfileTrendPoint
            {
                Timestamp = InsightsQueryHelper.GetDateTime(r, "bucket"),
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                MethodName = InsightsQueryHelper.GetString(r, "method_name"),
                CallCount = calls,
                TotalElapsedMs = elapsed,
                ExceptionCount = exceptions,
                AvgLatencyMs = calls > 0 ? elapsed / calls : 0,
                CallsPerSecond = bucketSeconds > 0 ? calls / (double)bucketSeconds : 0,
                ExceptionRate = calls > 0 ? (exceptions / (double)calls) * 100 : 0
            };
        }).ToList();

        return Task.FromResult(points);
    }

    /// <inheritdoc/>
    public Task<List<MethodProfileTrendPoint>> GetMethodProfileTrendForMethod(string grainType, string methodName, TimeSpan duration, int bucketSeconds = 1)
    {
        var cutoff = DateTime.UtcNow - duration;

        var sql = $"""
            SELECT
                time_bucket(INTERVAL '{bucketSeconds} seconds', timestamp) as bucket,
                grain_type,
                method_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions
            FROM method_profile
            WHERE timestamp >= $1 AND grain_type = $2 AND method_name = $3
            GROUP BY bucket, grain_type, method_name
            ORDER BY bucket
            """;

        var results = Database.Query(sql, cutoff, grainType, methodName);

        var points = results.Select(r =>
        {
            var calls = InsightsQueryHelper.GetLong(r, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(r, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(r, "total_exceptions");

            return new MethodProfileTrendPoint
            {
                Timestamp = InsightsQueryHelper.GetDateTime(r, "bucket"),
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                MethodName = InsightsQueryHelper.GetString(r, "method_name"),
                CallCount = calls,
                TotalElapsedMs = elapsed,
                ExceptionCount = exceptions,
                AvgLatencyMs = calls > 0 ? elapsed / calls : 0,
                CallsPerSecond = bucketSeconds > 0 ? calls / (double)bucketSeconds : 0,
                ExceptionRate = calls > 0 ? (exceptions / (double)calls) * 100 : 0
            };
        }).ToList();

        return Task.FromResult(points);
    }

    /// <inheritdoc/>
    public Task<List<MethodProfileSummary>> GetTopMethodsByLatency(int count = 10, TimeSpan? duration = null)
    {
        var cutoff = DateTime.UtcNow - (duration ?? Settings.DefaultQueryDuration);
        var durationSeconds = (duration ?? Settings.DefaultQueryDuration).TotalSeconds;

        var sql = $"""
            SELECT
                grain_type,
                method_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions,
                COUNT(DISTINCT silo_id) as silo_count,
                MAX(timestamp) as last_updated
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type, method_name
            HAVING SUM(call_count) > 0
            ORDER BY SUM(total_elapsed_ms) / SUM(call_count) DESC
            LIMIT {count}
            """;

        var results = Database.Query(sql, cutoff);

        var summaries = results.Select(r =>
        {
            var calls = InsightsQueryHelper.GetLong(r, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(r, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(r, "total_exceptions");

            return new MethodProfileSummary
            {
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                MethodName = InsightsQueryHelper.GetString(r, "method_name"),
                TotalCalls = calls,
                TotalElapsedMs = elapsed,
                TotalExceptions = exceptions,
                AvgLatencyMs = calls > 0 ? elapsed / calls : 0,
                CallsPerSecond = durationSeconds > 0 ? calls / durationSeconds : 0,
                ExceptionRate = calls > 0 ? (exceptions / (double)calls) * 100 : 0,
                SiloCount = InsightsQueryHelper.GetInt(r, "silo_count"),
                LastUpdated = InsightsQueryHelper.GetDateTime(r, "last_updated")
            };
        }).ToList();

        return Task.FromResult(summaries);
    }

    /// <inheritdoc/>
    public Task<List<MethodProfileSummary>> GetTopMethodsByCallCount(int count = 10, TimeSpan? duration = null)
    {
        var cutoff = DateTime.UtcNow - (duration ?? Settings.DefaultQueryDuration);
        var durationSeconds = (duration ?? Settings.DefaultQueryDuration).TotalSeconds;

        var sql = $"""
            SELECT
                grain_type,
                method_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions,
                COUNT(DISTINCT silo_id) as silo_count,
                MAX(timestamp) as last_updated
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type, method_name
            ORDER BY total_calls DESC
            LIMIT {count}
            """;

        var results = Database.Query(sql, cutoff);

        var summaries = results.Select(r =>
        {
            var calls = InsightsQueryHelper.GetLong(r, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(r, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(r, "total_exceptions");

            return new MethodProfileSummary
            {
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                MethodName = InsightsQueryHelper.GetString(r, "method_name"),
                TotalCalls = calls,
                TotalElapsedMs = elapsed,
                TotalExceptions = exceptions,
                AvgLatencyMs = calls > 0 ? elapsed / calls : 0,
                CallsPerSecond = durationSeconds > 0 ? calls / durationSeconds : 0,
                ExceptionRate = calls > 0 ? (exceptions / (double)calls) * 100 : 0,
                SiloCount = InsightsQueryHelper.GetInt(r, "silo_count"),
                LastUpdated = InsightsQueryHelper.GetDateTime(r, "last_updated")
            };
        }).ToList();

        return Task.FromResult(summaries);
    }

    /// <inheritdoc/>
    public Task<List<MethodProfileSummary>> GetTopMethodsByExceptions(int count = 10, TimeSpan? duration = null)
    {
        var cutoff = DateTime.UtcNow - (duration ?? Settings.DefaultQueryDuration);
        var durationSeconds = (duration ?? Settings.DefaultQueryDuration).TotalSeconds;

        var sql = $"""
            SELECT
                grain_type,
                method_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions,
                COUNT(DISTINCT silo_id) as silo_count,
                MAX(timestamp) as last_updated
            FROM method_profile
            WHERE timestamp >= $1
            GROUP BY grain_type, method_name
            HAVING SUM(exception_count) > 0
            ORDER BY total_exceptions DESC
            LIMIT {count}
            """;

        var results = Database.Query(sql, cutoff);

        var summaries = results.Select(r =>
        {
            var calls = InsightsQueryHelper.GetLong(r, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(r, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(r, "total_exceptions");

            return new MethodProfileSummary
            {
                GrainType = InsightsQueryHelper.GetString(r, "grain_type"),
                MethodName = InsightsQueryHelper.GetString(r, "method_name"),
                TotalCalls = calls,
                TotalElapsedMs = elapsed,
                TotalExceptions = exceptions,
                AvgLatencyMs = calls > 0 ? elapsed / calls : 0,
                CallsPerSecond = durationSeconds > 0 ? calls / durationSeconds : 0,
                ExceptionRate = calls > 0 ? (exceptions / (double)calls) * 100 : 0,
                SiloCount = InsightsQueryHelper.GetInt(r, "silo_count"),
                LastUpdated = InsightsQueryHelper.GetDateTime(r, "last_updated")
            };
        }).ToList();

        return Task.FromResult(summaries);
    }

    /// <inheritdoc/>
    public Task<List<MethodProfileSiloSummary>> GetMethodProfileBySilo(string grainType, string methodName, TimeSpan? duration = null)
    {
        var cutoff = DateTime.UtcNow - (duration ?? Settings.DefaultQueryDuration);
        var durationSeconds = (duration ?? Settings.DefaultQueryDuration).TotalSeconds;

        var sql = """
            SELECT
                silo_id,
                MAX(host_name) as host_name,
                SUM(call_count) as total_calls,
                SUM(total_elapsed_ms) as total_elapsed,
                SUM(exception_count) as total_exceptions,
                MAX(timestamp) as last_updated
            FROM method_profile
            WHERE timestamp >= $1 AND grain_type = $2 AND method_name = $3
            GROUP BY silo_id
            ORDER BY total_calls DESC
            """;

        var results = Database.Query(sql, cutoff, grainType, methodName);

        var summaries = results.Select(r =>
        {
            var calls = InsightsQueryHelper.GetLong(r, "total_calls");
            var elapsed = InsightsQueryHelper.GetDouble(r, "total_elapsed");
            var exceptions = InsightsQueryHelper.GetLong(r, "total_exceptions");

            return new MethodProfileSiloSummary
            {
                SiloId = InsightsQueryHelper.GetString(r, "silo_id"),
                HostName = InsightsQueryHelper.GetString(r, "host_name"),
                TotalCalls = calls,
                TotalElapsedMs = elapsed,
                TotalExceptions = exceptions,
                AvgLatencyMs = calls > 0 ? elapsed / calls : 0,
                CallsPerSecond = durationSeconds > 0 ? calls / durationSeconds : 0,
                ExceptionRate = calls > 0 ? (exceptions / (double)calls) * 100 : 0,
                LastUpdated = InsightsQueryHelper.GetDateTime(r, "last_updated")
            };
        }).ToList();

        return Task.FromResult(summaries);
    }

    #endregion

    #region Database Management

    /// <inheritdoc/>
    public Task<InsightDatabaseSummary> GetDatabaseSummary()
    {
        var sizeBytes = Database.GetEstimatedSizeBytes();
        var rowCount = Database.GetTotalRowCount();

        var rangeResults = Database.Query("""
            SELECT
                MIN(timestamp) as oldest,
                MAX(timestamp) as newest,
                COUNT(DISTINCT silo_id) as silo_count
            FROM cluster_metrics
            """);

        var grainCountResults = Database.Query("SELECT COUNT(DISTINCT grain_type) as cnt FROM grain_metrics");
        var methodCountResults = Database.Query("SELECT COUNT(DISTINCT grain_type || '::' || method_name) as cnt FROM method_metrics");

        var rangeRow = rangeResults.FirstOrDefault();
        var grainCount = grainCountResults.FirstOrDefault();
        var methodCount = methodCountResults.FirstOrDefault();

        var summary = new InsightDatabaseSummary
        {
            GeneratedAt = DateTime.UtcNow,
            EstimatedSizeBytes = sizeBytes,
            EstimatedSizeFormatted = InsightsQueryHelper.FormatBytes(sizeBytes),
            TotalRows = rowCount,
            RetentionPeriod = Settings.RetentionPeriod,
            OldestDataPoint = rangeRow != null ? InsightsQueryHelper.GetDateTime(rangeRow, "oldest") : DateTime.MinValue,
            NewestDataPoint = rangeRow != null ? InsightsQueryHelper.GetDateTime(rangeRow, "newest") : DateTime.MinValue,
            UniqueSilos = rangeRow != null ? InsightsQueryHelper.GetInt(rangeRow, "silo_count") : 0,
            UniqueGrainTypes = grainCount != null ? InsightsQueryHelper.GetInt(grainCount, "cnt") : 0,
            UniqueMethods = methodCount != null ? InsightsQueryHelper.GetInt(methodCount, "cnt") : 0
        };

        return Task.FromResult(summary);
    }

    /// <inheritdoc/>
    public Task<string> ExecuteQuery(string sql)
    {
        var trimmed = sql.Trim();
        if (!trimmed.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult("{\"error\": \"Only SELECT queries are allowed\"}");
        }

        if (DangerousPatternRegex().IsMatch(trimmed))
        {
            return Task.FromResult("{\"error\": \"Modifying queries are not allowed\"}");
        }

        try
        {
            var json = Database.QueryJson(sql);
            return Task.FromResult(json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Custom query failed: {Sql}", sql);
            return Task.FromResult($"{{\"error\": \"{ex.Message.Replace("\"", "\\\"").Replace("\n", " ")}\"}}");
        }
    }

    [GeneratedRegex(@"\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE)\b", RegexOptions.IgnoreCase)]
    private static partial Regex DangerousPatternRegex();

    #endregion

    #region Maintenance

    private async Task MaintenanceCallbackAsync()
    {
        if (_disposed) return;

        try
        {
            // Flush pending buffers
            _buffer?.FlushTo(Database);

            // Apply retention policies
            _schemaManager?.ApplyRetention(Settings.RetentionPeriod);

            // Vacuum periodically
            if (_vacuumStopwatch.Elapsed >= Settings.VacuumInterval)
            {
                _vacuumStopwatch.Restart();
                Database.Vacuum();
            }

            // Log metrics periodically
            if (_metricsStopwatch.Elapsed >= Settings.MetricsLogInterval)
            {
                _metricsStopwatch.Restart();
                Database.UpdateSizeMetrics();

                var summary = await GetDatabaseSummary();
                _logger.LogInformation(
                    "Insights: Size={Size}, Rows={Rows}, Silos={Silos}, Grains={Grains}, Methods={Methods}",
                    summary.EstimatedSizeFormatted,
                    summary.TotalRows,
                    summary.UniqueSilos,
                    summary.UniqueGrainTypes,
                    summary.UniqueMethods);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Maintenance callback failed");
        }
    }

    #endregion

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _vacuumStopwatch.Stop();
        _metricsStopwatch.Stop();

        _maintenanceTimer?.Dispose();
        _maintenanceTimer = null;

        // Dispose broadcast timer
        DisposeBroadcastTimer();

        _database?.Dispose();
        _database = null;

        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Helper methods for extracting typed values from query result rows.
/// Throws on unhandled types to surface schema/query mismatches early.
/// </summary>
internal static class InsightsQueryHelper
{
    /// <summary>
    /// Pre-allocated size suffixes to avoid per-call allocations.
    /// </summary>
    private static readonly string[] SizeSuffixes = ["B", "KB", "MB", "GB"];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DateTime GetDateTime(Dictionary<string, object?> row, string column)
    {
        ref var value = ref CollectionsMarshal.GetValueRefOrNullRef(row, column);
        if (Unsafe.IsNullRef(ref value) || value is null) return DateTime.MinValue;

        return value switch
        {
            DateTime dt => dt,
            DateTimeOffset dto => dto.UtcDateTime,
            _ => throw new InvalidOperationException($"Column '{column}' has unexpected type '{value.GetType().Name}' for DateTime conversion. Value: {value}")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string GetString(Dictionary<string, object?> row, string column)
    {
        ref var value = ref CollectionsMarshal.GetValueRefOrNullRef(row, column);
        if (Unsafe.IsNullRef(ref value) || value is null) return string.Empty;
        return value.ToString() ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetInt(Dictionary<string, object?> row, string column)
    {
        ref var value = ref CollectionsMarshal.GetValueRefOrNullRef(row, column);
        if (Unsafe.IsNullRef(ref value) || value is null) return 0;

        return value switch
        {
            int i => i,
            long l when l >= int.MinValue && l <= int.MaxValue => (int)l,
            long l => throw new OverflowException($"Column '{column}' value {l} overflows int range"),
            System.Numerics.BigInteger bi when bi >= int.MinValue && bi <= int.MaxValue => (int)bi,
            System.Numerics.BigInteger bi => throw new OverflowException($"Column '{column}' value {bi} overflows int range"),
            _ => throw new InvalidOperationException($"Column '{column}' has unexpected type '{value.GetType().Name}' for int conversion. Value: {value}")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetLong(Dictionary<string, object?> row, string column)
    {
        ref var value = ref CollectionsMarshal.GetValueRefOrNullRef(row, column);
        if (Unsafe.IsNullRef(ref value) || value is null) return 0L;

        return value switch
        {
            long l => l,
            int i => i,
            ulong ul when ul <= long.MaxValue => (long)ul,
            ulong ul => throw new OverflowException($"Column '{column}' value {ul} overflows long range"),
            System.Numerics.BigInteger bi when bi >= long.MinValue && bi <= long.MaxValue => (long)bi,
            System.Numerics.BigInteger bi => throw new OverflowException($"Column '{column}' value {bi} overflows long range"),
            _ => throw new InvalidOperationException($"Column '{column}' has unexpected type '{value.GetType().Name}' for long conversion. Value: {value}")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double GetDouble(Dictionary<string, object?> row, string column)
    {
        ref var value = ref CollectionsMarshal.GetValueRefOrNullRef(row, column);
        if (Unsafe.IsNullRef(ref value) || value is null) return 0.0;

        return value switch
        {
            double d => d,
            float f => f,
            int i => i,
            long l => l,
            decimal dec => (double)dec,
            System.Numerics.BigInteger bi => (double)bi,
            _ => throw new InvalidOperationException($"Column '{column}' has unexpected type '{value.GetType().Name}' for double conversion. Value: {value}")
        };
    }

    /// <summary>
    /// Formats byte count to human-readable string using stackalloc for zero allocations
    /// on the formatting path.
    /// </summary>
    public static string FormatBytes(long bytes)
    {
        double len = bytes;
        int order = 0;

        while (len >= 1024 && order < SizeSuffixes.Length - 1)
        {
            order++;
            len /= 1024;
        }

        // Use stackalloc for the intermediate formatting buffer
        Span<char> buffer = stackalloc char[32];
        if (len.TryFormat(buffer, out var charsWritten, "0.##"))
        {
            var suffix = SizeSuffixes[order];
            // Allocate final string: "value suffix"
            return string.Create(charsWritten + 1 + suffix.Length, (buffer[..charsWritten].ToString(), suffix),
                static (span, state) =>
                {
                    state.Item1.AsSpan().CopyTo(span);
                    span[state.Item1.Length] = ' ';
                    state.suffix.AsSpan().CopyTo(span[(state.Item1.Length + 1)..]);
                });
        }

        // Fallback for edge cases
        return $"{len:0.##} {SizeSuffixes[order]}";
    }
}
