using Orleans.Insights.Database;
using Microsoft.Extensions.Logging;
using System;

namespace Orleans.Insights.Schema;

/// <summary>
/// Manages DuckDB schema initialization and retention for Insights metrics.
/// Implements the single responsibility of schema management and data lifecycle.
/// </summary>
/// <remarks>
/// Defines tables for cluster, grain, and method metrics.
/// Provides index creation for optimized query patterns and retention enforcement.
/// </remarks>
internal sealed class InsightsSchemaManager
{
    private readonly ILogger _logger;
    private readonly InsightsDatabase _database;

    public InsightsSchemaManager(ILogger logger, InsightsDatabase database)
    {
        _logger = logger;
        _database = database;
    }

    /// <summary>
    /// Initializes all schema tables and indexes.
    /// </summary>
    public void InitializeSchema()
    {
        CreateTables();
        CreateIndexes();
        _logger.LogDebug("Insights schema initialized with cluster, grain, and method metrics support");
    }

    private void CreateTables()
    {
        // Cluster-level metrics time series
        _database.Execute("""
            CREATE TABLE IF NOT EXISTS cluster_metrics (
                timestamp TIMESTAMP NOT NULL,
                silo_id VARCHAR NOT NULL,
                host_name VARCHAR NOT NULL,
                total_activations BIGINT,
                connected_clients INTEGER,
                messages_sent BIGINT,
                messages_received BIGINT,
                messages_dropped BIGINT,
                cpu_usage_percent DOUBLE,
                memory_usage_mb BIGINT,
                available_memory_mb BIGINT,
                avg_latency_ms DOUBLE,
                total_requests BIGINT,
                -- Catalog metrics (activation lifecycle)
                -- See: https://learn.microsoft.com/en-us/dotnet/orleans/host/monitoring/?pivots=orleans-7-0#catalog
                activation_working_set BIGINT DEFAULT 0,
                activations_created BIGINT DEFAULT 0,
                activations_destroyed BIGINT DEFAULT 0,
                activations_failed_to_activate BIGINT DEFAULT 0,
                activation_collections BIGINT DEFAULT 0,
                activation_shutdowns BIGINT DEFAULT 0,
                activation_non_existent BIGINT DEFAULT 0,
                concurrent_registration_attempts BIGINT DEFAULT 0,
                -- Miscellaneous grain metrics
                grain_count BIGINT DEFAULT 0,
                system_targets BIGINT DEFAULT 0
            )
            """);

        // Grain-type metrics time series
        _database.Execute("""
            CREATE TABLE IF NOT EXISTS grain_metrics (
                timestamp TIMESTAMP NOT NULL,
                silo_id VARCHAR NOT NULL,
                grain_type VARCHAR NOT NULL,
                total_requests BIGINT,
                failed_requests BIGINT,
                avg_latency_ms DOUBLE,
                min_latency_ms DOUBLE,
                max_latency_ms DOUBLE,
                requests_per_second DOUBLE
            )
            """);

        // Method metrics time series
        _database.Execute("""
            CREATE TABLE IF NOT EXISTS method_metrics (
                timestamp TIMESTAMP NOT NULL,
                silo_id VARCHAR NOT NULL,
                grain_type VARCHAR NOT NULL,
                method_name VARCHAR NOT NULL,
                total_requests BIGINT,
                failed_requests BIGINT,
                avg_latency_ms DOUBLE,
                requests_per_second DOUBLE
            )
            """);

        // Method profile metrics (OrleansDashboard-style accurate totals)
        // Stores raw totals per second, enabling accurate average calculation: avg = total_elapsed_ms / call_count
        // This replaces the EMA-based method_metrics table for accurate method profiling
        _database.Execute("""
            CREATE TABLE IF NOT EXISTS method_profile (
                timestamp TIMESTAMP NOT NULL,
                silo_id VARCHAR NOT NULL,
                host_name VARCHAR NOT NULL,
                grain_type VARCHAR NOT NULL,
                method_name VARCHAR NOT NULL,
                call_count BIGINT NOT NULL,
                total_elapsed_ms DOUBLE NOT NULL,
                exception_count BIGINT NOT NULL
            )
            """);

        // Per-grain-type activation counts from orleans-grains metric with grain type tag
        // Stores activation counts per silo per grain type from OTel metrics
        _database.Execute("""
            CREATE TABLE IF NOT EXISTS grain_type_activations (
                timestamp TIMESTAMP NOT NULL,
                silo_id VARCHAR NOT NULL,
                grain_type VARCHAR NOT NULL,
                activations BIGINT NOT NULL
            )
            """);
    }

    private void CreateIndexes()
    {
        // Cluster metrics indexes - composite for common query patterns
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_cluster_ts ON cluster_metrics(timestamp)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_cluster_silo ON cluster_metrics(silo_id)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_cluster_silo_ts ON cluster_metrics(silo_id, timestamp DESC)");

        // Grain metrics indexes
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_grain_ts ON grain_metrics(timestamp)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_grain_type ON grain_metrics(grain_type)");

        // Method metrics indexes
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_method_ts ON method_metrics(timestamp)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_method_grain ON method_metrics(grain_type, method_name)");

        // Method profile indexes - optimized for hot query patterns
        // Composite index for timestamp-filtered aggregation queries (GetTopGrainTypes, GetTopMethods)
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_method_profile_ts ON method_profile(timestamp)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_method_profile_grain ON method_profile(grain_type, method_name)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_method_profile_silo ON method_profile(silo_id)");
        // Composite indexes for aggregation queries - covers timestamp filter + grouping columns
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_method_profile_ts_grain ON method_profile(timestamp, grain_type)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_method_profile_ts_grain_method ON method_profile(timestamp, grain_type, method_name)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_method_profile_ts_silo ON method_profile(timestamp, silo_id)");

        // Grain type activation indexes - composite for per-silo grain type queries
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_grain_type_activations_ts ON grain_type_activations(timestamp)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_grain_type_activations_grain ON grain_type_activations(grain_type)");
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_grain_type_activations_silo ON grain_type_activations(silo_id)");
        // Composite index for the common query pattern: filter by timestamp, group by grain_type/silo_id
        _database.Execute("CREATE INDEX IF NOT EXISTS idx_grain_type_activations_ts_grain_silo ON grain_type_activations(timestamp, grain_type, silo_id)");
    }

    /// <summary>
    /// Applies retention policies to all metrics tables.
    /// </summary>
    public void ApplyRetention(TimeSpan retentionPeriod)
    {
        _database.ApplyRetention("cluster_metrics", "timestamp", retentionPeriod);
        _database.ApplyRetention("grain_metrics", "timestamp", retentionPeriod);
        _database.ApplyRetention("method_metrics", "timestamp", retentionPeriod);
        _database.ApplyRetention("method_profile", "timestamp", retentionPeriod);
        _database.ApplyRetention("grain_type_activations", "timestamp", retentionPeriod);
    }
}
