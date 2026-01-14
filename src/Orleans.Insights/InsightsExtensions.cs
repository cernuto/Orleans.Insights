using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Insights.Instrumentation;
using Orleans.Runtime;

namespace Orleans.Insights;

/// <summary>
/// Extension methods for configuring Orleans.Insights on silos.
/// </summary>
public static class InsightsExtensions
{
    /// <summary>
    /// Adds Orleans.Insights to the silo for cluster monitoring and analytics.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The silo builder for chaining.</returns>
    public static ISiloBuilder AddOrleansInsights(
        this ISiloBuilder builder,
        Action<InsightsOptions>? configure = null)
    {
        builder.Services.AddOrleansInsightsCore(configure);

        // Register the grain call filter for method profiling
        builder.AddIncomingGrainCallFilter<GlobalGrainCallFilter>();

        return builder;
    }

    /// <summary>
    /// Adds Orleans.Insights core services to the service collection.
    /// </summary>
    internal static IServiceCollection AddOrleansInsightsCore(
        this IServiceCollection services,
        Action<InsightsOptions>? configure = null)
    {
        // Configure options
        if (configure != null)
        {
            services.Configure(configure);
        }
        else
        {
            services.Configure<InsightsOptions>(_ => { });
        }

        // Register OrleansMetricsListener as IHostedService to ensure it starts BEFORE Orleans
        // MeterListener only captures instruments published AFTER it starts, so timing is critical
        // Uses delta-based latency calculation for accurate display
        services.AddSingleton<OrleansMetricsListener>();
        services.AddSingleton<IOrleansMetricsListener>(sp => sp.GetRequiredService<OrleansMetricsListener>());
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<OrleansMetricsListener>());

        // Register the grain method profiler as singleton with all required interfaces
        services.AddSingleton<GrainMethodProfiler>();
        services.AddSingleton<IGrainMethodProfiler>(sp => sp.GetRequiredService<GrainMethodProfiler>());
        services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(sp => sp.GetRequiredService<GrainMethodProfiler>());

        // Register the silo metrics collector for cluster-level metrics (CPU, memory, etc.)
        services.AddSingleton<SiloMetricsCollector>();
        services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(sp => sp.GetRequiredService<SiloMetricsCollector>());

        return services;
    }
}
