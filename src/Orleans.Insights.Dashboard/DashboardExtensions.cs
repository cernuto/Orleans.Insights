using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using System.Reflection;

namespace Orleans.Insights.Dashboard;

/// <summary>
/// Extension methods for configuring the Orleans.Insights dashboard.
/// </summary>
/// <remarks>
/// <para>
/// <b>Minimal integration:</b>
/// <code>
/// var builder = WebApplication.CreateBuilder(args);
/// builder.UseOrleans(silo => silo.AddOrleansInsights());
/// builder.Services.AddOrleansInsightsDashboard();
///
/// var app = builder.Build();
/// app.UseStaticFiles();
/// app.MapOrleansInsightsDashboard();
/// app.Run();
/// </code>
/// </para>
/// </remarks>
public static class DashboardExtensions
{
    /// <summary>
    /// Adds Orleans.Insights dashboard services including SignalR.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddOrleansInsightsDashboard(this IServiceCollection services)
    {
        // Add SignalR with MessagePack for better performance (optional, falls back to JSON)
        services.AddSignalR(options =>
        {
            options.EnableDetailedErrors = false; // Disable in production for security
            options.MaximumReceiveMessageSize = 256 * 1024; // 256KB limit for dashboard data
        });

        return services;
    }

    /// <summary>
    /// Maps the Orleans.Insights dashboard endpoints at the specified path.
    /// </summary>
    /// <param name="app">The endpoint route builder (WebApplication or IEndpointRouteBuilder).</param>
    /// <param name="pathPrefix">The path prefix for the dashboard (default: "/dashboard").</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <example>
    /// <code>
    /// app.MapOrleansInsightsDashboard(); // Maps to /dashboard
    /// app.MapOrleansInsightsDashboard("/monitoring"); // Maps to /monitoring
    /// </code>
    /// </example>
    public static IEndpointRouteBuilder MapOrleansInsightsDashboard(
        this IEndpointRouteBuilder app,
        string pathPrefix = "/dashboard")
    {
        // Normalize path prefix
        pathPrefix = NormalizePathPrefix(pathPrefix);

        // Map SignalR hub
        app.MapHub<Hubs.DashboardHub>($"{pathPrefix}/hub");

        return app;
    }

    /// <summary>
    /// Maps the Orleans.Insights dashboard with full static file hosting for Blazor WASM.
    /// Use this overload when you need the complete dashboard UI.
    /// </summary>
    /// <param name="app">The web application.</param>
    /// <param name="pathPrefix">The path prefix for the dashboard (default: "/dashboard").</param>
    /// <returns>The web application for chaining.</returns>
    public static WebApplication MapOrleansInsightsDashboard(
        this WebApplication app,
        string pathPrefix = "/dashboard")
    {
        // Normalize path prefix
        pathPrefix = NormalizePathPrefix(pathPrefix);

        // Map SignalR hub at the dashboard path
        app.MapHub<Hubs.DashboardHub>($"{pathPrefix}/hub");

        // Use MapWhen to handle all requests to the dashboard path
        // This follows the Microsoft-recommended pattern for hosting Blazor WASM at a sub-path
        app.MapWhen(
            ctx => ctx.Request.Path.StartsWithSegments(pathPrefix, StringComparison.OrdinalIgnoreCase),
            dashboardApp =>
            {
                // Serve Blazor WebAssembly framework files at the sub-path
                dashboardApp.UseBlazorFrameworkFiles(pathPrefix);

                // Configure static file options with cache control headers
                // Fingerprinted files (with hashes) can be cached indefinitely
                // Non-fingerprinted files should have no-cache to ensure fresh content after rebuilds
                var staticFileOptions = new StaticFileOptions
                {
                    OnPrepareResponse = ctx =>
                    {
                        var path = ctx.Context.Request.Path.Value ?? "";

                        // blazor.boot.json controls what files to load - should not be cached
                        if (path.EndsWith("blazor.boot.json", StringComparison.OrdinalIgnoreCase))
                        {
                            ctx.Context.Response.Headers.CacheControl = "no-cache, no-store, must-revalidate";
                            ctx.Context.Response.Headers.Pragma = "no-cache";
                            ctx.Context.Response.Headers.Expires = "0";
                        }
                        // Framework files with hashes in names can be cached (they change on rebuild)
                        else if (path.Contains("/_framework/") &&
                                 (path.Contains(".") && path.LastIndexOf('.') > path.LastIndexOf('/')))
                        {
                            // Allow caching for 1 hour in development, fingerprinted files ensure fresh content
                            ctx.Context.Response.Headers.CacheControl = "public, max-age=3600";
                        }
                    }
                };

                // Serve static files (must be called twice per Microsoft docs)
                dashboardApp.UseStaticFiles(staticFileOptions);
                dashboardApp.UseStaticFiles(pathPrefix);

                dashboardApp.UseRouting();

                dashboardApp.UseEndpoints(endpoints =>
                {
                    // Fallback to index.html for SPA client-side routing
                    endpoints.MapFallbackToFile($"{pathPrefix}/{{*path:nonfile}}", $"{pathPrefix}/index.html");
                });
            });

        return app;
    }

    /// <summary>
    /// Normalizes the path prefix to ensure it starts with "/" and doesn't end with "/".
    /// </summary>
    private static string NormalizePathPrefix(string pathPrefix)
    {
        pathPrefix = pathPrefix.Trim();

        if (string.IsNullOrEmpty(pathPrefix))
            return "/dashboard";

        // Ensure it starts with /
        if (!pathPrefix.StartsWith('/'))
            pathPrefix = "/" + pathPrefix;

        // Remove trailing slash
        return pathPrefix.TrimEnd('/');
    }
}
