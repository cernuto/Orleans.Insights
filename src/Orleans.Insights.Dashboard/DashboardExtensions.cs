using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Insights.Dashboard;

/// <summary>
/// Extension methods for configuring the Orleans.Insights dashboard.
/// </summary>
public static class DashboardExtensions
{
    /// <summary>
    /// Adds Orleans.Insights dashboard services.
    /// </summary>
    public static IServiceCollection AddOrleansInsightsDashboard(this IServiceCollection services)
    {
        // Add SignalR
        services.AddSignalR();

        return services;
    }

    /// <summary>
    /// Maps the Orleans.Insights dashboard endpoints.
    /// </summary>
    /// <remarks>
    /// The dashboard is hosted at the specified path prefix (default: "/dashboard").
    /// This method should be called after UseStaticFiles() in the middleware pipeline.
    /// </remarks>
    /// <param name="app">The web application.</param>
    /// <param name="pathPrefix">The path prefix for the dashboard (default: "/dashboard").</param>
    public static WebApplication MapOrleansInsightsDashboard(
        this WebApplication app,
        string pathPrefix = "/dashboard")
    {
        // Normalize path prefix
        pathPrefix = pathPrefix.TrimEnd('/');
        if (!pathPrefix.StartsWith("/"))
        {
            pathPrefix = "/" + pathPrefix;
        }

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

                // Serve static files (must be called twice per Microsoft docs)
                dashboardApp.UseStaticFiles();
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
}
