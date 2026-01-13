using Orleans.Insights.Dashboard.Client;
using Orleans.Insights.Dashboard.Client.Services;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;

var builder = WebAssemblyHostBuilder.CreateDefault(args);

// Add root components
builder.RootComponents.Add<Routes>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

// Configure HttpClient for API calls
builder.Services.AddScoped(sp => new HttpClient
{
    BaseAddress = new Uri(builder.HostEnvironment.BaseAddress)
});

// SignalR dashboard data service (singleton for shared connection)
builder.Services.AddSingleton<DashboardSignalRService>();
builder.Services.AddSingleton<IDashboardDataService>(sp => sp.GetRequiredService<DashboardSignalRService>());

await builder.Build().RunAsync();
