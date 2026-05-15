using System.Net.WebSockets;
using WitsmlSocket;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(Constants.Port));

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss ";
});

builder.Services.AddSingleton<RigState>();
builder.Services.AddSingleton<AlarmRegistry>();
builder.Services.AddSingleton<WebSocketHub>();
builder.Services.AddHostedService<TelemetryService>();
builder.Services.AddHostedService<PingService>();
builder.Services.AddHostedService<AlarmPurgeService>();

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(10),
});

app.Map("/", async (HttpContext ctx, WebSocketHub hub, IHostApplicationLifetime life) =>
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        await ctx.Response.WriteAsync("WebSocket request expected");
        return;
    }

    using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
    var remote = ctx.Connection.RemoteIpAddress?.ToString() ?? "unknown";
    await hub.HandleAsync(ws, remote, life.ApplicationStopping);
});

var logger = app.Services.GetRequiredService<ILogger<Program>>();
logger.LogInformation("[SERVER] witsml-socket listening on ws://0.0.0.0:{Port}", Constants.Port);

var hubInstance = app.Services.GetRequiredService<WebSocketHub>();
app.Lifetime.ApplicationStopping.Register(() =>
{
    logger.LogInformation("[SHUTDOWN] draining clients");
    try { hubInstance.ShutdownAllAsync().GetAwaiter().GetResult(); } catch { }
});

app.Run();
