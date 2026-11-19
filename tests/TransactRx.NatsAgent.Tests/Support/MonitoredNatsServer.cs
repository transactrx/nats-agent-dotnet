using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using NATS.Client.Core;

namespace TransactRx.NatsAgent.Tests.Support;

/// <summary>
/// A private nats-server with the HTTP monitoring endpoint on, for the tests that count a
/// connection's subscriptions (the Go <c>nc.NumSubscriptions()</c>). NATS.Net does not expose
/// that count, so the tests ask the server, which also names the subjects. The binary is
/// found like <see cref="NatsServerFixture"/> does.
/// </summary>
public sealed class MonitoredNatsServer : IAsyncDisposable
{
    private readonly Process _server;
    private readonly HttpClient _http;

    private MonitoredNatsServer(Process server, string url, int monitorPort)
    {
        _server = server;
        Url = url;
        _http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{monitorPort}/"), Timeout = TimeSpan.FromSeconds(5) };
    }

    public string Url { get; }

    public static async Task<MonitoredNatsServer> StartAsync()
    {
        var port = FreePort();
        var monitorPort = FreePort();
        var binary = Environment.GetEnvironmentVariable("NATS_SERVER_PATH") is { Length: > 0 } path ? path : "nats-server";
        var process = Process.Start(new ProcessStartInfo(binary, ["-a", "127.0.0.1", "-p", port.ToString(), "-m", monitorPort.ToString()])
        {
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
        }) ?? throw new InvalidOperationException($"could not start '{binary}'");

        // Drain the pipes so the server never blocks on a full buffer.
        process.OutputDataReceived += (_, _) => { };
        process.ErrorDataReceived += (_, _) => { };
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        var server = new MonitoredNatsServer(process, $"nats://127.0.0.1:{port}", monitorPort);
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (true)
        {
            try
            {
                await using var probe = new NatsConnection(new NatsOpts { Url = server.Url });
                await probe.ConnectAsync();
                (await server._http.GetAsync("connz")).EnsureSuccessStatusCode();
                return server;
            }
            catch (Exception) when (DateTime.UtcNow < deadline && !process.HasExited)
            {
                await Task.Delay(50);
            }
            catch (Exception)
            {
                await server.DisposeAsync();
                throw;
            }
        }
    }

    /// <summary>
    /// The subjects the connection named <paramref name="connectionName"/> is subscribed to,
    /// as the server sees them (one entry per subscription, sorted).
    /// </summary>
    public async Task<List<string>> SubscriptionsAsync(string connectionName)
    {
        using var doc = JsonDocument.Parse(await _http.GetStringAsync("connz?subs=1&limit=1024"));
        var matches = doc.RootElement.GetProperty("connections").EnumerateArray()
            .Where(c => c.TryGetProperty("name", out var name) && name.GetString() == connectionName)
            .ToList();
        var connection = Assert.Single(matches);
        var subjects = connection.TryGetProperty("subscriptions_list", out var list)
            ? list.EnumerateArray().Select(s => s.GetString()!).ToList()
            : [];
        Assert.Equal(connection.GetProperty("subscriptions").GetInt32(), subjects.Count);
        subjects.Sort(StringComparer.Ordinal);
        return subjects;
    }

    public ValueTask DisposeAsync()
    {
        _http.Dispose();
        try
        {
            if (!_server.HasExited)
            {
                _server.Kill(entireProcessTree: true);
                _server.WaitForExit(5000);
            }
        }
        catch (Exception)
        {
            // already gone
        }

        _server.Dispose();
        return ValueTask.CompletedTask;
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
