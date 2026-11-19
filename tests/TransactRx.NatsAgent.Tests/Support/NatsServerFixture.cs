using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using NATS.Client.Core;
using TransactRx.NatsAgent.Tests.Support;

[assembly: AssemblyFixture(typeof(NatsServerFixture))]

// Like `go test` within one package: tests run one at a time, so fixed agent names and
// discovery counts are deterministic on the shared server.
[assembly: Xunit.v3.Parallelization(Mode = Xunit.Sdk.ParallelMode.None)]

namespace TransactRx.NatsAgent.Tests.Support;

/// <summary>
/// One real nats-server for the whole test run, on a free local port (the Go suite embeds the
/// server; here the binary is started as a child process). The binary is <c>nats-server</c>
/// on PATH, or the path in <c>NATS_SERVER_PATH</c>.
/// </summary>
public sealed class NatsServerFixture : IAsyncLifetime
{
    private Process? _server;

    public string Url { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        var port = FreePort();
        var binary = Environment.GetEnvironmentVariable("NATS_SERVER_PATH") is { Length: > 0 } path ? path : "nats-server";
        try
        {
            _server = Process.Start(new ProcessStartInfo(binary, ["-a", "127.0.0.1", "-p", port.ToString()])
            {
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
            });
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"could not start '{binary}': install nats-server (https://github.com/nats-io/nats-server/releases) or set NATS_SERVER_PATH. {ex.Message}", ex);
        }

        // Drain the pipes so the server never blocks on a full buffer.
        _server!.OutputDataReceived += (_, _) => { };
        _server.ErrorDataReceived += (_, _) => { };
        _server.BeginOutputReadLine();
        _server.BeginErrorReadLine();

        Url = $"nats://127.0.0.1:{port}";
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (true)
        {
            try
            {
                await using var probe = new NatsConnection(new NatsOpts { Url = Url });
                await probe.ConnectAsync();
                return;
            }
            catch (Exception) when (DateTime.UtcNow < deadline && !_server.HasExited)
            {
                await Task.Delay(50);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        try
        {
            if (_server is { HasExited: false })
            {
                _server.Kill(entireProcessTree: true);
                _server.WaitForExit(5000);
            }
        }
        catch (Exception)
        {
            // already gone
        }

        _server?.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>A fresh client connection; closed with the test class (see <see cref="NatsTestBase"/>).</summary>
    public async Task<NatsConnection> ConnectAsync()
    {
        var nc = new NatsConnection(new NatsOpts { Url = Url });
        await nc.ConnectAsync();
        return nc;
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
