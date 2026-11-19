using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;

namespace TransactRx.NatsAgent.Service;

/// <summary>
/// How to reach NATS. Every unset value falls back to the org environment conventions:
/// <c>NATS_URL</c>, <c>NATS_JWT</c>, <c>NATS_KEY</c> (and <c>APPID</c> for the connection name).
/// </summary>
public sealed record NatsSettings
{
    /// <summary>Server URL(s), e.g. <c>nats://localhost:4222</c>. Default: <c>$NATS_URL</c>.</summary>
    public string? Url { get; init; }

    /// <summary>User JWT. Default: <c>$NATS_JWT</c>. Used only together with <see cref="Seed"/>.</summary>
    public string? Jwt { get; init; }

    /// <summary>User NKey seed. Default: <c>$NATS_KEY</c>.</summary>
    public string? Seed { get; init; }

    /// <summary>Connection name shown by the server. Default: <c>$APPID</c>.</summary>
    public string? Name { get; init; }

    /// <summary>
    /// When the connection is lost and cannot be re-established for 15 minutes, end the
    /// process (exit code 1) so the orchestrator restarts it — what the Go services do.
    /// Default true. Set false to keep the process alive and handle it yourself.
    /// </summary>
    public bool ExitWhenConnectionLost { get; init; } = true;
}

internal static class NatsConnector
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan TotalReconnectWait = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Opens a connection (fail-fast: throws when the server cannot be reached). Reconnect
    /// policy follows the Go services: retry every second for 15 minutes.
    /// </summary>
    public static async Task<NatsConnection> ConnectAsync(NatsSettings? settings, string defaultName, ILoggerFactory loggerFactory, CancellationToken ct)
    {
        settings ??= new NatsSettings();
        var url = NonEmpty(settings.Url) ?? NonEmpty(Environment.GetEnvironmentVariable("NATS_URL"))
            ?? throw new InvalidOperationException("NATS_URL is not set");
        var jwt = NonEmpty(settings.Jwt) ?? NonEmpty(Environment.GetEnvironmentVariable("NATS_JWT"));
        var seed = NonEmpty(settings.Seed) ?? NonEmpty(Environment.GetEnvironmentVariable("NATS_KEY"));
        var name = NonEmpty(settings.Name) ?? NonEmpty(Environment.GetEnvironmentVariable("APPID")) ?? defaultName;

        var opts = NatsOpts.Default with
        {
            Url = url,
            Name = name,
            ReconnectWaitMin = ReconnectDelay,
            ReconnectWaitMax = ReconnectDelay,
            MaxReconnectRetry = (int)(TotalReconnectWait / ReconnectDelay),
            // The client's own logging stays quiet unless the application wires a factory.
            LoggerFactory = loggerFactory is StdoutLoggerFactory ? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance : loggerFactory,
        };
        if (jwt is not null && seed is not null)
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { Jwt = jwt.Trim(), Seed = seed.Trim() } };
        }

        var logger = loggerFactory.CreateLogger("TransactRx.NatsAgent");
        var nc = new NatsConnection(opts);
        nc.ConnectionDisconnected += (_, _) =>
        {
            logger.LogWarning("NATS disconnected, will attempt reconnects for {Minutes}m", (int)TotalReconnectWait.TotalMinutes);
            return ValueTask.CompletedTask;
        };
        var failures = 0;
        nc.ConnectionOpened += (_, _) =>
        {
            Interlocked.Exchange(ref failures, 0);
            return ValueTask.CompletedTask;
        };
        nc.ReconnectFailed += (_, _) =>
        {
            // Raised once per failed attempt.
            if (Interlocked.Increment(ref failures) >= opts.MaxReconnectRetry)
            {
                logger.LogCritical("NATS connection lost and not restored after {Minutes}m", (int)TotalReconnectWait.TotalMinutes);
                if (settings.ExitWhenConnectionLost)
                {
                    Environment.Exit(1);
                }
            }

            return ValueTask.CompletedTask;
        };

        try
        {
            await nc.ConnectAsync().AsTask().WaitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            await nc.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        logger.LogInformation("Connected to NATS server at {Url}", url);
        return nc;
    }

    private static string? NonEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;
}
