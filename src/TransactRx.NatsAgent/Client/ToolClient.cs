using System.Text.Json;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Client;

/// <summary>
/// Consumes network agentic tools (SPEC.md §8): direct discovery and execution. See
/// <see cref="ToolRegistry"/> for a live view of the tool mesh.
/// </summary>
public sealed class ToolClient : IAsyncDisposable
{
    public static readonly TimeSpan DefaultDiscoverWindow = TimeSpan.FromSeconds(2);

    /// <summary>Added to a tool card's advisory timeoutSeconds to size the request timeout.</summary>
    internal static readonly TimeSpan RunTimeoutMargin = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Applies when neither the caller nor the card names one: the host's default advisory
    /// timeout plus margin.
    /// </summary>
    internal static readonly TimeSpan DefaultRunTimeout = TimeSpan.FromSeconds(30) + RunTimeoutMargin;

    private readonly NatsConnection _nc;
    private readonly NatsServiceClient _svc;
    private readonly bool _ownsConnection;

    /// <summary>Wraps an existing connection; the caller keeps ownership.</summary>
    public ToolClient(NatsConnection connection, ILoggerFactory? loggerFactory = null)
        : this(connection, ownsConnection: false, loggerFactory)
    {
    }

    private ToolClient(NatsConnection connection, bool ownsConnection, ILoggerFactory? loggerFactory)
    {
        _nc = connection;
        _ownsConnection = ownsConnection;
        Logger = (loggerFactory ?? StdoutLoggerFactory.Instance).CreateLogger("TransactRx.NatsAgent.Tools");
        _svc = new NatsServiceClient(connection, Logger);
    }

    /// <summary>The underlying NATS connection.</summary>
    public NatsConnection Connection => _nc;

    internal ILogger Logger { get; }

    /// <summary>
    /// Connects using the org env conventions (<c>NATS_URL</c>, <c>NATS_JWT</c>,
    /// <c>NATS_KEY</c>) unless <paramref name="settings"/> says otherwise. The client owns the
    /// connection and closes it on dispose.
    /// </summary>
    public static async Task<ToolClient> ConnectAsync(NatsSettings? settings = null, ILoggerFactory? loggerFactory = null, CancellationToken cancellationToken = default)
    {
        var nc = await NatsConnector.ConnectAsync(
            settings, "unknownDotnetClientConnection", loggerFactory ?? StdoutLoggerFactory.Instance, cancellationToken).ConfigureAwait(false);
        return new ToolClient(nc, ownsConnection: true, loggerFactory);
    }

    /// <summary>Closes the connection if the client owns it.</summary>
    public ValueTask DisposeAsync() => _ownsConnection ? _nc.DisposeAsync() : ValueTask.CompletedTask;

    /// <summary>Scatter-gathers tool cards from every tool matching the filter.</summary>
    public async Task<IReadOnlyList<ToolCard>> DiscoverAsync(DiscoverFilter? filter = null, TimeSpan? window = null, CancellationToken cancellationToken = default)
    {
        var replies = await Discovery.ScatterGatherAsync(
            _nc, Protocol.ToolDiscoverSubject, WireJson.Serialize(filter ?? new DiscoverFilter()),
            window is { } w && w > TimeSpan.Zero ? w : DefaultDiscoverWindow, cancellationToken).ConfigureAwait(false);
        var seen = new HashSet<string>();
        var cards = new List<ToolCard>();
        foreach (var data in replies)
        {
            if (WireJson.TryDeserialize<ToolCard>(data, out var card) && !string.IsNullOrEmpty(card.Name) && seen.Add(card.Name))
            {
                cards.Add(card);
            }
        }

        return cards;
    }

    /// <summary>Fetches one tool's card directly.</summary>
    public async Task<ToolCard> CardAsync(string name, CancellationToken cancellationToken = default)
    {
        var reply = await _svc.RequestAsync(Protocol.ToolSubject(name, "card"), null, "{}"u8.ToArray(), TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
        try
        {
            return WireJson.Deserialize<ToolCard>(reply) ?? throw new InvalidDataException($"decoding card of tool {name}: empty reply");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException($"decoding card of tool {name}: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Executes a tool. A null (or zero) <paramref name="timeout"/> defaults to the standard
    /// advisory timeout plus margin; when the caller knows the card, prefer
    /// <see cref="ToolRegistry.RunAsync"/> so the timeout matches the tool's own advertisement.
    /// <para>
    /// The ambient token (<see cref="CallContext.WithIdt"/>) is sent as the X-TRX-IDT header
    /// (SPEC §8.4). The call returns when <paramref name="cancellationToken"/> is cancelled
    /// even if the tool has not replied; the tool itself is not interrupted.
    /// </para>
    /// </summary>
    /// <exception cref="NatsServiceException">The tool refused the request (SPEC §9 envelope).</exception>
    /// <exception cref="TimeoutException">No reply in time.</exception>
    public async Task<ToolRunResponse> RunAsync(string name, ToolRunRequest request, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var effective = timeout is { } t && t > TimeSpan.Zero ? t : DefaultRunTimeout;
        if (request.Input is null || request.Input.Value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            request = request with { Input = WireJson.EmptyObject };
        }

        byte[] reply;
        try
        {
            reply = await _svc.RequestAsync(
                Protocol.ToolSubject(name, "run"), CallContext.IdtHeader(), WireJson.Serialize(request), effective, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException ex)
        {
            throw new TimeoutException($"running tool {name}: {ex.Message}", ex);
        }

        try
        {
            return WireJson.Deserialize<ToolRunResponse>(reply) ?? throw new InvalidDataException($"decoding reply of tool {name}: empty reply");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException($"decoding reply of tool {name}: {ex.Message}", ex);
        }
    }
}
