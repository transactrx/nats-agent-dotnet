using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Progress;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Client;

/// <summary>Tunes one chat turn. The default value behaves exactly as a plain chat.</summary>
public sealed record ChatOptions
{
    /// <summary>
    /// When greater than zero, cuts the run when no progress (any event but a ping) arrives
    /// for this long: the client publishes a best-effort cancel (reason progress_idle) and
    /// ends the stream with an error event, reason <see cref="Reasons.ProgressIdle"/>. It
    /// applies only to agents whose ack declares progress (<see cref="AckCaps.Progress"/>);
    /// for others it is ignored. Zero = no idle cut.
    /// </summary>
    public TimeSpan IdleWindow { get; init; }
}

/// <summary>
/// The consumer side of the NATS Agent Protocol: UIs, services, and other agents use it to
/// discover agents and run chat turns against them. Create one per process and share it.
/// </summary>
public sealed partial class AgentClient : IAsyncDisposable
{
    /// <summary>Bounds simple request/reply calls (card, ping, chat ack, sessions).</summary>
    public static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(10);

    /// <summary>How long discovery collects scatter-gather replies.</summary>
    public static readonly TimeSpan DefaultDiscoverWindow = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long a run may go without any event (agents heartbeat every ≤15s) before the
    /// client declares it dead.
    /// </summary>
    public static readonly TimeSpan DeadRunTimeout = TimeSpan.FromSeconds(45);

    /// <summary>
    /// Bounds one renew request/reply. The agent's renew handler may make up to two fresh
    /// identity checks (5 s each).
    /// </summary>
    public static readonly TimeSpan RenewTimeout = TimeSpan.FromSeconds(12);

    private static readonly TimeSpan CancelTimeout = TimeSpan.FromSeconds(2);

    private readonly NatsConnection _nc;
    private readonly NatsServiceClient _svc;
    private readonly bool _ownsConnection;
    private readonly ILogger _logger;

    /// <summary>Wraps an existing connection; the caller keeps ownership.</summary>
    public AgentClient(NatsConnection connection, ILoggerFactory? loggerFactory = null)
        : this(connection, ownsConnection: false, loggerFactory)
    {
    }

    private AgentClient(NatsConnection connection, bool ownsConnection, ILoggerFactory? loggerFactory)
    {
        _nc = connection;
        _ownsConnection = ownsConnection;
        _logger = (loggerFactory ?? StdoutLoggerFactory.Instance).CreateLogger("TransactRx.NatsAgent.Client");
        _svc = new NatsServiceClient(connection, _logger);
    }

    [GeneratedRegex("^[A-Za-z0-9_-]+$")]
    private static partial Regex RenewTokenRegex();

    /// <summary>The underlying NATS connection.</summary>
    public NatsConnection Connection => _nc;

    /// <summary>Timeout of the simple request/reply calls.</summary>
    public TimeSpan RequestTimeout { get; set; } = DefaultRequestTimeout;

    /// <summary>Clock of the run watchdogs (tests).</summary>
    internal TimeProvider Time { get; set; } = TimeProvider.System;

    /// <summary>Test seam: <see cref="DeadRunTimeout"/> for runs started by this client.</summary>
    internal TimeSpan DeadRunTimeoutOverride { get; set; } = DeadRunTimeout;

    /// <summary>
    /// Connects using the org env conventions (<c>NATS_URL</c>, <c>NATS_JWT</c>,
    /// <c>NATS_KEY</c>) unless <paramref name="settings"/> says otherwise. The client owns the
    /// connection and closes it on dispose.
    /// </summary>
    public static async Task<AgentClient> ConnectAsync(NatsSettings? settings = null, ILoggerFactory? loggerFactory = null, CancellationToken cancellationToken = default)
    {
        var nc = await NatsConnector.ConnectAsync(
            settings, "unknownDotnetClientConnection", loggerFactory ?? StdoutLoggerFactory.Instance, cancellationToken).ConfigureAwait(false);
        return new AgentClient(nc, ownsConnection: true, loggerFactory);
    }

    /// <summary>Closes the connection if the client owns it.</summary>
    public ValueTask DisposeAsync() => _ownsConnection ? _nc.DisposeAsync() : ValueTask.CompletedTask;

    /// <summary>
    /// Scatter-gathers agent cards from every agent matching the filter. A null
    /// <paramref name="window"/> (or zero) uses <see cref="DefaultDiscoverWindow"/>.
    /// </summary>
    public async Task<IReadOnlyList<AgentCard>> DiscoverAsync(DiscoverFilter? filter = null, TimeSpan? window = null, CancellationToken cancellationToken = default)
    {
        var replies = await Discovery.ScatterGatherAsync(
            _nc, Protocol.AgentDiscoverSubject, WireJson.Serialize(filter ?? new DiscoverFilter()),
            window is { } w && w > TimeSpan.Zero ? w : DefaultDiscoverWindow, cancellationToken).ConfigureAwait(false);
        var seen = new HashSet<string>();
        var cards = new List<AgentCard>();
        foreach (var data in replies)
        {
            if (WireJson.TryDeserialize<AgentCard>(data, out var card) && !string.IsNullOrEmpty(card.Name) && seen.Add(card.Name))
            {
                cards.Add(card);
            }
        }

        return cards;
    }

    /// <summary>
    /// Fetches one agent's card directly. Unauthenticated by protocol: never sends X-TRX-IDT,
    /// even inside a <see cref="CallContext.WithIdt"/> scope.
    /// </summary>
    public Task<AgentCard> CardAsync(string agent, CancellationToken cancellationToken = default) =>
        RequestJson<AgentCard>(Protocol.AgentSubject(agent, "card"), new { }, authenticated: false, cancellationToken);

    /// <summary>
    /// Checks agent liveness. Unauthenticated by protocol: never sends X-TRX-IDT, even inside
    /// a <see cref="CallContext.WithIdt"/> scope.
    /// </summary>
    public Task<PingResponse> PingAsync(string agent, CancellationToken cancellationToken = default) =>
        RequestJson<PingResponse>(Protocol.AgentSubject(agent, "ping"), new { }, authenticated: false, cancellationToken);

    /// <summary>
    /// Starts a streaming chat turn. If <see cref="ChatRequest.StreamSubject"/> is empty, a
    /// unique inbox is generated. The returned run's <see cref="ChatRun.Events"/> deliver
    /// everything from start through the terminal event.
    /// <para>
    /// <paramref name="cancellationToken"/> governs the whole turn, not just the ack: if the
    /// agent goes silent past <see cref="DeadRunTimeout"/> or the token is cancelled, the
    /// client synthesizes a terminal error event (and, for cancellation, fires a best-effort
    /// cancel at the agent).
    /// </para>
    /// </summary>
    /// <exception cref="NatsServiceException">The agent refused the request (SPEC §9 envelope).</exception>
    public async Task<ChatRun> ChatAsync(string agent, ChatRequest request, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (request.Message?.Content is not { Count: > 0 })
        {
            throw new ArgumentException("chat request needs a message with content", nameof(request));
        }

        if (string.IsNullOrEmpty(request.StreamSubject))
        {
            request = request with { StreamSubject = _nc.NewInbox() };
        }

        request = WithAmbientContext(request);

        // Subscribed before the request goes out on the same connection, so no frame is missed.
        var sub = await _nc.SubscribeCoreAsync(
            request.StreamSubject, serializer: Framing.Raw,
            opts: new NatsSubOpts { ChannelOpts = new NatsSubChannelOpts { Capacity = 512 } },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        ChatAck ack;
        try
        {
            ack = await RequestJson<ChatAck>(Protocol.AgentSubject(agent, "chat"), request, authenticated: true, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await Background.Close(sub).ConfigureAwait(false);
            throw;
        }

        var events = Channel.CreateBounded<AgentEvent>(new BoundedChannelOptions(64) { SingleReader = false, SingleWriter = true });
        var run = new ChatRun(this, agent, ack, events.Reader);

        // The no-progress cut applies only to agents that declared progress; others promised
        // pings only and run exactly as a plain chat.
        var window = run.Caps.Progress ? options?.IdleWindow ?? TimeSpan.Zero : TimeSpan.Zero;
        var reader = new StreamReader(ack.RunId, window, sub.Msgs, events.Writer, w => new ProgressWatcher(w, Time))
        {
            CancelRemote = reason => PublishCancel(agent, ack.RunId, reason),
        };

        _ = Background.Run(async () =>
        {
            try
            {
                await reader.RunAsync(Time, DeadRunTimeoutOverride, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError("run {RunId}: stream reader failed: {Error}", ack.RunId, ex.Message);
            }
            finally
            {
                reader.Stop();
                events.Writer.TryComplete();
                await Background.Close(sub).ConfigureAwait(false);
            }
        });

        return run;
    }

    /// <summary>
    /// Asks all instances of an agent to cancel a run; the owner replies. Returns false
    /// (without error) when nothing owns the run — already finished, unknown, or the owner
    /// died.
    /// </summary>
    public Task<bool> CancelAsync(string agent, string runId, CancellationToken cancellationToken = default) =>
        CancelWithReasonAsync(agent, runId, null, cancellationToken);

    /// <summary>
    /// <see cref="CancelAsync"/> carrying a cut reason (see
    /// <see cref="ChatRun.CancelWithReasonAsync"/>); null or empty = a plain cancel.
    /// </summary>
    public async Task<bool> CancelWithReasonAsync(string agent, string runId, string? reason, CancellationToken cancellationToken = default)
    {
        var data = WireJson.Serialize(new CancelRequest { RunId = runId, Reason = string.IsNullOrEmpty(reason) ? null : reason });
        NatsMsg<byte[]> reply;
        try
        {
            reply = await _nc.RequestAsync<byte[], byte[]>(
                Protocol.AgentSubject(agent, "cancel"), data, null, Framing.Raw, Framing.Raw,
                replyOpts: new NatsSubOpts { Timeout = CancelTimeout },
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (NatsException)
        {
            return false; // silence = nobody owns it
        }

        return WireJson.Deserialize<CancelResponse>(reply.Data ?? [])?.Cancelled ?? false;
    }

    /// <summary>Runs a synchronous turn (capability "sync").</summary>
    public Task<InvokeResponse> InvokeAsync(string agent, ChatRequest request, CancellationToken cancellationToken = default) =>
        RequestJson<InvokeResponse>(
            Protocol.AgentSubject(agent, "invoke"), WithAmbientContext(request with { StreamSubject = "" }), authenticated: true, cancellationToken);

    // ─── Sessions (capability "sessions") ─────────────────────────────────

    public Task<SessionsListResponse> SessionsListAsync(string agent, string userId, CancellationToken cancellationToken = default) =>
        RequestJson<SessionsListResponse>(Protocol.AgentSubject(agent, "sessionsList"), new SessionsListRequest { UserId = userId }, authenticated: true, cancellationToken);

    public Task<SessionGetResponse> SessionGetAsync(string agent, string userId, string sessionId, CancellationToken cancellationToken = default) =>
        RequestJson<SessionGetResponse>(Protocol.AgentSubject(agent, "sessionsGet"), new SessionGetRequest { UserId = userId, SessionId = sessionId }, authenticated: true, cancellationToken);

    public Task<SessionDeleteResponse> SessionDeleteAsync(string agent, string userId, string sessionId, CancellationToken cancellationToken = default) =>
        RequestJson<SessionDeleteResponse>(Protocol.AgentSubject(agent, "sessionsDelete"), new SessionDeleteRequest { UserId = userId, SessionId = sessionId }, authenticated: true, cancellationToken);

    public Task<SessionRenameResponse> SessionRenameAsync(string agent, string userId, string sessionId, string title, CancellationToken cancellationToken = default) =>
        RequestJson<SessionRenameResponse>(Protocol.AgentSubject(agent, "sessionsRename"), new SessionRenameRequest { UserId = userId, SessionId = sessionId, Title = title }, authenticated: true, cancellationToken);

    public Task<SessionSetFavoriteResponse> SessionSetFavoriteAsync(string agent, string userId, string sessionId, bool favorite, CancellationToken cancellationToken = default) =>
        RequestJson<SessionSetFavoriteResponse>(Protocol.AgentSubject(agent, "sessionsSetFavorite"), new SessionSetFavoriteRequest { UserId = userId, SessionId = sessionId, Favorite = favorite }, authenticated: true, cancellationToken);

    // ─── internals ────────────────────────────────────────────────────────

    /// <summary>Does one documented request/reply against an agent endpoint.</summary>
    private async Task<T> RequestJson<T>(string subject, object body, bool authenticated, CancellationToken ct)
        where T : class
    {
        var data = JsonSerializer.SerializeToUtf8Bytes(body, body.GetType(), WireJson.Options);
        var reply = await _svc.RequestAsync(subject, authenticated ? CallContext.IdtHeader() : null, data, RequestTimeout, ct).ConfigureAwait(false);
        try
        {
            return WireJson.Deserialize<T>(reply) ?? throw new InvalidDataException($"decoding reply from {subject}: empty reply");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException($"decoding reply from {subject}: {ex.Message}", ex);
        }
    }

    // The user's display context follows them from agent to agent; an explicit request
    // context wins.
    private static ChatRequest WithAmbientContext(ChatRequest request) =>
        request.Context is null && CallContext.UserContext is { } ambient ? request with { Context = ambient } : request;

    private async ValueTask PublishCancel(string agent, string runId, string? reason)
    {
        var data = WireJson.Serialize(new CancelRequest { RunId = runId, Reason = string.IsNullOrEmpty(reason) ? null : reason });
        try
        {
            await _nc.PublishAsync(Protocol.AgentSubject(agent, "cancel"), data, serializer: Framing.Raw).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // best effort
        }
    }
}

internal static class Discovery
{
    /// <summary>
    /// Publishes a request and collects every reply that arrives within the window. Shared by
    /// agent and tool discovery.
    /// </summary>
    public static async Task<List<byte[]>> ScatterGatherAsync(NatsConnection nc, string subject, byte[] body, TimeSpan window, CancellationToken ct)
    {
        var inbox = nc.NewInbox();
        var sub = await nc.SubscribeCoreAsync(inbox, serializer: Framing.Raw, cancellationToken: ct).ConfigureAwait(false);
        try
        {
            await nc.PublishAsync(subject, body, replyTo: inbox, serializer: Framing.Raw, cancellationToken: ct).ConfigureAwait(false);

            var replies = new List<byte[]>();
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            deadline.CancelAfter(window);
            try
            {
                await foreach (var msg in sub.Msgs.ReadAllAsync(deadline.Token).ConfigureAwait(false))
                {
                    // A "no responders" status means nobody listens at all: the window is moot.
                    if (msg.HasNoResponders)
                    {
                        break;
                    }

                    if (msg.Data is { Length: > 0 } data)
                    {
                        replies.Add(data);
                    }
                }
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // window closed
            }

            return replies;
        }
        finally
        {
            await Background.Close(sub).ConfigureAwait(false);
        }
    }
}
