using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent;

/// <summary>
/// Runs one streaming chat turn. Emit events through the stream; if the handler returns
/// without emitting a terminal event, the runtime emits done (returned normally), error (threw)
/// or done/cancelled (the run was cancelled). When the run was cut (<see cref="Turn.CutReason"/>:
/// the request budget ran out, <see cref="Agent.InterruptAll"/>, or a cancel carrying a cut
/// reason), the fallback is an error event carrying that reason. The token is cancelled when
/// the run is cancelled or cut.
/// </summary>
public delegate Task ChatHandler(Turn turn, AgentStream stream, CancellationToken cancellationToken);

/// <summary>Runs one synchronous turn (capability "sync").</summary>
public delegate Task<InvokeResponse> InvokeHandler(Turn turn, CancellationToken cancellationToken);

/// <summary>
/// The server side of the NATS Agent Protocol (SPEC.md). Supply a card and a chat handler;
/// the runtime provides the subject space, discovery, ack/stream/heartbeat plumbing,
/// cancellation, and optional sync + session endpoints. Every endpoint is also visible to
/// <c>nats-discover</c>.
/// </summary>
public sealed partial class Agent : IAsyncDisposable
{
    /// <summary>
    /// How much earlier than the caller's budget the run ends, so the handler can still send a
    /// clean terminal frame.
    /// </summary>
    public static readonly TimeSpan BudgetMargin = TimeSpan.FromSeconds(5);

    /// <summary>The shortest deadline a budget can produce.</summary>
    internal static readonly TimeSpan MinBudget = TimeSpan.FromSeconds(1);

    // Names that would collide with the common discovery subjects.
    private static readonly HashSet<string> ReservedNames = ["discover", "announce"];

    // Endpoint paths owned by the runtime; extensions may not use them.
    private static readonly HashSet<string> ReservedEndpoints = ["card", "ping", "chat", "cancel", "invoke"];

    // The cancel reasons treated as a cut (→ fallback error text); any other reason is a plain cancel.
    private static readonly Dictionary<string, string> CutReasons = new()
    {
        [Reasons.ProgressIdle] = "stopped: no progress",
        [Reasons.TokenRotation] = "stopped: access could not be renewed",
        [Reasons.Interrupted] = "interrupted",
        [Reasons.TimeCap] = "time limit reached",
        [Reasons.Unavailable] = "stopped: a required service was unavailable",
    };

    private readonly AgentConfig _cfg;
    private readonly NatsConnection _nc;
    private readonly bool _ownsConnection;
    private readonly NatsService _svc;
    private readonly NatsService? _regional;
    private readonly IdtValidator _idt;
    private readonly ILogger _logger;
    private readonly TimeProvider _time;
    private readonly List<EndpointRegistration> _extra = [];
    private readonly object _gate = new();
    private readonly Dictionary<string, AgentRun> _runs = [];

    private ChatHandler? _chat;
    private InvokeHandler? _invoke;
    private ISessionStore? _sessions;
    private long _startTimestamp;
    private INatsSub<byte[]>? _discoverSub;
    private INatsSub<byte[]>? _cancelSub;
    private INatsSub<byte[]>? _regionalCancelSub;
    private int _disposed;

    private Agent(AgentConfig cfg, NatsConnection nc, bool ownsConnection, ILoggerFactory loggerFactory)
    {
        _cfg = cfg;
        _nc = nc;
        _ownsConnection = ownsConnection;
        _logger = loggerFactory.CreateLogger("TransactRx.NatsAgent.Agent");
        _time = cfg.TimeProvider ?? TimeProvider.System;

        var instanceId = NatsService.NewInstanceId();
        _svc = new NatsService(nc, Protocol.AgentPrefix + "." + cfg.Name, "agent." + cfg.Name, instanceId, _logger)
        {
            Description = cfg.Description,
            RepositoryUrl = cfg.RepositoryUrl,
        };
        if (!string.IsNullOrEmpty(cfg.Region))
        {
            var alias = cfg.Name + "_" + cfg.Region;
            _regional = new NatsService(nc, Protocol.AgentPrefix + "." + alias, "agent." + alias, instanceId, _logger)
            {
                Description = "Regional route for " + cfg.Name + " in " + cfg.Region,
            };
        }

        _idt = new IdtValidator(nc, cfg.Access, cfg.IdtValidation!, _logger, _time);
    }

    [GeneratedRegex("^[a-zA-Z0-9_-]+$")]
    private static partial Regex NameRegex();

    /// <summary>The underlying NATS connection (e.g. for a tool registry sharing it).</summary>
    public NatsConnection Connection => _nc;

    /// <summary>The unique identifier of this running instance.</summary>
    public string InstanceId => _svc.InstanceId;

    /// <summary>How many chat/invoke runs this instance is serving.</summary>
    public int ActiveRuns
    {
        get
        {
            lock (_gate)
            {
                return _runs.Count;
            }
        }
    }

    /// <summary>
    /// Creates an agent. The NATS connection is established immediately (fail-fast);
    /// endpoints are registered on <see cref="StartAsync"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The configuration is invalid.</exception>
    public static async Task<Agent> CreateAsync(AgentConfig config, CancellationToken cancellationToken = default)
    {
        var loggerFactory = config.LoggerFactory ?? StdoutLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger("TransactRx.NatsAgent.Agent");
        var cfg = Validate(config, Environment.GetEnvironmentVariable, logger);

        var nc = cfg.Connection;
        var owns = false;
        if (nc is null)
        {
            try
            {
                nc = await NatsConnector.ConnectAsync(cfg.Nats, "unknownDotnetServiceConnection", loggerFactory, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException and not InvalidOperationException)
            {
                throw new InvalidOperationException($"connecting agent \"{cfg.Name}\" to NATS: {ex.Message}", ex);
            }

            owns = true;
        }

        var agent = new Agent(cfg, nc, owns, loggerFactory);
        if (cfg.IdtValidation!.Enabled)
        {
            // Log the validator's effective (defaulted) subject, not the configured one, which
            // may still be empty here.
            logger.LogInformation(
                "agent \"{Name}\": IDT validation enabled (subject={Subject} appId={AppId} functionId={FunctionId} observeOnly={ObserveOnly} failOpen={FailOpen} cacheTTL={CacheTtl})",
                cfg.Name, agent._idt.Config.Subject, cfg.Access!.AppId, cfg.Access.FunctionId,
                cfg.IdtValidation.ObserveOnly, cfg.IdtValidation.FailOpen, GoDuration.Format(cfg.IdtValidation.CacheTtl));
        }

        return agent;
    }

    /// <summary>Checks the configuration and fills its defaults (modalities, access and validation from env).</summary>
    internal static AgentConfig Validate(AgentConfig cfg, Func<string, string?> env, ILogger logger)
    {
        if (cfg.Name is null || !NameRegex().IsMatch(cfg.Name))
        {
            throw new ArgumentException($"invalid agent name \"{cfg.Name}\": must match ^[a-zA-Z0-9_-]+$");
        }

        if (!string.IsNullOrEmpty(cfg.Region) && !NameRegex().IsMatch(cfg.Region))
        {
            throw new ArgumentException($"invalid agent region \"{cfg.Region}\"");
        }

        if (ReservedNames.Contains(cfg.Name))
        {
            throw new ArgumentException($"agent name \"{cfg.Name}\" is reserved");
        }

        if (string.IsNullOrEmpty(cfg.Description))
        {
            throw new ArgumentException("agent description is required");
        }

        if (cfg.InputModalities is not { Count: > 0 })
        {
            cfg = cfg with { InputModalities = ["text"] };
        }

        if (cfg.OutputModalities is not { Count: > 0 })
        {
            cfg = cfg with { OutputModalities = ["text"] };
        }

        if (cfg.Access is null)
        {
            cfg = cfg with { Access = AccessFromEnvironment(env, logger) };
        }

        // A caller-supplied Access must be complete regardless of whether IDT validation is
        // enabled: a partial Access still ends up on the public card, and downstream identity
        // checks treat an empty functionId as BAD_REQUEST for every caller.
        if (cfg.Access is not null && (string.IsNullOrEmpty(cfg.Access.AppId) || string.IsNullOrEmpty(cfg.Access.FunctionId)))
        {
            throw new ArgumentException(
                $"agent \"{cfg.Name}\": Access requires both AppId and FunctionId (got appId=\"{cfg.Access.AppId}\" functionId=\"{cfg.Access.FunctionId}\")");
        }

        if (cfg.IdtValidation is null)
        {
            cfg = cfg with { IdtValidation = IdtValidation.FromEnvironment(env, logger) };
        }

        if (cfg.IdtValidation!.Enabled && cfg.Access is null)
        {
            throw new ArgumentException(
                $"agent \"{cfg.Name}\": IDT_VALIDATION=true requires Access.AppId and Access.FunctionId (env APP_ID / APP_FUNCTION_ID)");
        }

        return cfg;
    }

    /// <summary>
    /// The <see cref="AgentConfig.Access"/> fallback (<c>APP_ID</c> / <c>APP_FUNCTION_ID</c>).
    /// Returns null unless BOTH are set: a partial declaration would otherwise advertise a
    /// card with an empty functionId, which downstream identity checks treat as BAD_REQUEST
    /// for every caller.
    /// </summary>
    internal static AgentAccess? AccessFromEnvironment(Func<string, string?> env, ILogger logger)
    {
        var appId = (env("APP_ID") ?? "").Trim();
        var fnId = (env("APP_FUNCTION_ID") ?? "").Trim();
        if (appId.Length == 0 && fnId.Length == 0)
        {
            return null;
        }

        if (appId.Length == 0 || fnId.Length == 0)
        {
            logger.LogWarning("IDT_METRIC event=access.misconfig reason=partial_env");
            return null;
        }

        return new AgentAccess { AppId = appId, FunctionId = fnId };
    }

    /// <summary>Sets the streaming chat handler (required).</summary>
    public void OnChat(ChatHandler handler) => _chat = handler;

    /// <summary>Sets the synchronous handler and advertises capability "sync".</summary>
    public void OnInvoke(InvokeHandler handler) => _invoke = handler;

    /// <summary>Wires the session endpoints and advertises capability "sessions".</summary>
    public void UseSessionStore(ISessionStore store) => _sessions = store;

    /// <summary>
    /// Registers an agent-specific extension endpoint in the agent's subject space,
    /// documented for <c>nats-discover</c>. Must be called before <see cref="StartAsync"/>.
    /// Extension endpoints are unauthenticated unless the handler calls
    /// <see cref="AuthorizeAsync"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The path is reserved by the agent protocol.</exception>
    public void AddEndpoint(EndpointRegistration registration)
    {
        if (ReservedEndpoints.Contains(registration.Path))
        {
            throw new ArgumentException($"endpoint path \"{registration.Path}\" is reserved by the agent protocol");
        }

        _extra.Add(registration);
    }

    /// <summary>
    /// Checks the request's X-TRX-IDT header against this agent's configured access function.
    /// Use from <see cref="AddEndpoint"/> handlers that expose user data; the exception it
    /// throws on deny is the 403 envelope the service replies with. Built-in
    /// chat/invoke/sessions endpoints call this automatically.
    /// </summary>
    /// <exception cref="NatsServiceException">Denied (403 / 4031).</exception>
    public Task<CallerIdentity> AuthorizeAsync(ServiceRequest request, string? sessionId = null, CancellationToken cancellationToken = default) =>
        _idt.AuthorizeAsync(request.Header(Protocol.HeaderIdt), sessionId, cancellationToken);

    /// <summary>Builds the agent card from the config and wired capabilities.</summary>
    public AgentCard Card()
    {
        var endpoints = new List<string> { "card", "ping", "chat", "cancel" };
        if (_invoke is not null)
        {
            endpoints.Add("invoke");
        }

        if (_sessions is not null)
        {
            endpoints.AddRange(["sessionsList", "sessionsGet", "sessionsDelete", "sessionsRename", "sessionsSetFavorite"]);
        }

        endpoints.AddRange(_extra.Select(e => e.Path));
        return new AgentCard
        {
            ProtocolVersion = Protocol.Version,
            Kind = Protocol.KindAgent,
            Name = _cfg.Name,
            DisplayName = NullIfEmpty(_cfg.DisplayName),
            Description = _cfg.Description,
            Version = NullIfEmpty(_cfg.Version),
            RepositoryUrl = NullIfEmpty(_cfg.RepositoryUrl),
            Tags = _cfg.Tags is { Count: > 0 } ? _cfg.Tags : null,
            Category = NullIfEmpty(_cfg.Category),
            Access = _cfg.Access,
            Capabilities = new Capabilities
            {
                Streaming = true,
                Sync = _invoke is not null,
                Sessions = _sessions is not null,
                Attachments = _cfg.Attachments,
                UserContext = _cfg.UserContext,
                Progress = _cfg.Progress,
                Renew = _cfg.Renew,
            },
            InputModalities = _cfg.InputModalities,
            OutputModalities = _cfg.OutputModalities,
            Skills = _cfg.Skills is { Count: > 0 } ? _cfg.Skills : null,
            Endpoints = endpoints,
            Metadata = _cfg.Metadata is { Count: > 0 } ? _cfg.Metadata : null,
        };
    }

    /// <summary>Registers all endpoints and subscriptions and begins serving.</summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_chat is null)
        {
            throw new InvalidOperationException($"agent \"{_cfg.Name}\" has no chat handler");
        }

        _startTimestamp = _time.GetTimestamp();

        var regs = new List<EndpointRegistration>
        {
            new()
            {
                Path = "card",
                Description = "Agent card: self-description with capabilities, skills, and endpoints (agent protocol v" + Protocol.Version + ").",
                Response = new ResponseDoc { Description = "AgentCard", ContentType = "application/json" },
                Handler = (_, _) => Task.FromResult(WireJson.Serialize(Card())),
            },
            new()
            {
                Path = "ping",
                Description = "Liveness probe.",
                Response = new ResponseDoc { Description = "PingResponse", ContentType = "application/json" },
                Handler = HandlePing,
            },
            new()
            {
                Path = "chat",
                Description = "Streaming chat turn. Request: {sessionId?, userId?, message, streamSubject, metadata?, context?}. Replies with an ack; events stream to streamSubject until a terminal done/error event.",
                Response = new ResponseDoc { Description = "ChatAck", ContentType = "application/json" },
                Handler = HandleChat,
            },
            new()
            {
                Path = "cancel",
                Description = "Cancel a run by runId. Broadcast to all instances; the owning instance replies {cancelled:true}.",
                Response = new ResponseDoc { Description = "CancelResponse", ContentType = "application/json" },

                // The queue-balanced delivery must stay silent: the real work happens on the
                // broadcast subscription below, which every instance (including this one)
                // receives. Responding here would race the owner's reply.
                Handler = (_, _) => Task.FromResult<byte[]>([]),
                Silent = true,
            },
        };
        if (_cfg.Renew)
        {
            regs.Add(new EndpointRegistration
            {
                Path = "renew.>",
                Description = "Per-run renew subjects (trx.agent.<name>.renew.<random>), announced in the chat ack. Answered only by the owning run's own subscription.",

                // The service's queue subscription on trx.agent.<name>.> also receives renew
                // requests; stay silent so the owning run's reply (or no reply once it ended)
                // is the only answer.
                Handler = (_, _) => Task.FromResult<byte[]>([]),
                Silent = true,
            });
        }

        if (_invoke is not null)
        {
            regs.Add(new EndpointRegistration
            {
                Path = "invoke",
                Description = "Synchronous single-shot turn. Request: chat request without streamSubject; reply is the complete result.",
                Response = new ResponseDoc { Description = "InvokeResponse", ContentType = "application/json" },
                Handler = HandleInvoke,
            });
        }

        if (_sessions is not null)
        {
            regs.AddRange(SessionRegistrations());
        }

        regs.AddRange(_extra);

        _svc.AddEndpoints(regs);
        if (_regional is not null)
        {
            _regional.AddEndpoints(regs);
            await _regional.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        // Common discovery: one reply per agent (queue group), card on match.
        _discoverSub = await _nc.SubscribeCoreAsync(Protocol.AgentDiscoverSubject, "agent." + _cfg.Name, Framing.Raw, cancellationToken: cancellationToken).ConfigureAwait(false);
        _ = Background.Pump(_discoverSub, _logger, HandleDiscover);

        // Cancel broadcast: every instance listens; only the run owner replies.
        _cancelSub = await _nc.SubscribeCoreAsync(Protocol.AgentSubject(_cfg.Name, "cancel"), serializer: Framing.Raw, cancellationToken: cancellationToken).ConfigureAwait(false);
        _ = Background.Pump(_cancelSub, _logger, HandleCancel);
        if (_regional is not null)
        {
            _regionalCancelSub = await _nc.SubscribeCoreAsync(Protocol.AgentSubject(_cfg.Name + "_" + _cfg.Region, "cancel"), serializer: Framing.Raw, cancellationToken: cancellationToken).ConfigureAwait(false);
            _ = Background.Pump(_regionalCancelSub, _logger, HandleCancel);
        }

        await _svc.StartAsync(cancellationToken).ConfigureAwait(false);

        // Return only after the server has registered every subscription: callers may
        // immediately issue requests from another connection.
        await _nc.PingAsync(cancellationToken).ConfigureAwait(false);
        _logger.LogInformation("agent \"{Name}\" serving on {Prefix}.> (instance {InstanceId})", _cfg.Name, Protocol.AgentPrefix + "." + _cfg.Name, InstanceId);
    }

    /// <summary>
    /// Prepares this instance to stop without dropping work: it stops taking new requests
    /// (the queue group routes them to other instances), keeps cancel subscribed so users can
    /// still stop their runs, and waits until in-flight runs finish or
    /// <paramref name="cancellationToken"/> is cancelled. Call <see cref="ShutdownAsync"/>
    /// afterwards. Returns the number of runs still active when it gave up (0 = clean).
    /// </summary>
    public async Task<int> DrainAsync(CancellationToken cancellationToken = default)
    {
        if (_regional is not null)
        {
            await _regional.StopAcceptingAsync().ConfigureAwait(false);
        }

        await DrainSub(_discoverSub).ConfigureAwait(false);
        await _svc.StopAcceptingAsync().ConfigureAwait(false);
        while (true)
        {
            var n = ActiveRuns;
            if (n == 0)
            {
                return 0;
            }

            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return ActiveRuns;
            }
        }
    }

    /// <summary>
    /// Cancels every run this instance is serving as interrupted (e.g. when
    /// <see cref="DrainAsync"/> gave up before shutdown) and returns how many. Not a user
    /// cancel: a chat handler that returns without a terminal frame ends with an error event
    /// of reason "interrupted".
    /// </summary>
    public int InterruptAll()
    {
        AgentRun[] runs;
        lock (_gate)
        {
            runs = [.. _runs.Values];
        }

        foreach (var run in runs)
        {
            run.Cancel(Reasons.Interrupted);
        }

        return runs.Length;
    }

    /// <summary>
    /// Ends all subscriptions. In-flight runs keep their tokens until the process exits; use
    /// <see cref="DrainAsync"/> first to let them finish.
    /// </summary>
    public async Task ShutdownAsync()
    {
        if (_regional is not null)
        {
            await _regional.DisposeAsync().ConfigureAwait(false);
        }

        await DrainSub(_discoverSub).ConfigureAwait(false);
        await DrainSub(_regionalCancelSub).ConfigureAwait(false);
        await DrainSub(_cancelSub).ConfigureAwait(false);
        await _svc.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Shuts down and closes the connection if the agent opened it.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await ShutdownAsync().ConfigureAwait(false);
        if (_ownsConnection)
        {
            await _nc.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task DrainSub(INatsSub<byte[]>? sub)
    {
        if (sub is null)
        {
            return;
        }

        try
        {
            await sub.DrainAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            await Background.Close(sub).ConfigureAwait(false);
        }
    }

    // ─── plain subscriptions ──────────────────────────────────────────────

    private ValueTask HandleDiscover(NatsMsg<byte[]> msg)
    {
        if (string.IsNullOrEmpty(msg.ReplyTo))
        {
            return ValueTask.CompletedTask;
        }

        DiscoverFilter? filter = null;
        if (msg.Data is { Length: > 0 } data)
        {
            try
            {
                filter = WireJson.Deserialize<DiscoverFilter>(data);
            }
            catch (JsonException)
            {
                return ValueTask.CompletedTask; // malformed filters get silence, like a non-match
            }
        }

        var card = Card();
        return card.MatchesFilter(filter)
            ? msg.ReplyAsync(WireJson.Serialize(card), serializer: Framing.Raw)
            : ValueTask.CompletedTask;
    }

    private ValueTask HandleCancel(NatsMsg<byte[]> msg)
    {
        if (!WireJson.TryDeserialize<CancelRequest>(msg.Data ?? [], out var req) || string.IsNullOrEmpty(req.RunId))
        {
            return ValueTask.CompletedTask;
        }

        AgentRun? run;
        lock (_gate)
        {
            _runs.TryGetValue(req.RunId, out run);
        }

        if (run is null)
        {
            return ValueTask.CompletedTask;
        }

        // A known cut reason means the caller's limits ended the turn: not a user cancel.
        run.Cancel(req.Reason is not null && CutReasons.ContainsKey(req.Reason) ? req.Reason : null);
        return string.IsNullOrEmpty(msg.ReplyTo)
            ? ValueTask.CompletedTask
            : msg.ReplyAsync(WireJson.Serialize(new CancelResponse { Cancelled = true }), serializer: Framing.Raw);
    }

    // ─── endpoints ────────────────────────────────────────────────────────

    private Task<byte[]> HandlePing(ServiceRequest request, CancellationToken ct) =>
        Task.FromResult(WireJson.Serialize(new PingResponse
        {
            Status = "ok",
            Name = _cfg.Name,
            InstanceId = InstanceId,
            Version = NullIfEmpty(_cfg.Version),
            UptimeSeconds = (long)_time.GetElapsedTime(_startTimestamp).TotalSeconds,
        }));

    private async Task<Turn> ParseTurn(ServiceRequest request, bool needStream, CancellationToken ct)
    {
        ChatRequest req;
        try
        {
            req = WireJson.Deserialize<ChatRequest>(request.Body) ?? new ChatRequest();
        }
        catch (JsonException ex)
        {
            throw NatsServiceException.Validation("invalid request body", Codes.InvalidBody, ex);
        }

        if (req.Message?.Content is not { Count: > 0 })
        {
            throw NatsServiceException.Validation("message with content is required", Codes.MissingField);
        }

        if (string.IsNullOrEmpty(req.Message.Role))
        {
            req = req with { Message = req.Message with { Role = "user" } };
        }

        if (needStream && string.IsNullOrEmpty(req.StreamSubject))
        {
            throw NatsServiceException.Validation("streamSubject is required", Codes.MissingField);
        }

        req = req with
        {
            SessionId = string.IsNullOrEmpty(req.SessionId) ? Guid.NewGuid().ToString("D") : req.SessionId,
            StreamSubject = req.StreamSubject ?? "",

            // Display-only context: bad values are dropped, never fatal.
            Context = req.Context.Normalize(),
        };

        var id = await AuthorizeAsync(request, req.SessionId, ct).ConfigureAwait(false);
        if (id.Verified)
        {
            req = req with { UserId = id.UserId };
        }

        return new Turn(req, "run_" + Guid.NewGuid().ToString("D"), request.Logger, id, _time);
    }

    /// <summary>
    /// Builds one run: cancellable with a cause, plus a deadline (cause time_cap) when the
    /// request carries a budget. Without a budget the run has no deadline.
    /// </summary>
    internal AgentRun NewRun(Turn turn, bool honorBudget)
    {
        var run = new AgentRun();
        if (honorBudget && turn.Budget is { RemainingMs: > 0 } budget)
        {
            var delay = TimeSpan.FromMilliseconds(budget.RemainingMs) - BudgetMargin;
            run.SetDeadline(_time, delay < MinBudget ? MinBudget : delay);
        }

        turn.Run = run;
        lock (_gate)
        {
            _runs[turn.RunId] = run;
        }

        return run;
    }

    private void EndRun(Turn turn, AgentRun run)
    {
        lock (_gate)
        {
            _runs.Remove(turn.RunId);
        }

        run.Finish();
    }

    private async Task<byte[]> HandleChat(ServiceRequest request, CancellationToken ct)
    {
        var turn = await ParseTurn(request, needStream: true, ct).ConfigureAwait(false);

        // caps is absent unless the agent opted into a capability.
        var caps = _cfg.Progress || _cfg.Renew ? new AckCaps { Progress = _cfg.Progress } : null;

        // Private renew subject, subscribed before the ack goes out on this same connection
        // so the caller can use it right away.
        INatsSub<byte[]>? renewSub = null;
        if (_cfg.Renew)
        {
            try
            {
                (var subject, renewSub) = await SubscribeRenew(turn).ConfigureAwait(false);
                caps = caps! with { RenewSubject = subject };
            }
            catch (Exception ex)
            {
                turn.Logger.LogWarning("run {RunId}: renew unavailable: {Error}", turn.RunId, ex.Message);
            }
        }

        var ack = WireJson.Serialize(new ChatAck
        {
            Accepted = true,
            RunId = turn.RunId,
            SessionId = turn.SessionId,
            Agent = _cfg.Name,
            InstanceId = InstanceId,
            StreamSubject = turn.StreamSubject,
            Caps = caps,
        });

        var run = NewRun(turn, honorBudget: true);
        var stream = new AgentStream(
            (subject, data) => _nc.PublishAsync(subject, data, serializer: Framing.Raw),
            turn.StreamSubject, turn.RunId, turn.Logger, _cfg.Progress, _time);
        var chat = _chat!;

        _ = Background.Run(async () =>
        {
            try
            {
                Exception? error = null;
                try
                {
                    await stream.StartAsync(turn.SessionId).ConfigureAwait(false);

                    // Consults made from the handler forward the user's context automatically.
                    using (CallContext.WithUserContext(turn.Context))
                    {
                        await chat(turn, stream, run.Token).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    error = ex;
                }

                // The handler is done: stop taking renews before the terminal frame goes out,
                // so a caller that has seen the run end never gets a late renew applied.
                turn.Token.Close();

                if (stream.IsTerminated)
                {
                    // handler already ended the run
                }
                else if (run.CutReason is { } reason)
                {
                    // The first cause decides: a cut keeps its reason even if a user cancel
                    // arrives later. Fixed text per reason: the handler's raw error is not shown.
                    var text = CutReasons.GetValueOrDefault(reason, "turn cut: " + reason);
                    await stream.ErrorWithReasonAsync(text, Codes.Internal, reason).ConfigureAwait(false);
                }
                else if (run.UserCancelled)
                {
                    await stream.DoneAsync(StopReasons.Cancelled).ConfigureAwait(false);
                }
                else if (error is not null)
                {
                    turn.Logger.LogError("chat handler failed (run {RunId}): {Error}", turn.RunId, error.Message);
                    await stream.ErrorAsync(error.Message, Codes.Internal).ConfigureAwait(false);
                }
                else
                {
                    await stream.DoneAsync(StopReasons.EndTurn).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                turn.Logger.LogError("run {RunId} ended abnormally: {Error}", turn.RunId, ex.Message);
            }
            finally
            {
                turn.Token.Close();
                await Background.Close(renewSub).ConfigureAwait(false);
                EndRun(turn, run);
            }
        });

        return ack;
    }

    private async Task<byte[]> HandleInvoke(ServiceRequest request, CancellationToken ct)
    {
        var turn = await ParseTurn(request, needStream: false, ct).ConfigureAwait(false);
        var run = NewRun(turn, honorBudget: false);
        try
        {
            InvokeResponse resp;
            try
            {
                using (CallContext.WithUserContext(turn.Context))
                {
                    resp = await _invoke!(turn, run.Token).ConfigureAwait(false);
                }
            }
            catch (NatsServiceException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw NatsServiceException.Server(ex.Message, Codes.Internal, ex);
            }

            resp ??= new InvokeResponse();
            return WireJson.Serialize(resp with
            {
                RunId = string.IsNullOrEmpty(resp.RunId) ? turn.RunId : resp.RunId,
                SessionId = string.IsNullOrEmpty(resp.SessionId) ? turn.SessionId : resp.SessionId,
            });
        }
        finally
        {
            EndRun(turn, run);
        }
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;
}
