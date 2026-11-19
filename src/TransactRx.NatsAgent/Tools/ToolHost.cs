using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tools;

/// <summary>
/// The server side of network agentic tools (SPEC.md §8): one deployable process serving any
/// number of tools. Each hosted tool is indistinguishable on the wire from a tool running
/// alone: its own <c>trx.tool.&lt;name&gt;.&gt;</c> subject space, card, queue group and
/// discovery reply.
/// </summary>
public sealed partial class ToolHost : IAsyncDisposable
{
    private const int DefaultTimeoutSeconds = 30;
    private static readonly HashSet<string> ReservedNames = ["discover", "announce"];

    private readonly NatsSettings? _settings;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly Dictionary<string, HostedTool> _tools = [];
    private NatsConnection? _nc;
    private bool _ownsConnection;
    private bool _started;
    private IdtValidation _validation = new();
    private int _disposed;

    /// <summary>
    /// A host that connects on <see cref="StartAsync"/> with explicit settings (unset values
    /// fall back to <c>NATS_URL</c>, <c>NATS_JWT</c>, <c>NATS_KEY</c>). Token validation is
    /// off until <see cref="SetIdtValidation"/>.
    /// </summary>
    public ToolHost(NatsSettings? settings = null, ILoggerFactory? loggerFactory = null)
    {
        _settings = settings;
        _loggerFactory = loggerFactory ?? StdoutLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger("TransactRx.NatsAgent.Tools");
    }

    /// <summary>
    /// A host serving on an existing connection; the caller keeps ownership. Token validation
    /// is off until <see cref="SetIdtValidation"/>.
    /// </summary>
    public ToolHost(NatsConnection connection, ILoggerFactory? loggerFactory = null)
        : this((NatsSettings?)null, loggerFactory)
    {
        _nc = connection;
    }

    [GeneratedRegex("^[a-zA-Z0-9_-]+$")]
    private static partial Regex NameRegex();

    /// <summary>
    /// A host configured from the org environment: the NATS conventions (<c>NATS_URL</c>,
    /// <c>NATS_JWT</c>, <c>NATS_KEY</c>) and the IDT validation contract
    /// (<see cref="IdtValidation.FromEnvironment(ILogger?)"/>).
    /// </summary>
    /// <exception cref="InvalidOperationException"><c>NATS_URL</c> is not set.</exception>
    public static ToolHost FromEnvironment(ILoggerFactory? loggerFactory = null)
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("NATS_URL")))
        {
            throw new InvalidOperationException("NATS_URL is not set");
        }

        var host = new ToolHost((NatsSettings?)null, loggerFactory);
        host._validation = IdtValidation.FromEnvironment(host._logger);
        return host;
    }

    /// <summary>The cards of all registered tools.</summary>
    public IReadOnlyList<ToolCard> Cards => [.. _tools.Values.Select(t => t.Card)];

    /// <summary>
    /// Configures inbound token checks on every tool's run endpoint (SPEC §8.4). Must be
    /// called before <see cref="StartAsync"/>.
    /// </summary>
    public void SetIdtValidation(IdtValidation validation)
    {
        if (_started)
        {
            throw new InvalidOperationException("host already started");
        }

        _validation = validation;
    }

    /// <summary>Adds a tool to the host. Must be called before <see cref="StartAsync"/>.</summary>
    /// <exception cref="ArgumentException">The tool's name or info is invalid, or the name is taken.</exception>
    public void Register(ITool tool, ToolInfo? info = null)
    {
        if (_started)
        {
            throw new InvalidOperationException("host already started");
        }

        var name = tool.Name;
        if (name is null || !NameRegex().IsMatch(name))
        {
            throw new ArgumentException($"invalid tool name \"{name}\": must match ^[a-zA-Z0-9_-]+$");
        }

        if (ReservedNames.Contains(name))
        {
            throw new ArgumentException($"tool name \"{name}\" is reserved");
        }

        if (_tools.ContainsKey(name))
        {
            throw new ArgumentException($"tool \"{name}\" registered twice");
        }

        var i = info ?? new ToolInfo();
        if (i.TimeoutSeconds <= 0)
        {
            i = i with { TimeoutSeconds = DefaultTimeoutSeconds };
        }

        if (i.Access is not null && (string.IsNullOrEmpty(i.Access.AppId) || string.IsNullOrEmpty(i.Access.FunctionId)))
        {
            throw new ArgumentException($"tool \"{name}\": Access requires both AppId and FunctionId");
        }

        if (i.MaxConcurrent < 0)
        {
            throw new ArgumentException($"tool \"{name}\": MaxConcurrent must not be negative");
        }

        _tools[name] = new HostedTool(tool, i, new ToolCard
        {
            ProtocolVersion = Protocol.Version,
            Kind = Protocol.KindTool,
            Name = name,
            DisplayName = NullIfEmpty(i.DisplayName),
            Description = tool.Description,
            Version = NullIfEmpty(i.Version),
            RepositoryUrl = NullIfEmpty(i.RepositoryUrl),
            Tags = i.Tags is { Count: > 0 } ? i.Tags : null,
            InputSchema = tool.InputSchema,
            TimeoutSeconds = i.TimeoutSeconds,
            Access = i.Access,
            Metadata = i.Metadata is { Count: > 0 } ? i.Metadata : null,
        });
    }

    /// <summary>
    /// Brings every registered tool onto the wire: its own subject space and queue group,
    /// discovery reply, and a startup announcement.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_tools.Count == 0)
        {
            throw new InvalidOperationException("no tools registered");
        }

        if (_validation.Enabled)
        {
            foreach (var (name, hosted) in _tools)
            {
                if (hosted.Card.Access is null)
                {
                    throw new InvalidOperationException($"tool \"{name}\": IDT validation requires Info.Access (appId + functionId)");
                }
            }
        }

        _started = true;
        if (_nc is null)
        {
            _nc = await NatsConnector.ConnectAsync(_settings, "unknownDotnetServiceConnection", _loggerFactory, cancellationToken).ConfigureAwait(false);
            _ownsConnection = true;
        }

        foreach (var (name, hosted) in _tools)
        {
            var svc = new NatsService(_nc, Protocol.ToolPrefix + "." + name, "tool." + name, NatsService.NewInstanceId(), _logger)
            {
                Description = hosted.Card.Description,
                RepositoryUrl = hosted.Card.RepositoryUrl,
            };
            hosted.Start(svc, new IdtValidator(_nc, hosted.Card.Access, _validation, _logger));
            if (_validation.Enabled)
            {
                _logger.LogInformation(
                    "tool \"{Name}\": IDT validation enabled (subject={Subject} appId={AppId} functionId={FunctionId} observeOnly={ObserveOnly} failOpen={FailOpen})",
                    name, hosted.Validator!.Config.Subject, hosted.Card.Access!.AppId, hosted.Card.Access.FunctionId, _validation.ObserveOnly, _validation.FailOpen);
            }

            svc.AddEndpoints(
            [
                new EndpointRegistration
                {
                    Path = "card",
                    Description = "Tool card: model-facing description and input schema (agent protocol v" + Protocol.Version + ").",
                    Response = new ResponseDoc { Description = "ToolCard", ContentType = "application/json" },
                    Handler = (_, _) => Task.FromResult(WireJson.Serialize(hosted.Card)),
                },
                new EndpointRegistration
                {
                    Path = "ping",
                    Description = "Liveness probe.",
                    Response = new ResponseDoc { Description = "PingResponse", ContentType = "application/json" },
                    Handler = (_, _) => Task.FromResult(WireJson.Serialize(hosted.Ping())),
                },
                new EndpointRegistration
                {
                    Path = "run",
                    Description = "Execute the tool. Request: {toolUseId?, input, userId?, sessionId?, agent?, metadata?}; header X-TRX-IDT when the card declares access. Execution errors return status:\"error\" in a 200 reply (model-visible data).",
                    Response = new ResponseDoc { Description = "ToolRunResponse", ContentType = "application/json" },
                    Handler = hosted.HandleRun,
                },
            ]);

            hosted.DiscoverSub = await _nc.SubscribeCoreAsync(Protocol.ToolDiscoverSubject, "tool." + name, Framing.Raw, cancellationToken: cancellationToken).ConfigureAwait(false);
            _ = Background.Pump(hosted.DiscoverSub, _logger, hosted.HandleDiscover);

            await svc.StartAsync(cancellationToken).ConfigureAwait(false);

            // Every subscription is registered with the server before the tool announces itself.
            await _nc.PingAsync(cancellationToken).ConfigureAwait(false);

            // Startup announcement so agents pick the tool up immediately.
            await _nc.PublishAsync(Protocol.ToolAnnounceSubject, WireJson.Serialize(hosted.Card), serializer: Framing.Raw, cancellationToken: cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("tool \"{Name}\" serving on {Prefix}.>", name, Protocol.ToolPrefix + "." + name);
        }
    }

    /// <summary>
    /// Stops every tool from taking new runs (the queue group routes them to other hosts) and
    /// waits until in-flight runs finish or <paramref name="cancellationToken"/> is cancelled.
    /// Call <see cref="ShutdownAsync"/> afterwards. Returns the runs still active when it gave
    /// up.
    /// </summary>
    public async Task<long> DrainAsync(CancellationToken cancellationToken = default)
    {
        foreach (var hosted in _tools.Values)
        {
            await hosted.StopAcceptingAsync().ConfigureAwait(false);
        }

        while (true)
        {
            var active = _tools.Values.Sum(t => t.InFlight);
            if (active == 0)
            {
                return 0;
            }

            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return _tools.Values.Sum(t => t.InFlight);
            }
        }
    }

    /// <summary>Ends all tool subscriptions.</summary>
    public async Task ShutdownAsync()
    {
        foreach (var hosted in _tools.Values)
        {
            await hosted.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Shuts down and closes the connection if the host opened it.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await ShutdownAsync().ConfigureAwait(false);
        if (_ownsConnection && _nc is not null)
        {
            await _nc.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;

    private sealed class HostedTool(ITool tool, ToolInfo info, ToolCard card)
    {
        private readonly SemaphoreSlim? _slots = info.MaxConcurrent > 0 ? new SemaphoreSlim(info.MaxConcurrent, info.MaxConcurrent) : null;
        private NatsService? _svc;
        private long _startTimestamp;
        private long _inFlight;

        public ToolCard Card { get; } = card;

        public IdtValidator? Validator { get; private set; }

        public INatsSub<byte[]>? DiscoverSub { get; set; }

        public long InFlight => Interlocked.Read(ref _inFlight);

        public void Start(NatsService svc, IdtValidator validator)
        {
            _svc = svc;
            Validator = validator;
            _startTimestamp = Stopwatch.GetTimestamp();
        }

        public PingResponse Ping() => new()
        {
            Status = "ok",
            Name = Card.Name,
            InstanceId = _svc!.InstanceId,
            Version = Card.Version,
            UptimeSeconds = (long)Stopwatch.GetElapsedTime(_startTimestamp).TotalSeconds,
        };

        public async Task StopAcceptingAsync()
        {
            await Background.Close(DiscoverSub).ConfigureAwait(false);
            DiscoverSub = null;
            if (_svc is not null)
            {
                await _svc.StopAcceptingAsync().ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await StopAcceptingAsync().ConfigureAwait(false);
            if (_svc is not null)
            {
                await _svc.DisposeAsync().ConfigureAwait(false);
            }
        }

        public ValueTask HandleDiscover(NatsMsg<byte[]> msg)
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
                    return ValueTask.CompletedTask;
                }
            }

            return Card.MatchesFilter(filter)
                ? msg.ReplyAsync(WireJson.Serialize(Card), serializer: Framing.Raw)
                : ValueTask.CompletedTask;
        }

        public async Task<byte[]> HandleRun(ServiceRequest request, CancellationToken ct)
        {
            Interlocked.Increment(ref _inFlight);
            try
            {
                ToolRunRequest req;
                try
                {
                    req = WireJson.Deserialize<ToolRunRequest>(request.Body) ?? new ToolRunRequest();
                }
                catch (JsonException ex)
                {
                    throw NatsServiceException.Validation("invalid request body", Codes.InvalidBody, ex);
                }

                var input = req.Input is { ValueKind: not (JsonValueKind.Null or JsonValueKind.Undefined) } given ? given : WireJson.EmptyObject;
                if (input.ValueKind != JsonValueKind.Object)
                {
                    throw NatsServiceException.Validation("invalid request body", Codes.InvalidBody);
                }

                // Authenticate before doing any work (SPEC §8.4). The decision cache is
                // partitioned by session, like an agent's.
                var id = await Validator!.AuthorizeAsync(request.Header(Protocol.HeaderIdt), req.SessionId, ct).ConfigureAwait(false);
                var userId = id.Verified ? id.UserId : req.UserId;

                if (_slots is not null && !_slots.Wait(0, CancellationToken.None))
                {
                    throw new NatsServiceException(new NatsServiceError { Status = 429, ErrorMessage = "tool is at capacity", ApiStatusCode = Codes.Busy });
                }

                try
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(Card.TimeoutSeconds));
                    var started = Stopwatch.GetTimestamp();
                    IReadOnlyList<ToolResultContent>? content = null;
                    string? error = null;
                    try
                    {
                        // The tool's onward authenticated calls forward the caller's token.
                        using (CallContext.WithIdt(id.Idt))
                        {
                            if (tool is ICallTool callTool)
                            {
                                content = await callTool.RunCallAsync(
                                    new ToolCall
                                    {
                                        ToolUseId = req.ToolUseId,
                                        SessionId = req.SessionId,
                                        Agent = req.Agent,
                                        Input = input,
                                        Metadata = req.Metadata,
                                        Identity = id,
                                        UserId = userId,
                                    },
                                    timeout.Token).ConfigureAwait(false);
                            }
                            else
                            {
                                content = [new ToolResultContent { Text = await tool.RunAsync(input, timeout.Token).ConfigureAwait(false) }];
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Model-visible failure, not a protocol error.
                        error = ex.Message;
                        request.Logger.LogWarning("tool {Name} run failed (user={User} agent={Agent}): {Error}", Card.Name, userId, req.Agent, ex.Message);
                    }

                    return WireJson.Serialize(new ToolRunResponse
                    {
                        ToolUseId = req.ToolUseId,
                        LatencyMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds,
                        Status = error is null ? ToolStatuses.Success : ToolStatuses.Error,
                        Content = error is null ? content : null,
                        Error = error,
                    });
                }
                finally
                {
                    _slots?.Release();
                }
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }
    }
}
