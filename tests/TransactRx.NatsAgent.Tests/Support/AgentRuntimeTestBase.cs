using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests.Support;

/// <summary>
/// The Go <c>fakeIdentity</c> of renew_test.go: answers validateInternalToken on the test
/// server. The users map is token → {userId, accountId}; other tokens are invalid.
/// <see cref="Revoke"/> makes a token invalid from then on.
/// </summary>
public sealed class RenewFakeIdentity
{
    private readonly object _gate = new();

    private readonly Dictionary<string, (string UserId, string AccountId)> _users = new()
    {
        ["IDT-alice1.c"] = ("alice", "acc1"),
        ["IDT-alice2.c"] = ("alice", "acc1"),
        ["IDT-bob.c"] = ("bob", "acc1"),
        ["IDT-aliceOther.c"] = ("alice", "acc2"),
        ["IDT-aliceCached.c"] = ("alice", "acc1"),
        ["IDT-bobNoFn.c"] = ("bob", "acc1"),
        ["IDT-aliceNoFn.c"] = ("alice", "acc1"),
    };

    // Valid tokens whose function is not granted.
    private readonly HashSet<string> _noFn = ["IDT-bobNoFn.c", "IDT-aliceNoFn.c"];

    private int _calls;

    public int CallCount
    {
        get
        {
            lock (_gate)
            {
                return _calls;
            }
        }
    }

    public void Revoke(string token)
    {
        lock (_gate)
        {
            _users.Remove(token);
        }
    }

    /// <summary>The reply to one validateInternalToken request.</summary>
    internal byte[] Answer(byte[]? request)
    {
        var idt = "";
        try
        {
            using var doc = JsonDocument.Parse(request ?? "{}"u8.ToArray());
            if (doc.RootElement.TryGetProperty("idt", out var value) && value.ValueKind == JsonValueKind.String)
            {
                idt = value.GetString()!;
            }
        }
        catch (JsonException)
        {
            // an unreadable request names no token: invalid
        }

        bool known;
        bool noFn;
        (string UserId, string AccountId) user;
        lock (_gate)
        {
            _calls++;
            known = _users.TryGetValue(idt, out user);
            noFn = _noFn.Contains(idt);
        }

        if (!known)
        {
            return JsonSerializer.SerializeToUtf8Bytes(new { valid = false, functionGranted = false, reason = "INVALID_IDT" });
        }

        return noFn
            ? JsonSerializer.SerializeToUtf8Bytes(new { valid = true, functionGranted = false, userId = user.UserId, accountId = user.AccountId, reason = "DENIED_FN" })
            : JsonSerializer.SerializeToUtf8Bytes(new { valid = true, functionGranted = true, userId = user.UserId, accountId = user.AccountId });
    }
}

/// <summary>The Go <c>testAgentOpts</c>.</summary>
public sealed record RenewAgentOptions
{
    public bool Renew { get; init; }

    public bool Observe { get; init; }

    /// <summary>Validate subject; null = IDT validation off.</summary>
    public string? Identity { get; init; }

    /// <summary>Agent name; null = derived from the test name.</summary>
    public string? Name { get; init; }

    /// <summary>Overrides the default handler (block until release or the run's token is cancelled).</summary>
    public ChatHandler? Handler { get; init; }
}

/// <summary>What the Go <c>startRenewAgent</c> returns: the agent, the turns its default handler saw, and the release.</summary>
public sealed class RenewAgent(Agent agent, string name, ChannelReader<Turn> turns, TaskCompletionSource release)
{
    public Agent Agent { get; } = agent;

    /// <summary>The Go <c>a.cfg.Name</c>.</summary>
    public string Name { get; } = name;

    /// <summary>The turns handed over by the default handler.</summary>
    public ChannelReader<Turn> Turns { get; } = turns;

    /// <summary>The Go <c>close(release)</c>: lets the default handler return.</summary>
    public void Release() => release.TrySetResult();

    public void Deconstruct(out RenewAgent agent, out ChannelReader<Turn> turns)
    {
        agent = this;
        turns = Turns;
    }
}

/// <summary>One reply as the caller's connection saw it (the Go <c>rawReply</c>).</summary>
public sealed record ReceivedReply(NatsHeaders? Headers, byte[] Data)
{
    public string Body => System.Text.Encoding.UTF8.GetString(Data);

    public int HeaderCount => Headers?.Count ?? 0;

    public override string ToString() =>
        $"header=[{(Headers is null ? "" : string.Join(", ", Headers.Select(h => h.Key + "=" + h.Value)))}] code={Headers?.Code ?? 0} body={Body}";
}

/// <summary>
/// The helpers of the Go agent package tests (renew_test.go, interrupt_test.go): a full agent
/// on the test server whose default handler hands over its turn and blocks, the fake
/// identity, and raw renew requests.
/// </summary>
public abstract class AgentRuntimeTestBase(NatsServerFixture nats) : NatsTestBase(nats)
{
    private NatsConnection? _caller;
    private MonitoredNatsServer? _monitored;

    /// <summary>The connection of the test's caller side (the Go <c>streamConn</c>).</summary>
    protected async Task<NatsConnection> CallerAsync()
    {
        if (_caller is null)
        {
            if (_monitored is null)
            {
                _caller = await ConnectAsync();
            }
            else
            {
                _caller = Track(new NatsConnection(new NatsOpts { Url = _monitored.Url }));
                await _caller.ConnectAsync();
            }
        }

        return _caller;
    }

    /// <summary>The name of the agent's connection on a <see cref="MonitoredNatsServer"/>.</summary>
    protected const string AgentConnectionName = "agentUnderTest";

    /// <summary>
    /// Moves this test (its agents and its caller) to a private server that reports each
    /// connection's subscriptions. Call it first.
    /// </summary>
    protected async Task<MonitoredNatsServer> UseMonitoredServerAsync()
    {
        Assert.Null(_caller);
        return _monitored = Track(await MonitoredNatsServer.StartAsync());
    }

    /// <summary>The Go <c>"test.renew.identity." + t.Name()</c>.</summary>
    protected static string RenewIdentitySubject(string? subtest = null, [CallerMemberName] string test = "") =>
        "test.renew.identity.Test" + test + (subtest is null ? "" : "/" + subtest.Replace(' ', '_'));

    /// <summary>The Go default agent name: <c>"renew" + t.Name()</c> with "/" replaced.</summary>
    protected static string RenewAgentName(string test, string? subtest = null) =>
        "renewTest" + test + (subtest is null ? "" : "_" + subtest.Replace(' ', '_'));

    /// <summary>The Go <c>startFakeIdentity</c> of renew_test.go.</summary>
    protected async Task<RenewFakeIdentity> StartRenewIdentityAsync(string subject)
    {
        var identity = new RenewFakeIdentity();
        var nc = await ConnectAsync();
        var sub = await nc.SubscribeCoreAsync<byte[]>(subject);
        Cleanup(sub.DisposeAsync);
        _ = Task.Run(async () =>
        {
            await foreach (var msg in sub.Msgs.ReadAllAsync())
            {
                await msg.ReplyAsync(identity.Answer(msg.Data));
            }
        });
        await nc.PingAsync();
        return identity;
    }

    /// <summary>
    /// The Go <c>startRenewAgent</c>: starts a full agent on the test server. The default
    /// handler hands its turn to <see cref="RenewAgent.Turns"/> and blocks until
    /// <see cref="RenewAgent.Release"/> (or the end of the run).
    /// </summary>
    protected async Task<RenewAgent> StartRenewAgentAsync(RenewAgentOptions options, string? subtest = null, [CallerMemberName] string test = "")
    {
        var name = options.Name ?? RenewAgentName(test, subtest);
        var config = new AgentConfig
        {
            Name = name,
            Description = "renew test",
            Renew = options.Renew,
            IdtValidation = new IdtValidation(),
        };
        if (options.Identity is not null)
        {
            config = config with
            {
                Access = new AgentAccess { AppId = "appT", FunctionId = "fnT" },
                IdtValidation = new IdtValidation
                {
                    Enabled = true,
                    ObserveOnly = options.Observe,
                    Subject = options.Identity,
                    Timeout = TimeSpan.FromSeconds(2),
                    CacheTtl = TimeSpan.FromMinutes(1),
                },
            };
        }

        if (_monitored is not null)
        {
            config = config with { Nats = new NatsSettings { Url = _monitored.Url, Name = AgentConnectionName, ExitWhenConnectionLost = false } };
        }

        var agent = await NewAgentAsync(config);
        var turns = Channel.CreateUnbounded<Turn>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Cleanup(() =>
        {
            release.TrySetResult(); // a failed test must not leave handlers parked
            return ValueTask.CompletedTask;
        });
        agent.OnChat(options.Handler ?? (async (turn, _, ct) =>
        {
            turns.Writer.TryWrite(turn);
            try
            {
                await release.Task.WaitAsync(ct);
            }
            catch (OperationCanceledException)
            {
                // the run ended first
            }
        }));
        await agent.StartAsync();
        return new RenewAgent(agent, name, turns.Reader, release);
    }

    /// <summary>The Go <c>chatAs</c>: a turn in session s1 carrying <paramref name="token"/> ("" = no token).</summary>
    protected async Task<ChatRun> ChatAsAsync(RenewAgent agent, string token)
    {
        var client = new AgentClient(await CallerAsync(), Quiet);
        using (CallContext.WithIdt(token))
        {
            return await client.ChatAsync(agent.Name, UserMessage("hi", sessionId: "s1"));
        }
    }

    /// <summary>The Go <c>waitTurn</c> / <c>waitStarted</c>: the next item, or a failure after 5s.</summary>
    protected static async Task<T> WaitAsync<T>(ChannelReader<T> reader, string because = "handler never ran")
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            return await reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            Assert.Fail(because);
            throw;
        }
    }

    /// <summary>The Go <c>renew</c>: one raw request to the run's renew subject, decoded.</summary>
    protected async Task<RenewResponse> RenewAsync(ChatRun run, RenewRequest request)
    {
        Assert.False(string.IsNullOrEmpty(run.Ack.Caps?.RenewSubject), "ack has no renewSubject");
        var nc = await CallerAsync();
        var reply = await nc.RequestAsync<byte[], byte[]>(
            run.Ack.Caps!.RenewSubject!, WireJson.Serialize(request),
            replyOpts: new NatsSubOpts { Timeout = TimeSpan.FromSeconds(3) });
        var response = WireJson.Deserialize<RenewResponse>(reply.Data ?? []);
        Assert.NotNull(response);
        return response;
    }

    /// <summary>The Go <c>terminal</c>: the last event of the run.</summary>
    protected static async Task<AgentEvent> TerminalAsync(ChatRun run)
    {
        var events = await DrainAsync(run);
        Assert.NotEmpty(events);
        return events[^1];
    }

    /// <summary>
    /// The Go <c>allReplies</c>: sends one request to <paramref name="subject"/> and returns
    /// every reply that arrives within <paramref name="wait"/> — not only the first one a
    /// request call would return.
    /// </summary>
    protected async Task<List<ReceivedReply>> AllRepliesAsync(string subject, byte[] body, TimeSpan wait)
    {
        var nc = await CallerAsync();
        var inbox = nc.NewInbox();
        var got = new List<ReceivedReply>();
        await using var sub = await nc.SubscribeCoreAsync<byte[]>(inbox);
        var pump = Task.Run(async () =>
        {
            await foreach (var msg in sub.Msgs.ReadAllAsync())
            {
                lock (got)
                {
                    got.Add(new ReceivedReply(msg.Headers, msg.Data ?? []));
                }
            }
        });
        await nc.PublishAsync(subject, body, replyTo: inbox);
        await Task.Delay(wait);
        await sub.UnsubscribeAsync();
        await pump.WaitAsync(TimeSpan.FromSeconds(5));
        lock (got)
        {
            return [.. got];
        }
    }
}
