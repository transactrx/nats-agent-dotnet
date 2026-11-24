using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Tools;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests.Support;

/// <summary>
/// Base of the tests that talk to the shared nats-server. Everything created through it
/// (connections, agents, hosts, subscriptions) is closed when the test class is disposed —
/// the counterpart of the Go suite's <c>t.Cleanup</c>.
/// </summary>
public abstract class NatsTestBase(NatsServerFixture nats) : IAsyncLifetime
{
    private readonly ConcurrentStack<Func<ValueTask>> _cleanup = new();

    protected static readonly ILoggerFactory Quiet = NullLoggerFactory.Instance;

    protected static readonly TimeSpan Second = TimeSpan.FromSeconds(1);

    protected NatsServerFixture Nats { get; } = nats;

    protected string Url => Nats.Url;

    public virtual ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public virtual async ValueTask DisposeAsync()
    {
        while (_cleanup.TryPop(out var action))
        {
            try
            {
                await action();
            }
            catch (Exception)
            {
                // best effort
            }
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>Runs <paramref name="action"/> when the test class is disposed (last registered first).</summary>
    protected void Cleanup(Func<ValueTask> action) => _cleanup.Push(action);

    protected T Track<T>(T disposable)
        where T : IAsyncDisposable
    {
        Cleanup(disposable.DisposeAsync);
        return disposable;
    }

    /// <summary>A fresh client connection (the Go <c>testConn</c>).</summary>
    protected async Task<NatsConnection> ConnectAsync() => Track(await Nats.ConnectAsync());

    protected async Task<AgentClient> AgentClientAsync() => new(await ConnectAsync(), Quiet);

    protected async Task<ToolClient> ToolClientAsync() => new(await ConnectAsync(), Quiet);

    /// <summary>NATS settings pointing at the test server.</summary>
    protected NatsSettings Settings => new() { Url = Url, ExitWhenConnectionLost = false };

    /// <summary>An agent on its own connection, disposed with the test.</summary>
    protected async Task<Agent> NewAgentAsync(AgentConfig config)
    {
        // Validation off unless the test says otherwise: the developer's environment must not leak in.
        config = config with
        {
            Nats = config.Nats ?? Settings,
            LoggerFactory = config.LoggerFactory ?? Quiet,
            IdtValidation = config.IdtValidation ?? new IdtValidation(),
        };
        return Track(await Agent.CreateAsync(config));
    }

    protected ToolHost NewHost() => Track(new ToolHost(Settings, Quiet));

    /// <summary>The Go <c>startTestAgent</c>: agent "testAgent" (or <paramref name="name"/>) with sessions and an echoing chat handler.</summary>
    protected async Task<(Agent Agent, MemSessionStore Store)> StartTestAgentAsync(string name = "testAgent")
    {
        var store = new MemSessionStore();
        var agent = await NewAgentAsync(new AgentConfig
        {
            Name = name,
            DisplayName = "Test Agent",
            Description = "Agent used by the nats-agent e2e tests.",
            Version = "0.1.0",
            Tags = ["test"],
            Skills = [new Skill { Name = "echoing", Description = "echoes things" }],
        });
        agent.UseSessionStore(store);
        agent.OnChat(async (turn, stream, ct) =>
        {
            var text = turn.Message.Content.Count > 0 ? turn.Message.Content[0].Text ?? "" : "";
            if (text == "block")
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct); // wait for cancellation
            }

            await stream.StatusAsync("thinking");
            await stream.ToolUseAsync("tu_1", "echo", new Dictionary<string, object> { ["say"] = text });
            await stream.ToolResultAsync("tu_1", "echo", "{\"say\":\"" + text + "\"}");
            await stream.TextAsync("you said: ");
            await stream.TextAsync(text);
            await stream.DoneAsync(StopReasons.EndTurn, new Usage { InputTokens = 1, OutputTokens = 2, TotalTokens = 3 });
        });
        await agent.StartAsync();
        return (agent, store);
    }

    /// <summary>
    /// The Go <c>startFakeIdentity</c>: answers validateInternalToken on the test server.
    /// <paramref name="grant"/> maps idt → userId (for appTest/fnTest); tokens not in the map
    /// are DENIED_FN; "revoked.x" is TOKEN_REVOKED. Returns the call counter.
    /// </summary>
    protected async Task<Counter> StartFakeIdentityAsync(string subject, IReadOnlyDictionary<string, string> grant)
    {
        var nc = await ConnectAsync();
        var calls = new Counter();
        var sub = await nc.SubscribeCoreAsync<byte[]>(subject);
        Cleanup(sub.DisposeAsync);
        _ = Task.Run(async () =>
        {
            await foreach (var msg in sub.Msgs.ReadAllAsync())
            {
                calls.Increment();
                using var doc = JsonDocument.Parse(msg.Data ?? "{}"u8.ToArray());
                var idt = Str(doc, "idt");
                object reply = new { valid = true, functionGranted = false, reason = "DENIED_FN" };
                if (idt == "revoked.x")
                {
                    reply = new { valid = false, functionGranted = false, reason = "TOKEN_REVOKED" };
                }
                else if (grant.TryGetValue(idt, out var user) && Str(doc, "agentId") == "appTest" && Str(doc, "functionId") == "fnTest")
                {
                    reply = new { valid = true, functionGranted = true, userId = user, accountId = "acc1", reason = (string?)null };
                }

                await msg.ReplyAsync(JsonSerializer.SerializeToUtf8Bytes(reply));
            }
        });
        await nc.PingAsync();
        return calls;

        static string Str(JsonDocument doc, string name) =>
            doc.RootElement.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString()! : "";
    }

    /// <summary>Reads every event of a run up to and including the terminal one.</summary>
    protected static async Task<List<AgentEvent>> DrainAsync(ChatRun run, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(20));
        var events = new List<AgentEvent>();
        await foreach (var ev in run.Events.ReadAllAsync(cts.Token))
        {
            events.Add(ev);
        }

        return events;
    }

    protected static ChatRequest UserMessage(string text, string? sessionId = null, string? userId = null) =>
        new() { SessionId = sessionId, UserId = userId, Message = Message.UserText(text) };

    /// <summary>The Go <c>assertForbidden</c>: a 403 / 4031 envelope with the given reason.</summary>
    protected static async Task AssertForbiddenAsync(Func<Task> call, string reason)
    {
        var ex = await Assert.ThrowsAsync<NatsServiceException>(call);
        Assert.Equal((403, Codes.Forbidden, reason), (ex.Status, ex.ApiStatusCode, ex.ErrorMessage));
    }

    /// <summary>Polls until <paramref name="condition"/> holds; fails after <paramref name="timeout"/>.</summary>
    protected static async Task EventuallyAsync(Func<bool> condition, string because, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, because);
            await Task.Delay(10);
        }
    }
}

public sealed class Counter
{
    private int _value;

    public int Value => Volatile.Read(ref _value);

    public void Increment() => Interlocked.Increment(ref _value);
}

/// <summary>The Go <c>memSessionStore</c>.</summary>
public sealed class MemSessionStore : ISessionStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, SessionGetResponse> _sessions = [];

    private static string Key(string userId, string sessionId) => userId + "/" + sessionId;

    public void Put(string userId, SessionGetResponse session)
    {
        lock (_gate)
        {
            _sessions[Key(userId, session.SessionId)] = session;
        }
    }

    public Task<IReadOnlyList<SessionMeta>> ListAsync(string userId, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            IReadOnlyList<SessionMeta> result = [.. _sessions
                .Where(kv => kv.Key.StartsWith(userId + "/", StringComparison.Ordinal))
                .Select(kv => new SessionMeta { SessionId = kv.Value.SessionId, Title = kv.Value.Title, Favorite = kv.Value.Favorite })];
            return Task.FromResult(result);
        }
    }

    public Task<SessionGetResponse> GetAsync(string userId, string sessionId, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return _sessions.TryGetValue(Key(userId, sessionId), out var session)
                ? Task.FromResult(session)
                : throw new SessionNotFoundException();
        }
    }

    public Task DeleteAsync(string userId, string sessionId, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return _sessions.Remove(Key(userId, sessionId)) ? Task.CompletedTask : throw new SessionNotFoundException();
        }
    }

    public Task RenameAsync(string userId, string sessionId, string title, CancellationToken cancellationToken) =>
        Update(userId, sessionId, s => s with { Title = title });

    public Task SetFavoriteAsync(string userId, string sessionId, bool favorite, CancellationToken cancellationToken) =>
        Update(userId, sessionId, s => s with { Favorite = favorite });

    private Task Update(string userId, string sessionId, Func<SessionGetResponse, SessionGetResponse> change)
    {
        lock (_gate)
        {
            if (!_sessions.TryGetValue(Key(userId, sessionId), out var session))
            {
                throw new SessionNotFoundException();
            }

            _sessions[Key(userId, sessionId)] = change(session);
            return Task.CompletedTask;
        }
    }
}

/// <summary>The Go <c>echoTool</c>: echoes the input back as JSON.</summary>
public class EchoTool : ITool
{
    public virtual string Name => "echo";

    public string Description => "Echo the input back as JSON. For tests.";

    public JsonElement InputSchema { get; } = WireJson.Parse("""{"type":"object","properties":{"say":{"type":"string"}}}""");

    public Task<string> RunAsync(JsonElement input, CancellationToken cancellationToken) => Task.FromResult(input.GetRawText());
}

/// <summary>The Go <c>renamedEcho</c>.</summary>
public sealed class RenamedEcho(string name) : EchoTool
{
    public override string Name { get; } = name;
}

/// <summary>The Go <c>failTool</c>: always fails.</summary>
public sealed class FailTool(string name = "alwaysfail") : ITool
{
    public string Name { get; } = name;

    public string Description => "Always fails. For tests.";

    public JsonElement InputSchema { get; } = WireJson.Parse("""{"type":"object"}""");

    public Task<string> RunAsync(JsonElement input, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("intentional failure");
}
