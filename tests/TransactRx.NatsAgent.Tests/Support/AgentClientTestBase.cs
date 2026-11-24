using System.Collections.Concurrent;
using System.Threading.Channels;
using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests.Support;

/// <summary>Publishes one event on the fake agent's stream subject (the run id is filled in).</summary>
public delegate ValueTask PublishEvent(AgentEvent ev);

/// <summary>
/// The Go <c>fakeAgent</c> of agentclient/client_test.go: answers chat on a unique agent name
/// with raw NATS subscriptions. Its script runs after the ack and publishes events on the
/// stream subject. Start one with <see cref="AgentClientTestBase.StartFakeAgentAsync"/>.
/// </summary>
public sealed class FakeAgent
{
    /// <summary>The run id of every run the fake acknowledges.</summary>
    public const string RunId = "run-1";

    private readonly object _gate = new();
    private readonly List<string> _idts = [];
    private readonly Channel<string> _cancels = Channel.CreateUnbounded<string>();
    private readonly Channel<string> _reasons = Channel.CreateUnbounded<string>();

    internal FakeAgent(string name) => Name = name;

    public string Name { get; }

    /// <summary>The run id of each cancel request received.</summary>
    public ChannelReader<string> Cancels => _cancels.Reader;

    /// <summary>The reason of each cancel ("" when none), same order as <see cref="Cancels"/>.</summary>
    public ChannelReader<string> Reasons => _reasons.Reader;

    /// <summary>The X-TRX-IDT header of each chat request ("" when absent), in arrival order.</summary>
    public IReadOnlyList<string> SeenIdts
    {
        get
        {
            lock (_gate)
            {
                return [.. _idts];
            }
        }
    }

    internal void RecordIdt(string idt)
    {
        lock (_gate)
        {
            _idts.Add(idt);
        }
    }

    internal void RecordCancel(CancelRequest request)
    {
        _reasons.Writer.TryWrite(request.Reason ?? "");
        _cancels.Writer.TryWrite(request.RunId);
    }
}

/// <summary>One raw reply of <see cref="AgentClientTestBase.RawRenewResponderAsync"/> (headers may be null).</summary>
public sealed record RawReply(string Data = "", IReadOnlyDictionary<string, string>? Headers = null);

/// <summary>
/// Helpers of the Go agentclient tests (client_test.go): fake agents and renew responders
/// built on raw NATS subscriptions. A failure inside one of their handlers (Go's
/// <c>t.Errorf</c> from a callback) fails the test when the class is disposed.
/// </summary>
public abstract class AgentClientTestBase(NatsServerFixture nats) : NatsTestBase(nats)
{
    private static int _sequence;

    private readonly ConcurrentQueue<string> _failures = new();

    /// <summary>The ack caps of an agent that declares progress (needed for the client's idle cut).</summary>
    protected static readonly AckCaps ProgressCaps = new() { Progress = true };

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        Assert.True(_failures.IsEmpty, "helper failures: " + string.Join("; ", _failures));
    }

    /// <summary>The next value of the sequence that makes fake agent and tool names unique.</summary>
    protected static int NextSequence() => Interlocked.Increment(ref _sequence);

    /// <summary>The Go <c>chatReq</c>: a user message "hi".</summary>
    protected static ChatRequest ChatReq() => new() { Message = Message.UserText("hi") };

    /// <summary>Reads one header value of a raw message, "" when absent.</summary>
    protected static string Header(NatsHeaders? headers, string name) =>
        headers is not null && headers.TryGetValue(name, out var values) && values.Count > 0 ? values[0] ?? "" : "";

    /// <summary>The Go <c>respondOK</c>: replies the way a nats-service endpoint does on success.</summary>
    protected static ValueTask RespondOkAsync<T>(NatsMsg<byte[]> request, T body) =>
        request.ReplyAsync(WireJson.Serialize(body), new NatsHeaders { ["status"] = "200" });

    /// <summary>
    /// Subscribes <paramref name="subject"/> on <paramref name="nc"/> and hands every message
    /// to <paramref name="handle"/> until the test ends. Returns once the server knows the
    /// subscription (the Go <c>nc.Flush()</c>).
    /// </summary>
    protected async Task SubscribeAsync(NatsConnection nc, string subject, Func<NatsMsg<byte[]>, ValueTask> handle)
    {
        var sub = await nc.SubscribeCoreAsync<byte[]>(subject);
        Cleanup(sub.DisposeAsync);
        _ = Task.Run(async () =>
        {
            await foreach (var msg in sub.Msgs.ReadAllAsync())
            {
                try
                {
                    await handle(msg);
                }
                catch (Exception ex)
                {
                    _failures.Enqueue($"handler of {subject}: {ex.Message}");
                }
            }
        });
        await nc.PingAsync();
    }

    /// <summary>
    /// The Go <c>startFakeAgent</c>. <paramref name="caps"/> goes into every ack;
    /// <paramref name="script"/> (may be null) runs in the background after each ack.
    /// </summary>
    protected async Task<FakeAgent> StartFakeAgentAsync(AckCaps? caps, Func<PublishEvent, Task>? script)
    {
        var nc = await ConnectAsync();
        var fake = new FakeAgent($"fake{NextSequence()}");

        await SubscribeAsync(nc, Protocol.AgentSubject(fake.Name, "chat"), async msg =>
        {
            if (!WireJson.TryDeserialize<ChatRequest>(msg.Data ?? [], out var request))
            {
                _failures.Enqueue("chat request: not a ChatRequest");
                return;
            }

            fake.RecordIdt(Header(msg.Headers, Protocol.HeaderIdt));
            await RespondOkAsync(msg, new ChatAck
            {
                Accepted = true,
                RunId = FakeAgent.RunId,
                Agent = fake.Name,
                StreamSubject = request.StreamSubject,
                Caps = caps,
            });
            if (script is null)
            {
                return;
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    await script(async ev =>
                    {
                        try
                        {
                            await nc.PublishAsync(request.StreamSubject, WireJson.Serialize(ev with { RunId = FakeAgent.RunId }));
                        }
                        catch (Exception)
                        {
                            // best effort, as in Go: the connection may already be closed
                        }
                    });
                }
                catch (OperationCanceledException)
                {
                    // the test ended while the script was waiting
                }
                catch (Exception ex)
                {
                    _failures.Enqueue($"script of {fake.Name}: {ex.Message}");
                }
            });
        });

        await SubscribeAsync(nc, Protocol.AgentSubject(fake.Name, "cancel"), msg =>
        {
            WireJson.TryDeserialize<CancelRequest>(msg.Data ?? [], out var request);
            fake.RecordCancel(request ?? new CancelRequest());
            return ValueTask.CompletedTask;
        });

        return fake;
    }

    /// <summary>
    /// A signal for <see cref="PingsUntilAsync"/> that is also set when the test ends (the Go
    /// <c>stop := make(chan struct{}); defer close(stop)</c>).
    /// </summary>
    protected CancellationTokenSource StopSignal()
    {
        var stop = new CancellationTokenSource();
        Cleanup(() =>
        {
            stop.Cancel();
            stop.Dispose();
            return ValueTask.CompletedTask;
        });
        return stop;
    }

    /// <summary>The Go <c>pingsUntil</c>: publishes pings every 20ms until <paramref name="stop"/> is set.</summary>
    protected static async Task PingsUntilAsync(CancellationToken stop, PublishEvent publish)
    {
        while (true)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(20), stop);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            await publish(new AgentEvent { Type = EventTypes.Ping });
        }
    }

    /// <summary>
    /// The Go <c>drain</c>: every event of the run through the terminal one; fails when the run
    /// does not end within <paramref name="within"/>.
    /// </summary>
    protected static async Task<List<AgentEvent>> DrainWithinAsync(ChatRun run, TimeSpan within)
    {
        var events = new List<AgentEvent>();
        using var deadline = new CancellationTokenSource(within);
        try
        {
            await foreach (var ev in run.Events.ReadAllAsync(deadline.Token))
            {
                events.Add(ev);
            }
        }
        catch (OperationCanceledException)
        {
            Assert.Fail($"run did not end within {within}; got {Describe(events)}");
        }

        return events;
    }

    /// <summary>Receives the next item; fails with <paramref name="because"/> when none arrives in time.</summary>
    protected static async Task<T> ReceiveAsync<T>(ChannelReader<T> reader, TimeSpan within, string because)
    {
        using var deadline = new CancellationTokenSource(within);
        try
        {
            return await reader.ReadAsync(deadline.Token);
        }
        catch (OperationCanceledException)
        {
            Assert.Fail(because);
            throw;
        }
    }

    /// <summary>Fails with <paramref name="because"/> (given the item) when anything arrives within <paramref name="within"/>.</summary>
    protected static async Task AssertNothingAsync<T>(ChannelReader<T> reader, TimeSpan within, Func<T, string> because)
    {
        using var deadline = new CancellationTokenSource(within);
        T item;
        try
        {
            item = await reader.ReadAsync(deadline.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        Assert.Fail(because(item));
    }

    /// <summary>The Go <c>renewResponder</c>: answers RenewRequests on <paramref name="subject"/> with <paramref name="response"/> and records them.</summary>
    protected async Task<ChannelReader<RenewRequest>> RenewResponderAsync(string subject, RenewResponse response)
    {
        var nc = await ConnectAsync();
        var got = Channel.CreateUnbounded<RenewRequest>();
        await SubscribeAsync(nc, subject, async msg =>
        {
            WireJson.TryDeserialize<RenewRequest>(msg.Data ?? [], out var request);
            got.Writer.TryWrite(request ?? new RenewRequest());
            await msg.ReplyAsync(WireJson.Serialize(response));
        });
        return got.Reader;
    }

    /// <summary>
    /// The Go <c>rawRenewResponder</c>: answers every request on <paramref name="subject"/>
    /// with each of <paramref name="replies"/>, in order.
    /// </summary>
    protected async Task RawRenewResponderAsync(string subject, params RawReply[] replies)
    {
        var nc = await ConnectAsync();
        await SubscribeAsync(nc, subject, async msg =>
        {
            foreach (var reply in replies)
            {
                NatsHeaders? headers = null;
                if (reply.Headers is not null)
                {
                    headers = new NatsHeaders();
                    foreach (var (name, value) in reply.Headers)
                    {
                        headers[name] = value;
                    }
                }

                await nc.PublishAsync(msg.ReplyTo!, System.Text.Encoding.UTF8.GetBytes(reply.Data), headers);
            }
        });
    }

    /// <summary>
    /// The Go <c>notFoundEnvelope</c>: what an agent instance without the renew.&gt; endpoint
    /// (an older build in the same queue group) answers on a renew subject.
    /// </summary>
    protected static RawReply NotFoundEnvelope(string subject) => new(
        "{\"status\":404,\"errorMessage\":\"subject: " + subject + " not found\",\"apiStatusCode\":404,\"internalError\":\"\"}",
        new Dictionary<string, string> { ["status"] = "404" });

    /// <summary>A run that was not started through chat (the Go tests build <c>&amp;Run{…}</c> directly).</summary>
    protected static ChatRun DetachedRun(AgentClient client, string agent, string runId, string? renewSubject = null) =>
        new(client, agent, new ChatAck { RunId = runId, Caps = renewSubject is null ? null : new AckCaps { RenewSubject = renewSubject } },
            Channel.CreateUnbounded<AgentEvent>().Reader);

    protected static string Describe(IEnumerable<AgentEvent> events) =>
        "[" + string.Join(", ", events.Select(WireJson.SerializeToString)) + "]";
}
