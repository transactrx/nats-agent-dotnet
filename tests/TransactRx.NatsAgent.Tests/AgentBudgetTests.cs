using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/agent/budget_test.go: budgets and deadlines, ack caps, the progress throttle
/// and terminal frames with a reason.
/// </summary>
public sealed class AgentBudgetTests(NatsServerFixture nats) : NatsTestBase(nats)
{
    /// <summary>
    /// The Go <c>collect</c>: subscribes to <paramref name="subject"/> and returns a function
    /// that flushes and lists the events received so far.
    /// </summary>
    private async Task<Func<Task<List<AgentEvent>>>> CollectAsync(NatsConnection nc, string subject)
    {
        var events = new List<AgentEvent>();
        var sub = await nc.SubscribeCoreAsync<byte[]>(subject);
        Cleanup(sub.DisposeAsync);
        _ = Task.Run(async () =>
        {
            await foreach (var msg in sub.Msgs.ReadAllAsync())
            {
                if (WireJson.TryDeserialize<AgentEvent>(msg.Data ?? [], out var ev))
                {
                    lock (events)
                    {
                        events.Add(ev);
                    }
                }
            }
        });
        await nc.PingAsync();
        return async () =>
        {
            await nc.PingAsync();
            await Task.Delay(50);
            lock (events)
            {
                return [.. events];
            }
        };
    }

    private static List<AgentEvent> ProgressFrames(IEnumerable<AgentEvent> events) =>
        [.. events.Where(ev => ev.Type == EventTypes.Data && ev.Kind == DataKinds.Progress)];

    /// <summary>The Go <c>newStream</c>: a run's stream publishing on <paramref name="nc"/>.</summary>
    private static AgentStream NewStream(NatsConnection nc, string subject, string runId, bool progress, TimeProvider? time = null) =>
        new((s, data) => nc.PublishAsync(s, data), subject, runId, NullLogger.Instance, progress, time ?? TimeProvider.System);

    private static Turn TestTurn(Budget? budget, string runId = "run_t") =>
        new(new ChatRequest { Budget = budget }, runId, NullLogger.Instance, new CallerIdentity(), TimeProvider.System);

    /// <summary>An agent whose runs take their deadline from <paramref name="time"/> (not started).</summary>
    private Task<Agent> NewClockedAgentAsync(string name, TimeProvider time) =>
        NewAgentAsync(new AgentConfig { Name = name, Description = "d", TimeProvider = time });

    /// <summary>Starts <paramref name="agent"/> with a handler that returns at once, and runs one chat against it.</summary>
    private async Task<ChatRun> ChatOnceAsync(Agent agent)
    {
        agent.OnChat((_, _, _) => Task.CompletedTask);
        await agent.StartAsync();
        var run = await (await AgentClientAsync()).ChatAsync(agent.Card().Name, UserMessage("hi"));
        await DrainAsync(run);
        return run;
    }

    // The Go test reads ctx.Deadline() of runContext(turn). A .NET run has no deadline to
    // read: the fact is checked on the agent's clock instead — without a budget no amount of
    // time ends the run.
    [Fact]
    public async Task NoBudgetNoDeadline()
    {
        var time = new FakeTimeProvider();
        var agent = await NewClockedAgentAsync("budgetNone", time);
        var turn = TestTurn(null, "run_t1");
        var run = agent.NewRun(turn, honorBudget: true);

        // Zero/negative budget behaves like no budget.
        var run2 = agent.NewRun(TestTurn(new Budget { RemainingMs = 0 }, "run_t2"), honorBudget: true);
        var run3 = agent.NewRun(TestTurn(new Budget { RemainingMs = -1 }, "run_t3"), honorBudget: true);

        time.Advance(TimeSpan.FromDays(365));
        Assert.False(run.Token.IsCancellationRequested, "run has a deadline without a budget");
        Assert.False(run2.Token.IsCancellationRequested, "run has a deadline with RemainingMs 0");
        Assert.False(run3.Token.IsCancellationRequested, "run has a deadline with a negative RemainingMs");

        // Cancel keeps today's meaning: plain cancellation.
        run.Cancel(null);
        Assert.True(run.Token.IsCancellationRequested);
        Assert.Null(turn.CutReason);
        Assert.True(run.UserCancelled);
        run2.Finish();
        run3.Finish();

        // No capabilities: the ack carries no caps at all.
        var ack = (await ChatOnceAsync(agent)).Ack;
        Assert.True(ack.Accepted);
        Assert.Null(ack.Caps);
        Assert.DoesNotContain("caps", WireJson.SerializeToString(new ChatAck { Accepted = true, Caps = null }));

        // ... also as the bytes on the wire.
        var nc = await ConnectAsync();
        var reply = await nc.RequestAsync<byte[], byte[]>(
            Protocol.AgentSubject("budgetNone", "chat"),
            WireJson.Serialize(UserMessage("hi") with { StreamSubject = nc.NewInbox() }),
            replyOpts: new NatsSubOpts { Timeout = TimeSpan.FromSeconds(5) });
        var json = Encoding.UTF8.GetString(reply.Data ?? []);
        Assert.Contains("\"accepted\":true", json);
        Assert.DoesNotContain("caps", json);
    }

    [Fact]
    public async Task BudgetSetsDeadline()
    {
        var time = new FakeTimeProvider();
        var agent = await NewClockedAgentAsync("budgetDeadline", time);

        // The deadline is the budget minus the margin: 60s − BudgetMargin from the start.
        var turn = TestTurn(new Budget { RemainingMs = 60_000 }, "run_long");
        var run = agent.NewRun(turn, honorBudget: true);

        // A budget smaller than the margin still gets at least 1s, then fires as time_cap.
        var shortTurn = TestTurn(new Budget { RemainingMs = 2_000 }, "run_short");
        var shortRun = agent.NewRun(shortTurn, honorBudget: true);

        time.Advance(TimeSpan.FromSeconds(1) - TimeSpan.FromMilliseconds(1));
        Assert.False(shortRun.Token.IsCancellationRequested, "short deadline fired before the 1s floor");
        time.Advance(TimeSpan.FromMilliseconds(1));
        Assert.True(shortRun.Token.IsCancellationRequested, "budget deadline did not fire");
        Assert.Equal(Reasons.TimeCap, shortTurn.CutReason);
        Assert.False(shortRun.UserCancelled);

        // 1s in; the 60s budget ends at 55s.
        var want = TimeSpan.FromSeconds(60) - Agent.BudgetMargin;
        Assert.Equal(TimeSpan.FromSeconds(55), want);
        time.Advance(want - TimeSpan.FromSeconds(1) - TimeSpan.FromMilliseconds(1));
        Assert.False(run.Token.IsCancellationRequested, "deadline fired before budget − margin");
        Assert.Null(turn.CutReason);
        time.Advance(TimeSpan.FromMilliseconds(1));
        Assert.True(run.Token.IsCancellationRequested, "run has no deadline with a budget");
        Assert.Equal(Reasons.TimeCap, turn.CutReason);

        // The synchronous path (invoke) does not take a deadline from the budget.
        var invokeRun = agent.NewRun(TestTurn(new Budget { RemainingMs = 2_000 }, "run_invoke"), honorBudget: false);
        time.Advance(TimeSpan.FromDays(1));
        Assert.False(invokeRun.Token.IsCancellationRequested);
        invokeRun.Finish();
    }

    [Fact]
    public async Task CapsAdvertisedOnlyWhenOptedIn()
    {
        var a = await NewAgentAsync(new AgentConfig { Name = "capsNone", Description = "d" });
        var card = a.Card();
        Assert.False(card.Capabilities.Progress || card.Capabilities.Renew, $"card caps on without opt-in: {card.Capabilities}");
        Assert.Null((await ChatOnceAsync(a)).Ack.Caps); // no ack caps without opt-in

        a = await NewAgentAsync(new AgentConfig { Name = "capsProgress", Description = "d", Progress = true });
        card = a.Card();
        Assert.True(card.Capabilities.Progress && !card.Capabilities.Renew, $"card caps = {card.Capabilities}, want progress only");
        var caps = (await ChatOnceAsync(a)).Ack.Caps;
        Assert.NotNull(caps);
        Assert.True(caps.Progress, $"ack caps = {caps}, want progress");

        a = await NewAgentAsync(new AgentConfig { Name = "capsRenew", Description = "d", Renew = true });
        Assert.True(a.Card().Capabilities.Renew, "card renew cap not advertised");
        caps = (await ChatOnceAsync(a)).Ack.Caps;
        Assert.NotNull(caps); // ack caps present with renew opted in
        Assert.False(caps.Progress);
    }

    [Fact]
    public async Task ProgressThrottled()
    {
        var nc = await ConnectAsync();
        const string subject = "test.progress.throttle";
        var got = await CollectAsync(nc, subject);

        var time = new FakeTimeProvider();
        var s = NewStream(nc, subject, "run_p", progress: true, time);

        for (var i = 0; i < 10; i++)
        {
            await s.ProgressAsync("model");
            time.Advance(TimeSpan.FromMilliseconds(100)); // 10 calls within 1s
        }

        Assert.Single(ProgressFrames(await got())); // after 10 calls in 1s

        // Still inside the 15s window, counted from the frame that went out.
        time.Advance(AgentStream.ProgressEvery - TimeSpan.FromSeconds(1) - TimeSpan.FromMilliseconds(1));
        await s.ProgressAsync("model");
        Assert.Single(ProgressFrames(await got()));

        time.Advance(TimeSpan.FromSeconds(15));
        await s.ProgressAsync("tool", "search");
        var frames = ProgressFrames(await got());
        Assert.Equal(2, frames.Count);
        Assert.NotNull(frames[1].Payload);
        var p = frames[1].Payload!.Value.Deserialize<ProgressPayload>(WireJson.Options);
        Assert.Equal(("tool", "search"), (p?.Stage, p?.Detail));
    }

    [Fact]
    public async Task ProgressNoopWithoutCapability()
    {
        var nc = await ConnectAsync();
        const string subject = "test.progress.off";
        var got = await CollectAsync(nc, subject);

        var s = NewStream(nc, subject, "run_q", progress: false);
        await s.ProgressAsync("model");
        Assert.Empty(await got()); // nothing published without the capability
    }

    [Fact]
    public async Task DoneAndErrorWithReason()
    {
        var nc = await ConnectAsync();
        var got = await CollectAsync(nc, "test.reason.done");
        var s = NewStream(nc, "test.reason.done", "run_r", progress: false);
        await s.DoneWithReasonAsync(StopReasons.EndTurn, Reasons.TimeCap);
        var ev = Assert.Single(await got());
        Assert.Equal((EventTypes.Done, Reasons.TimeCap), (ev.Type, ev.Reason));
        Assert.Equal(StopReasons.EndTurn, ev.StopReason);

        got = await CollectAsync(nc, "test.reason.error");
        s = NewStream(nc, "test.reason.error", "run_s", progress: false);
        await s.ErrorWithReasonAsync("cut", Codes.Internal, Reasons.Interrupted);
        ev = Assert.Single(await got());
        Assert.Equal((EventTypes.Error, Reasons.Interrupted, "cut"), (ev.Type, ev.Reason, ev.Error));
        Assert.Equal(Codes.Internal, ev.ErrorStatus);
    }
}
