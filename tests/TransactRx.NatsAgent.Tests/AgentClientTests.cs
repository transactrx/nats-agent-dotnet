using System.Diagnostics;
using System.Globalization;
using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;
using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Progress;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;
using StreamReader = TransactRx.NatsAgent.Client.StreamReader;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/agentclient/client_test.go, part 1: chat turns against a fake agent (idle cuts
/// driven by progress frames, ack caps, the IDT source, cancel reasons) and the stream reader
/// on a manual clock. The renew tests of the same Go file are in
/// <see cref="AgentClientRenewTests"/>.
/// </summary>
public sealed class AgentClientTests(NatsServerFixture nats) : AgentClientTestBase(nats)
{
    private static readonly TimeSpan FiveSeconds = TimeSpan.FromSeconds(5);

    private static TimeSpan Ms(int milliseconds) => TimeSpan.FromMilliseconds(milliseconds);

    private static string Num(int i) => i.ToString(CultureInfo.InvariantCulture);

    [Fact]
    public async Task ChatWithIdleCut()
    {
        var stop = StopSignal();
        var f = await StartFakeAgentAsync(ProgressCaps, publish => PingsUntilAsync(stop.Token, publish));
        var c = await AgentClientAsync();

        var window = Ms(300);
        var started = Stopwatch.StartNew();
        var run = await c.ChatAsync(f.Name, ChatReq(), new ChatOptions { IdleWindow = window });
        var evs = await DrainWithinAsync(run, FiveSeconds);
        var elapsed = started.Elapsed;
        Assert.True(evs.Count == 1, $"want only the terminal event (pings not delivered), got {Describe(evs)}");
        var ev = evs[0];
        Assert.True(
            ev.Type == EventTypes.Error && ev.Reason == Reasons.ProgressIdle && ev.RunId == "run-1",
            $"terminal event = {Describe([ev])}");
        Assert.True(ev.Error!.Contains("300ms", StringComparison.Ordinal), $"message should name the window: \"{ev.Error}\"");
        Assert.True(elapsed >= window, $"cut too early: {elapsed} < {window}");

        var id = await ReceiveAsync(f.Cancels, 2 * Second, "no cancel published on idle cut");
        Assert.Equal("run-1", id);
    }

    [Fact]
    public async Task ChatWithIdleCutNamesStage()
    {
        var stop = StopSignal();
        var f = await StartFakeAgentAsync(ProgressCaps, async publish =>
        {
            var payload = WireJson.ToElement(new ProgressPayload { Stage = "searching" });
            await publish(new AgentEvent { Type = EventTypes.Data, Kind = DataKinds.Progress, Payload = payload });
            await PingsUntilAsync(stop.Token, publish);
        });
        var c = await AgentClientAsync();

        var run = await c.ChatAsync(f.Name, ChatReq(), new ChatOptions { IdleWindow = Ms(200) });
        var evs = await DrainWithinAsync(run, FiveSeconds);
        var last = evs[^1];
        Assert.True(
            last.Reason == Reasons.ProgressIdle && (last.Error ?? "").Contains("searching", StringComparison.Ordinal),
            $"terminal event = {Describe([last])}");
    }

    [Fact]
    public async Task ChatWithIdleExtendsOnProgress()
    {
        var window = Ms(200);
        var f = await StartFakeAgentAsync(ProgressCaps, async publish =>
        {
            // Progress every 80ms for 3x the window, pings in between, then done.
            var running = Stopwatch.StartNew();
            while (running.Elapsed < 3 * window)
            {
                await publish(new AgentEvent { Type = EventTypes.Ping });
                await Task.Delay(Ms(40));
                await publish(new AgentEvent { Type = EventTypes.Text, TextDelta = "x" });
                await Task.Delay(Ms(40));
            }

            await publish(new AgentEvent { Type = EventTypes.Done, StopReason = "end_turn" });
        });
        var c = await AgentClientAsync();

        var run = await c.ChatAsync(f.Name, ChatReq(), new ChatOptions { IdleWindow = window });
        var evs = await DrainWithinAsync(run, FiveSeconds);
        var last = evs[^1];
        Assert.True(last.Type == EventTypes.Done, $"want done, got {Describe([last])}");
        Assert.DoesNotContain(evs, ev => ev.Type == EventTypes.Ping); // ping delivered to caller
        Assert.False(f.Cancels.TryRead(out _), "cancel published although progress kept the run alive");
    }

    [Fact]
    public async Task ChatUnchangedWithoutOptions()
    {
        var f = await StartFakeAgentAsync(null, async publish =>
        {
            // Only pings for a while: no idle window, so no cut.
            var running = Stopwatch.StartNew();
            while (running.Elapsed < Ms(400))
            {
                await publish(new AgentEvent { Type = EventTypes.Ping });
                await Task.Delay(Ms(20));
            }

            await publish(new AgentEvent { Type = EventTypes.Done, StopReason = "end_turn" });
        });
        var c = await AgentClientAsync();

        var run = await c.ChatAsync(f.Name, ChatReq());
        Assert.Equal(new AckCaps(), run.Caps); // caps on an old agent
        var evs = await DrainWithinAsync(run, FiveSeconds);
        Assert.True(evs.Count == 1 && evs[0].Type == EventTypes.Done, $"want only done, got {Describe(evs)}");
    }

    [Fact]
    public async Task RunCapsFromAck()
    {
        var caps = new AckCaps { Progress = true, RenewSubject = "trx.agent.x.renew.abc" };
        var f = await StartFakeAgentAsync(caps, async publish =>
        {
            await publish(new AgentEvent { Type = EventTypes.Done });
        });
        var run = await (await AgentClientAsync()).ChatAsync(f.Name, ChatReq());
        Assert.Equal(caps, run.Caps);
        await DrainWithinAsync(run, FiveSeconds);
    }

    [Fact]
    public async Task IDTSourceReadAtCallTime()
    {
        var f = await StartFakeAgentAsync(null, async publish =>
        {
            await publish(new AgentEvent { Type = EventTypes.Done });
        });
        var c = await AgentClientAsync();

        var current = "token-1";
        using (CallContext.WithIdt("static"))
        using (CallContext.WithIdtSource(() => Volatile.Read(ref current)))
        {
            foreach (var want in new[] { "token-1", "token-2" })
            {
                Volatile.Write(ref current, want);
                Assert.Equal(want, CallContext.Idt);
                var run = await c.ChatAsync(f.Name, ChatReq());
                await DrainWithinAsync(run, FiveSeconds);
            }

            Assert.Equal(["token-1", "token-2"], f.SeenIdts); // headers seen

            // An empty source falls back to the WithIdt value.
            Volatile.Write(ref current, "");
            Assert.Equal("static", CallContext.Idt);
        }

        // No source: unchanged behavior.
        using (CallContext.WithIdt("plain"))
        {
            Assert.Equal("plain", CallContext.Idt);
        }
    }

    [Fact]
    public async Task IDTSourceToolCall()
    {
        var nc = await ConnectAsync();
        var name = $"faketool{NextSequence()}";
        var gate = new object();
        var seen = new List<string>();
        await SubscribeAsync(nc, Protocol.ToolSubject(name, "run"), async msg =>
        {
            lock (gate)
            {
                seen.Add(Header(msg.Headers, Protocol.HeaderIdt));
            }

            await RespondOkAsync(msg, new ToolRunResponse());
        });
        var tc = await ToolClientAsync();

        var current = "token-1";
        using (CallContext.WithIdtSource(() => Volatile.Read(ref current)))
        {
            foreach (var token in new[] { "token-1", "token-2" })
            {
                Volatile.Write(ref current, token);
                await tc.RunAsync(name, new ToolRunRequest(), 2 * Second);
            }
        }

        lock (gate)
        {
            Assert.Equal(["token-1", "token-2"], seen); // tool headers seen
        }
    }

    // When the idle window expires while frames are still queued (the consumer was slow), the
    // queued frames win: they are delivered in order and a queued terminal ends the run
    // normally, with no idle cut and no cancel.
    [Fact]
    public async Task IdleExpiryPrefersQueuedTerminal()
    {
        const int texts = 80; // more than the Events buffer, so the reader blocks
        var f = await StartFakeAgentAsync(ProgressCaps, async publish =>
        {
            for (var i = 0; i < texts; i++)
            {
                await publish(new AgentEvent { Type = EventTypes.Text, TextDelta = Num(i) });
            }

            await publish(new AgentEvent { Type = EventTypes.Done, StopReason = StopReasons.EndTurn });
        });
        var c = await AgentClientAsync();
        var window = Ms(200);
        var run = await c.ChatAsync(f.Name, ChatReq(), new ChatOptions { IdleWindow = window });
        await Task.Delay(3 * window); // nobody reads: the watcher expires with frames queued
        var evs = await DrainWithinAsync(run, FiveSeconds);
        Assert.True(evs.Count == texts + 1, $"got {evs.Count} events, want {texts} texts + done; last = {Describe(evs.TakeLast(1))}");
        for (var i = 0; i < texts; i++)
        {
            Assert.True(
                evs[i].Type == EventTypes.Text && evs[i].TextDelta == Num(i),
                $"event {i} = {Describe([evs[i]])}, want text {i} in order");
        }

        var last = evs[texts];
        Assert.True(last.Type == EventTypes.Done && string.IsNullOrEmpty(last.Reason), $"terminal = {Describe([last])}, want the agent's done");
        await AssertNothingAsync(f.Cancels, Ms(200), id => $"cancel \"{id}\" published although the run ended normally");
    }

    // End to end: progress frames queued behind a slow reader when the idle window expires do
    // not cut the turn, whichever path handles them.
    [Fact]
    public async Task IdleExpiryQueuedProgressEndToEnd()
    {
        const int texts = 80; // more than the Events buffer, so the reader blocks
        var sendDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Cleanup(() =>
        {
            sendDone.TrySetCanceled();
            return ValueTask.CompletedTask;
        });
        var f = await StartFakeAgentAsync(ProgressCaps, async publish =>
        {
            for (var i = 0; i < texts; i++)
            {
                await publish(new AgentEvent { Type = EventTypes.Text, TextDelta = Num(i) });
            }

            await sendDone.Task;
            await publish(new AgentEvent { Type = EventTypes.Done, StopReason = StopReasons.EndTurn });
        });
        var c = await AgentClientAsync();
        var window = Ms(300);
        var run = await c.ChatAsync(f.Name, ChatReq(), new ChatOptions { IdleWindow = window });
        await Task.Delay(2 * window); // nobody reads: the watcher expires with text frames queued

        var evs = new List<AgentEvent>();
        using (var timeout = new CancellationTokenSource(FiveSeconds))
        {
            while (evs.Count < texts)
            {
                AgentEvent ev;
                try
                {
                    ev = await run.Events.ReadAsync(timeout.Token);
                }
                catch (OperationCanceledException)
                {
                    Assert.Fail($"only {evs.Count} texts");
                    throw;
                }
                catch (ChannelClosedException)
                {
                    Assert.Fail($"run ended after {evs.Count} events: {Describe(evs.TakeLast(1))}");
                    throw;
                }

                Assert.True(ev.Type == EventTypes.Text, $"event {evs.Count} = {Describe([ev])}, want text (no idle cut)");
                evs.Add(ev);
            }
        }

        sendDone.TrySetResult();
        var rest = await DrainWithinAsync(run, FiveSeconds);
        Assert.True(
            rest.Count == 1 && rest[0].Type == EventTypes.Done && string.IsNullOrEmpty(rest[0].Reason),
            $"after texts got {Describe(rest)}, want the agent's done only");
        for (var i = 0; i < evs.Count; i++)
        {
            Assert.True(evs[i].TextDelta == Num(i), $"text {i} = \"{evs[i].TextDelta}\", out of order");
        }

        await AssertNothingAsync(f.Cancels, Ms(200), id => $"cancel \"{id}\" published although progress was queued");
    }

    // Only pings queued at expiry: still idle, the cut happens.
    [Fact]
    public async Task IdleExpiryWithOnlyQueuedPingsCuts()
    {
        const int texts = 65; // the reader blocks on the last one
        var stop = StopSignal();
        var f = await StartFakeAgentAsync(ProgressCaps, async publish =>
        {
            for (var i = 0; i < texts; i++)
            {
                await publish(new AgentEvent { Type = EventTypes.Text, TextDelta = Num(i) });
            }

            await PingsUntilAsync(stop.Token, publish);
        });
        var c = await AgentClientAsync();
        var window = Ms(200);
        var run = await c.ChatAsync(f.Name, ChatReq(), new ChatOptions { IdleWindow = window });
        await Task.Delay(3 * window); // pings queue up behind the blocked reader
        var evs = await DrainWithinAsync(run, FiveSeconds);
        Assert.True(evs.Count == texts + 1, $"got {evs.Count} events, want {texts} texts + idle cut");
        var last = evs[texts];
        Assert.True(
            last.Type == EventTypes.Error && last.Reason == Reasons.ProgressIdle,
            $"terminal = {Describe([last])}, want progress_idle cut");

        var id = await ReceiveAsync(f.Cancels, 2 * Second, "no cancel published on idle cut");
        Assert.Equal("run-1", id);
    }

    // ─── the stream reader on a manual clock ──────────────────────────────

    /// <summary>The reader tests' idle window on a manual clock.</summary>
    private static readonly TimeSpan ReaderWindow = TimeSpan.FromSeconds(20);

    private sealed record ClockReader(
        StreamReader Reader, FakeTimeProvider Clock, Channel<NatsMsg<byte[]>> Msgs, Channel<AgentEvent> Out, Counter Cancels)
    {
        /// <summary>The Go <c>idleReady</c>: whether the reader's current watcher has expired.</summary>
        public bool IdleReady => Reader.Watcher!.Expired.IsCompleted;

        /// <summary>The Go <c>close(out)</c> + <c>range out</c>: everything the reader delivered.</summary>
        public List<AgentEvent> Delivered()
        {
            Out.Writer.Complete();
            var events = new List<AgentEvent>();
            while (Out.Reader.TryRead(out var ev))
            {
                events.Add(ev);
            }

            return events;
        }
    }

    /// <summary>
    /// The Go <c>newClockReader</c>: a stream reader whose watchers run on a manual clock;
    /// nothing in these tests waits on real time.
    /// </summary>
    private ClockReader NewClockReader()
    {
        var clock = new FakeTimeProvider();
        var msgs = Channel.CreateBounded<NatsMsg<byte[]>>(8);
        var output = Channel.CreateBounded<AgentEvent>(8);
        var cancels = new Counter();
        var reader = new StreamReader("run-1", ReaderWindow, msgs.Reader, output.Writer, w => new ProgressWatcher(w, clock))
        {
            CancelRemote = _ =>
            {
                cancels.Increment();
                return ValueTask.CompletedTask;
            },
        };
        Cleanup(() =>
        {
            reader.Stop();
            return ValueTask.CompletedTask;
        });
        return new ClockReader(reader, clock, msgs, output, cancels);
    }

    /// <summary>The Go <c>testReader</c>: <see cref="NewClockReader"/> with the watcher already expired.</summary>
    private ClockReader ExpiredReader()
    {
        var r = NewClockReader();
        r.Clock.Advance(ReaderWindow);
        Assert.True(r.IdleReady, "watcher did not expire at the window");
        return r;
    }

    private static NatsMsg<byte[]> Frame(AgentEvent ev)
    {
        var data = WireJson.Serialize(ev with { RunId = "run-1" });
        return new NatsMsg<byte[]>("stream", null, data.Length, null, data, null);
    }

    // One progress frame queued at expiry, taken by the normal message path: the watcher is
    // revived, so the following loop turn cannot cut.
    [Fact]
    public async Task ProgressAfterExpiryOnMainPathDoesNotCut()
    {
        var r = ExpiredReader();
        await r.Msgs.Writer.WriteAsync(Frame(new AgentEvent { Type = EventTypes.Text, TextDelta = "a" }));
        Assert.False(await r.Reader.OnMsg(await r.Msgs.Reader.ReadAsync()), "text ended the run");
        Assert.False(r.IdleReady, "watcher still expired after progress on the main path: next loop turn would cut");
        Assert.True(
            await r.Reader.OnMsg(Frame(new AgentEvent { Type = EventTypes.Done, StopReason = StopReasons.EndTurn })),
            "done did not end the run");

        var evs = r.Delivered();
        Assert.True(
            evs.Count == 2 && evs[0].Type == EventTypes.Text && evs[1].Type == EventTypes.Done && string.IsNullOrEmpty(evs[1].Reason),
            $"events = {Describe(evs)}, want text then done");
        Assert.Equal(0, r.Cancels.Value); // cancel published
    }

    // Progress frames queued at expiry, taken by the idle branch's drain: no cut, watcher
    // revived.
    [Fact]
    public async Task IdleExpiryWithQueuedProgressDoesNotCut()
    {
        var r = ExpiredReader();
        await r.Msgs.Writer.WriteAsync(Frame(new AgentEvent { Type = EventTypes.Ping }));
        await r.Msgs.Writer.WriteAsync(Frame(new AgentEvent { Type = EventTypes.Text, TextDelta = "a" }));
        Assert.False(await r.Reader.OnIdle(), "idle branch ended the run although progress was queued");
        Assert.False(r.IdleReady, "watcher not revived by queued progress");
        Assert.True(
            await r.Reader.OnMsg(Frame(new AgentEvent { Type = EventTypes.Done, StopReason = StopReasons.EndTurn })),
            "done did not end the run");

        var evs = r.Delivered();
        Assert.True(
            evs.Count == 2 && evs[0].Type == EventTypes.Text && evs[1].Type == EventTypes.Done,
            $"events = {Describe(evs)}, want text then done (ping not delivered)");
        Assert.Equal(0, r.Cancels.Value); // cancel published
    }

    // Only pings queued (or nothing): the idle branch cuts.
    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task IdleBranchCutsWithoutProgress(int queuedPings)
    {
        var r = ExpiredReader();
        for (var i = 0; i < queuedPings; i++)
        {
            await r.Msgs.Writer.WriteAsync(Frame(new AgentEvent { Type = EventTypes.Ping }));
        }

        Assert.True(await r.Reader.OnIdle(), $"queued {queuedPings} pings: no cut");

        var evs = r.Delivered();
        Assert.True(
            evs.Count == 1 && evs[0].Reason == Reasons.ProgressIdle,
            $"queued {queuedPings} pings: events = {Describe(evs)}, want one progress_idle cut");
        Assert.Equal(1, r.Cancels.Value);
    }

    // The window can end between the expiry check and Observe (Observe is then a no-op on the
    // expired watcher). The progress frame must still revive it.
    [Fact]
    public async Task ProgressAtWindowEdgeRevives()
    {
        var r = NewClockReader();
        r.Clock.Advance(ReaderWindow - Ms(1));
        Assert.False(r.IdleReady, "expired before the window");
        var fired = false;
        r.Reader.BeforeObserve = () =>
        {
            if (!fired)
            {
                fired = true;
                r.Clock.Advance(Ms(1)); // the timer fires right at the edge
            }
        };
        Assert.False(await r.Reader.OnMsg(Frame(new AgentEvent { Type = EventTypes.Text, TextDelta = "a" })), "text ended the run");
        Assert.True(fired, "edge hook not reached");
        Assert.False(r.IdleReady, "progress at the window edge left the watcher expired: next loop turn would cut");
        Assert.True(r.Out.Reader.TryRead(out var ev) && ev.Type == EventTypes.Text, "the text was not delivered");

        // The revived window is a full one from the frame.
        r.Clock.Advance(ReaderWindow - Ms(1));
        Assert.False(r.IdleReady, "revived watcher expired early");
        r.Clock.Advance(Ms(1));
        Assert.True(r.IdleReady, "revived watcher never expires");
        Assert.Equal(0, r.Cancels.Value);
    }

    // The client's own idle cut tells the agent why: reason progress_idle.
    [Fact]
    public async Task ClientIdleCutSendsReason()
    {
        var stop = StopSignal();
        var f = await StartFakeAgentAsync(ProgressCaps, publish => PingsUntilAsync(stop.Token, publish));
        var run = await (await AgentClientAsync()).ChatAsync(f.Name, ChatReq(), new ChatOptions { IdleWindow = Ms(200) });
        await DrainWithinAsync(run, FiveSeconds);
        var reason = await ReceiveAsync(f.Reasons, 2 * Second, "no cancel published on idle cut");
        Assert.Equal(Reasons.ProgressIdle, reason);
    }

    [Fact]
    public async Task CancelWithReasonSendsReason()
    {
        var f = await StartFakeAgentAsync(null, null);
        var c = await AgentClientAsync();
        await c.CancelWithReasonAsync(f.Name, "run-1", Reasons.TokenRotation);
        Assert.Equal(Reasons.TokenRotation, await ReceiveAsync(f.Reasons, FiveSeconds, "no cancel received"));

        var run = DetachedRun(c, f.Name, "run-1");
        await run.CancelWithReasonAsync(Reasons.Interrupted);
        Assert.Equal(Reasons.Interrupted, await ReceiveAsync(f.Reasons, FiveSeconds, "no cancel received"));

        await run.CancelAsync();
        Assert.Equal("", await ReceiveAsync(f.Reasons, FiveSeconds, "no cancel received")); // a plain cancel sends no reason
    }

    // An agent whose ack does not declare progress only promised pings: the idle window is
    // ignored for its run (as a plain chat).
    [Fact]
    public async Task IdleWindowIgnoredWithoutProgressCap()
    {
        foreach (var caps in new AckCaps?[] { null, new AckCaps { RenewSubject = "trx.agent.x.renew.abc" } })
        {
            var stop = StopSignal();
            var f = await StartFakeAgentAsync(caps, async publish =>
            {
                await PingsUntilAsync(stop.Token, publish);
                await publish(new AgentEvent { Type = EventTypes.Done, StopReason = StopReasons.EndTurn });
            });
            var window = Ms(150);
            var run = await (await AgentClientAsync()).ChatAsync(f.Name, ChatReq(), new ChatOptions { IdleWindow = window });
            await Task.Delay(4 * window);
            await stop.CancelAsync();
            var evs = await DrainWithinAsync(run, FiveSeconds);
            Assert.True(
                evs.Count == 1 && evs[0].Type == EventTypes.Done,
                $"caps {caps}: events = {Describe(evs)}, want only the agent's done (no idle cut)");
            Assert.False(f.Cancels.TryRead(out var id), $"caps {caps}: cancel \"{id}\" published");
        }
    }
}
