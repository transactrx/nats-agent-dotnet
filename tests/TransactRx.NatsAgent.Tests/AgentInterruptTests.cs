using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/agent/interrupt_test.go: InterruptAll and the cut reasons. Where the Go tests
/// read <c>context.Cause(ctx)</c> (ErrTimeCap, ErrInterrupted, *CutError), these read
/// <see cref="Turn.CutReason"/>.
/// </summary>
public sealed class AgentInterruptTests(NatsServerFixture nats) : AgentRuntimeTestBase(nats)
{
    /// <summary>What a handler does once its run has ended (the Go <c>ret(ctx)</c>).</summary>
    public enum Ret
    {
        /// <summary>Go <c>return ctx.Err()</c>: the cancellation surfaces as the handler's failure.</summary>
        ContextError,

        /// <summary>Go <c>return errors.New("upstream: secret detail")</c>.</summary>
        OwnError,

        /// <summary>Go <c>return nil</c>.</summary>
        Nil,
    }

    private static void Return(Ret ret, CancellationToken ct)
    {
        switch (ret)
        {
            case Ret.ContextError:
                ct.ThrowIfCancellationRequested();
                break;
            case Ret.OwnError:
                throw new InvalidOperationException("upstream: secret detail");
        }
    }

    /// <summary>Waits for the run to end, at most 5s, so a broken runtime fails instead of hanging.</summary>
    private static async Task WaitEnded(CancellationToken ct)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(5), ct);
        }
        catch (OperationCanceledException)
        {
            // the run ended
        }
    }

    /// <summary>
    /// The Go <c>waitCtxHandler</c>: signals started, waits for the run to end, reports why it
    /// was cut on <paramref name="causes"/> and returns <paramref name="ret"/> without a
    /// terminal frame.
    /// </summary>
    private static ChatHandler WaitCtxHandler(Channel<bool> started, Channel<string?> causes, Ret ret) =>
        async (turn, _, ct) =>
        {
            started.Writer.TryWrite(true);
            await WaitEnded(ct);
            causes.Writer.TryWrite(turn.CutReason);
            Return(ret, ct);
        };

    /// <summary>
    /// The Go <c>cutProbeHandler</c>: waits for the run to end and reports the cut reason and
    /// the run's user-cancel flag, then fails with the cancellation, without a terminal frame.
    /// </summary>
    private static ChatHandler CutProbeHandler(Channel<bool> started, Channel<(string? CutReason, bool Cancelled)> seen) =>
        async (turn, _, ct) =>
        {
            started.Writer.TryWrite(true);
            await WaitEnded(ct);
            seen.Writer.TryWrite((turn.CutReason, turn.Run!.UserCancelled));
            ct.ThrowIfCancellationRequested();
        };

    private static Task WaitStarted(Channel<bool> started) => WaitAsync(started.Reader, "handler never started");

    /// <summary>The Go <c>cancelRun</c>: the owner must acknowledge the cancel.</summary>
    private static async Task CancelRun(ChatRun run, string reason) =>
        Assert.True(await run.CancelWithReasonAsync(reason), "cancel not acknowledged, want cancelled:true");

    [Fact]
    public async Task InterruptAllNoRuns()
    {
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions());
        Assert.Equal(0, a.Agent.InterruptAll());
    }

    [Fact]
    public async Task InterruptAllCancelsWithCause()
    {
        var started = Channel.CreateUnbounded<bool>();
        var causes = Channel.CreateUnbounded<string?>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions { Handler = WaitCtxHandler(started, causes, Ret.Nil) });
        var r1 = await ChatAsAsync(a, "");
        var r2 = await ChatAsAsync(a, "");
        await WaitStarted(started);
        await WaitStarted(started);
        Assert.Equal(2, a.Agent.InterruptAll());
        for (var i = 0; i < 2; i++)
        {
            Assert.Equal(Reasons.Interrupted, await WaitAsync(causes.Reader, "handler never reported its cause"));
        }

        await DrainAsync(r1);
        await DrainAsync(r2);
    }

    [Fact]
    public async Task InterruptEmitsReason()
    {
        var started = Channel.CreateUnbounded<bool>();
        var causes = Channel.CreateUnbounded<string?>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions { Handler = WaitCtxHandler(started, causes, Ret.ContextError) });
        var run = await ChatAsAsync(a, "");
        await WaitStarted(started);
        a.Agent.InterruptAll();
        var ev = await TerminalAsync(run);
        Assert.Equal(
            (EventTypes.Error, Reasons.Interrupted, "interrupted", Codes.Internal),
            (ev.Type, ev.Reason, ev.Error, ev.ErrorStatus));
    }

    // The message is fixed: the handler's raw error text never leaks.
    [Theory]
    [InlineData(Ret.ContextError, "time limit reached")] // handler returns ctx error
    [InlineData(Ret.OwnError, "time limit reached")] // handler returns own error
    [InlineData(Ret.Nil, "time limit reached")] // handler returns nil
    public async Task TimeCapFallbackReason(Ret ret, string wantErr)
    {
        var started = Channel.CreateUnbounded<bool>();
        var causes = Channel.CreateUnbounded<string?>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions { Handler = WaitCtxHandler(started, causes, ret) }, ret.ToString());
        var c = new AgentClient(await CallerAsync(), Quiet);
        var run = await c.ChatAsync(a.Name, new ChatRequest
        {
            Message = Message.UserText("hi"),
            Budget = new Budget { RemainingMs = 2_000 }, // → 1s deadline floor
        });
        await WaitStarted(started);
        Assert.Equal(Reasons.TimeCap, await WaitAsync(causes.Reader, "handler never reported its cause"));
        var ev = await TerminalAsync(run);
        Assert.Equal(
            (EventTypes.Error, Reasons.TimeCap, wantErr, Codes.Internal),
            (ev.Type, ev.Reason, ev.Error, ev.ErrorStatus));
    }

    [Fact]
    public async Task CancelWithoutReasonIsUserCancel()
    {
        var started = Channel.CreateUnbounded<bool>();
        var seen = Channel.CreateUnbounded<(string? CutReason, bool Cancelled)>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions { Handler = CutProbeHandler(started, seen) });
        var run = await ChatAsAsync(a, "");
        await WaitStarted(started);
        Assert.True(await run.CancelAsync(), "Cancel not acknowledged");
        var got = await WaitAsync(seen.Reader, "handler never reported");
        Assert.Equal((null, true), got); // not a cut: a user cancel
        var ev = await TerminalAsync(run);
        Assert.Equal((EventTypes.Done, StopReasons.Cancelled, null), (ev.Type, ev.StopReason, ev.Reason));
    }

    [Fact]
    public async Task CancelUnknownReasonIsUserCancel()
    {
        var started = Channel.CreateUnbounded<bool>();
        var seen = Channel.CreateUnbounded<(string? CutReason, bool Cancelled)>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions { Handler = CutProbeHandler(started, seen) });
        var run = await ChatAsAsync(a, "");
        await WaitStarted(started);
        await CancelRun(run, "user_was_bored");
        var got = await WaitAsync(seen.Reader, "handler never reported");
        Assert.Equal((null, true), got); // a plain cancel
        var ev = await TerminalAsync(run);
        Assert.Equal((EventTypes.Done, StopReasons.Cancelled), (ev.Type, ev.StopReason));
    }

    [Theory]
    [InlineData(Reasons.ProgressIdle, "stopped: no progress")]
    [InlineData(Reasons.TokenRotation, "stopped: access could not be renewed")]
    [InlineData(Reasons.Interrupted, "interrupted")]
    [InlineData(Reasons.TimeCap, "time limit reached")]
    [InlineData(Reasons.Unavailable, "stopped: a required service was unavailable")]
    public async Task CancelWithReasonIsCut(string reason, string msg)
    {
        var started = Channel.CreateUnbounded<bool>();
        var seen = Channel.CreateUnbounded<(string? CutReason, bool Cancelled)>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions { Handler = CutProbeHandler(started, seen) }, reason);
        var run = await ChatAsAsync(a, "");
        await WaitStarted(started);
        await CancelRun(run, reason);
        var got = await WaitAsync(seen.Reader, "handler never reported");
        Assert.Equal((reason, false), got);
        var ev = await TerminalAsync(run);
        Assert.Equal((EventTypes.Error, reason, msg, Codes.Internal), (ev.Type, ev.Reason, ev.Error, ev.ErrorStatus));
    }

    // The Go test maps context causes to wire reasons through CutReason(ctx). The .NET run
    // has no error causes: each way a run can end records its reason on the internal AgentRun,
    // which Turn.CutReason reads. Same table, through that surface.
    [Fact]
    public async Task CutReasonMapping()
    {
        static (Turn Turn, AgentRun Run) NewRun()
        {
            var turn = new Turn(new ChatRequest(), "run_t", NullLogger.Instance, new CallerIdentity(), TimeProvider.System);
            var run = new AgentRun();
            turn.Run = run;
            return (turn, run);
        }

        // ErrTimeCap: the budget deadline fired.
        var time = new Microsoft.Extensions.Time.Testing.FakeTimeProvider();
        var (turn, run) = NewRun();
        run.SetDeadline(time, TimeSpan.FromSeconds(1));
        Assert.Null(turn.CutReason); // live, before the deadline
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.True(run.Token.IsCancellationRequested);
        Assert.Equal(Reasons.TimeCap, turn.CutReason);

        // ErrInterrupted and *CutError{Reason}: the reason of the cut.
        foreach (var reason in new[] { Reasons.Interrupted, Reasons.ProgressIdle, Reasons.Unavailable, Reasons.TokenRotation })
        {
            (turn, run) = NewRun();
            run.Cancel(reason);
            Assert.True(run.Token.IsCancellationRequested);
            Assert.Equal(reason, turn.CutReason);
            Assert.False(run.UserCancelled);
        }

        // Any other cause is not a cut: the handler returned (the run's token is cancelled
        // to stop what still uses it) ...
        (turn, run) = NewRun();
        run.Finish();
        Assert.True(run.Token.IsCancellationRequested);
        Assert.Null(turn.CutReason);
        Assert.False(run.UserCancelled);

        // ... or a plain cancel (Go: cancel(nil) → context.Canceled).
        (turn, run) = NewRun();
        run.Cancel(null);
        Assert.True(run.Token.IsCancellationRequested);
        Assert.Null(turn.CutReason);
        Assert.True(run.UserCancelled);

        // A live run, and a turn that never ran, were not cut.
        (turn, run) = NewRun();
        Assert.False(run.Token.IsCancellationRequested);
        Assert.Null(turn.CutReason);
        turn.Run = null;
        Assert.Null(turn.CutReason);

        // The Go CutError.Error() text "turn cut: <reason>" is, in .NET, the text of the
        // fallback error event of a cut whose reason has no fixed message.
        var started = Channel.CreateUnbounded<Turn>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions
        {
            Handler = async (t, _, ct) =>
            {
                started.Writer.TryWrite(t);
                await WaitEnded(ct);
            },
        });
        var chat = await ChatAsAsync(a, "");
        var live = await WaitAsync(started.Reader, "handler never started");
        live.Run!.Cancel("some_new_limit");
        var ev = await TerminalAsync(chat);
        Assert.Equal(
            (EventTypes.Error, "some_new_limit", "turn cut: some_new_limit", Codes.Internal),
            (ev.Type, ev.Reason, ev.Error, ev.ErrorStatus));
    }

    // The first cause of a run's end decides its terminal frame: a cut followed by a user
    // cancel stays a cut; a user cancel followed by a cut stays a user cancel.
    [Theory]
    [InlineData("cut then user cancel", Reasons.ProgressIdle, "", true)]
    [InlineData("user cancel then cut", "", Reasons.ProgressIdle, false)]
    public async Task FirstCauseDecidesReason(string name, string first, string second, bool wantCut) // cancel reasons ("" = user cancel)
    {
        var started = Channel.CreateUnbounded<bool>();
        var ended = Channel.CreateUnbounded<bool>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (a, _) = await StartRenewAgentAsync(
            new RenewAgentOptions
            {
                Handler = async (_, _, ct) =>
                {
                    started.Writer.TryWrite(true);
                    try
                    {
                        await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        // the run ended
                    }

                    ended.Writer.TryWrite(true);
                    await release.Task; // stays running so the second cancel reaches it
                    ct.ThrowIfCancellationRequested();
                },
            },
            name);
        var run = await ChatAsAsync(a, "");
        await WaitStarted(started);
        await CancelRun(run, first);
        await WaitAsync(ended.Reader, "handler never saw the run end");
        await CancelRun(run, second);
        release.SetResult();
        var ev = await TerminalAsync(run);
        if (wantCut)
        {
            Assert.Equal((EventTypes.Error, Reasons.ProgressIdle), (ev.Type, ev.Reason));
        }
        else
        {
            Assert.Equal((EventTypes.Done, StopReasons.Cancelled, null), (ev.Type, ev.StopReason, ev.Reason));
        }
    }
}
