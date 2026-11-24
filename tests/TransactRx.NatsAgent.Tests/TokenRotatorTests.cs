using System.Diagnostics;
using System.Threading.Channels;
using TransactRx.NatsAgent.Rotation;
using TransactRx.NatsAgent.Tests.Support;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/rotation/rotator_test.go. The Go fake clock is a
/// <see cref="TimerTrackingTimeProvider"/>: nothing sleeps for real, and a test advances the
/// clock only once the rotator is seen waiting on it (a pending <c>Task.Delay</c> = Go's
/// <c>clock.After</c> waiter; the per-attempt give-up bound = Go's <c>clock.AfterFunc</c>).
/// <para>
/// One step of the .NET clock is a tick (100ns) where the Go tests use a nanosecond.
/// </para>
/// </summary>
public sealed class TokenRotatorTests : IAsyncLifetime
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.FromUnixTimeSeconds(1_000_000);
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);
    private static readonly TimeSpan FiveSeconds = TimeSpan.FromSeconds(5);

    private readonly List<Harness> _harnesses = [];

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    // Go: the t.Cleanup of harness.start — cancel and give the run a moment to return.
    public async ValueTask DisposeAsync()
    {
        foreach (var h in _harnesses)
        {
            h.Cancel();
            if (h.Done is { } done)
            {
                try
                {
                    await done.WaitAsync(FiveSeconds);
                }
                catch (Exception)
                {
                    // the test already looked at the outcome
                }
            }
        }
    }

    private static TimeSpan Min(double minutes) => TimeSpan.FromMinutes(minutes);

    private static TimeSpan Sec(double seconds) => TimeSpan.FromSeconds(seconds);

    private static RotationToken FirstToken() => new() { Value = "tok0", ExpiresAt = T0 + Min(15), RunSessionId = "rs1" };

    // Collects the clock times of Redeem/Push/Retire calls.
    private sealed class Recorder
    {
        private readonly object _gate = new();
        private readonly List<TimeSpan> _redeems = [];
        private readonly List<RotationToken> _pushes = [];
        private readonly List<TimeSpan> _pushAt = [];
        private readonly List<RotationToken> _retired = [];
        private readonly List<TimeSpan> _retAt = [];

        public int AddRedeem(TimeSpan at)
        {
            lock (_gate)
            {
                _redeems.Add(at);
                return _redeems.Count;
            }
        }

        public int AddPush(RotationToken token, TimeSpan at)
        {
            lock (_gate)
            {
                _pushes.Add(token);
                _pushAt.Add(at);
                return _pushes.Count;
            }
        }

        public void AddRetire(RotationToken token, TimeSpan at)
        {
            lock (_gate)
            {
                _retired.Add(token);
                _retAt.Add(at);
            }
        }

        public Snapshot Snapshot()
        {
            lock (_gate)
            {
                return new Snapshot([.. _redeems], [.. _pushes], [.. _pushAt], [.. _retired], [.. _retAt]);
            }
        }
    }

    private sealed record Snapshot(TimeSpan[] Redeems, RotationToken[] Pushes, TimeSpan[] PushAt, RotationToken[] Retired, TimeSpan[] RetAt);

    private sealed class Harness
    {
        private readonly CancellationTokenSource _cts = new();
        private int _minted;

        // Builds a config whose Redeem returns tokens living `life` from now; redeemErrs /
        // pushErrs fail the first N calls (or every call when < 0).
        public Harness(TimeSpan life, int redeemErrs, int pushErrs)
        {
            Config = new RotationConfig
            {
                Ttl = Min(15),
                RotateBefore = Min(8),
                GiveUpBefore = Min(2),
                Redeem = (_, _) =>
                {
                    var now = Clock.GetUtcNow();
                    var count = Rec.AddRedeem(now - T0);
                    if (redeemErrs < 0 || count <= redeemErrs)
                    {
                        return Task.FromException<RotationToken>(new InvalidOperationException("identity down"));
                    }

                    var n = Interlocked.Increment(ref _minted);
                    return Task.FromResult(new RotationToken { Value = "tok" + n, ExpiresAt = now + life, RunSessionId = "rs1" });
                },
                Push = (tok, _) =>
                {
                    var now = Clock.GetUtcNow();
                    var count = Rec.AddPush(tok, now - T0);
                    if (pushErrs < 0 || count <= pushErrs)
                    {
                        return Task.FromException(new InvalidOperationException("agent unreachable"));
                    }

                    Pushed.Writer.TryWrite(tok);
                    return Task.CompletedTask;
                },
            };
        }

        public TimerTrackingTimeProvider Clock { get; } = new(T0);

        public Recorder Rec { get; } = new();

        public RotationConfig Config { get; set; }

        public Channel<RotationToken> Pushed { get; } = Channel.CreateUnbounded<RotationToken>();

        /// <summary>Completes when RunAsync returned (or threw); null before <see cref="Start"/>.</summary>
        public Task? Done { get; private set; }

        /// <summary>How far the fake clock is from t0.</summary>
        public TimeSpan Elapsed => Clock.GetUtcNow() - T0;

        public void Start(RotationToken first)
        {
            var (config, clock, ct) = (Config, Clock, _cts.Token);
            Done = Task.Run(() => TokenRotator.RunAsync(config, first, ct, clock));
        }

        public void Cancel() => _cts.Cancel();

        // Waits (in real time, briefly) until n delays are pending on the clock — the rotator
        // (and its retires) are asleep — or the run ended; false when the run ended first.
        public async Task<bool> WaitPending(int n)
        {
            var started = Stopwatch.GetTimestamp();
            while (Clock.PendingDelays < n)
            {
                if (Done!.IsCompleted)
                {
                    return false;
                }

                if (Stopwatch.GetElapsedTime(started) > FiveSeconds)
                {
                    Assert.Fail($"rotator never waited (pending {Clock.PendingDelays}, want {n})");
                }

                await Task.Delay(1);
            }

            return true;
        }

        public async Task<RotationToken> WaitPushed()
        {
            try
            {
                return await Pushed.Reader.ReadAsync().AsTask().WaitAsync(FiveSeconds);
            }
            catch (TimeoutException)
            {
                Assert.Fail("no successful push");
                throw;
            }
        }

        // Cancels and waits for the run; rethrows what RunAsync threw (Go: returns Run's error).
        public async Task Stop()
        {
            Cancel();
            Assert.True(await Returned(FiveSeconds), "Run did not return after cancel");
            await Done!;
        }

        /// <summary>Whether the run returned (or threw) within <paramref name="timeout"/>.</summary>
        public async Task<bool> Returned(TimeSpan timeout) =>
            await Task.WhenAny(Done!, Task.Delay(timeout)) == Done;
    }

    private Harness NewHarness(TimeSpan life, int redeemErrs, int pushErrs)
    {
        var h = new Harness(life, redeemErrs, pushErrs);
        _harnesses.Add(h);
        return h;
    }

    [Fact]
    public async Task RotatesAtThreshold()
    {
        var h = NewHarness(Min(15), 0, 0);
        h.Start(FirstToken());
        await h.WaitPending(1);
        h.Clock.Advance(Min(7) - Tick);
        Assert.Empty(h.Rec.Snapshot().Redeems); // redeemed early
        h.Clock.Advance(Tick);
        var tok = await h.WaitPushed();
        var s = h.Rec.Snapshot();
        Assert.Equal([Min(7)], s.Redeems);
        Assert.Single(s.Pushes);
        Assert.Equal("tok1", tok.Value);
        Assert.Equal(Min(7), s.PushAt[0]);
        Assert.Equal(T0 + Min(22), tok.ExpiresAt);
        await h.Stop(); // Run = nil
    }

    // Identity may ignore the requested TTL: the next rotation follows the expiry it
    // returned, not the config's TTL.
    [Fact]
    public async Task SchedulesFromReturnedExpiry()
    {
        var h = NewHarness(Min(60), 0, 0);
        h.Start(FirstToken());
        await h.WaitPending(1);
        h.Clock.Advance(Min(7));
        await h.WaitPushed();
        await h.WaitPending(1);
        h.Clock.Advance(Min(52) - Tick);
        Assert.Single(h.Rec.Snapshot().Redeems); // second redeem too early
        h.Clock.Advance(Tick);
        await h.WaitPushed();
        Assert.Equal([Min(7), Min(59)], h.Rec.Snapshot().Redeems);
        await h.Stop();
    }

    [Fact]
    public async Task RetriesThenSucceeds()
    {
        var h = NewHarness(Min(15), 2, 0);
        h.Start(FirstToken());
        for (var i = 0; i < 3; i++)
        {
            await h.WaitPending(1);
            h.Clock.FireNextDelay();
        }

        await h.WaitPushed();
        var s = h.Rec.Snapshot();
        Assert.Equal([Min(7), Min(7) + Sec(5), Min(7) + Sec(15)], s.Redeems);
        Assert.Single(s.Pushes);
        await h.Stop();
    }

    [Fact]
    public async Task GivesUp()
    {
        var h = NewHarness(Min(15), -1, 0);
        h.Start(FirstToken());
        while (await h.WaitPending(1))
        {
            h.Clock.FireNextDelay();
        }

        await Assert.ThrowsAsync<TokenRotationFailedException>(() => h.Done!);
        Assert.Equal(Min(13), h.Elapsed); // gave up at expiry - 2m
        var s = h.Rec.Snapshot();

        // 7:00, then backoff 5s, 10s, 20s, 30s (cap)… until 13:00.
        TimeSpan[] want = [Min(7), Min(7) + Sec(5), Min(7) + Sec(15), Min(7) + Sec(35)];
        Assert.True(s.Redeems.Length >= want.Length, $"only {s.Redeems.Length} redeems");
        for (var i = 0; i < want.Length; i++)
        {
            Assert.True(s.Redeems[i] == want[i], $"redeem {i} at {s.Redeems[i]}, want {want[i]}");
        }

        for (var i = want.Length; i < s.Redeems.Length; i++)
        {
            var d = s.Redeems[i] - s.Redeems[i - 1];
            Assert.True(d == Sec(30), $"backoff {i} = {d}, want 30s cap");
        }

        Assert.True(s.Redeems[^1] < Min(13), $"redeemed at {s.Redeems[^1]}, after give-up point");
        Assert.Empty(s.Pushes);
    }

    // A token that does not outlive the current one is a failed attempt.
    [Fact]
    public async Task StaleTokenCountsAsFailure()
    {
        var h = NewHarness(Min(15), 0, 0);
        var calls = 0;
        h.Config = h.Config with
        {
            Redeem = (_, _) =>
            {
                calls++;
                return Task.FromResult(calls == 1
                    ? new RotationToken { Value = "stale", ExpiresAt = T0 + Min(15) }
                    : new RotationToken { Value = "fresh", ExpiresAt = h.Clock.GetUtcNow() + Min(15) });
            },
        };
        h.Start(FirstToken());
        await h.WaitPending(1);
        h.Clock.FireNextDelay();
        await h.WaitPending(1);
        h.Clock.FireNextDelay();
        var tok = await h.WaitPushed();
        Assert.Equal("fresh", tok.Value);
        Assert.Equal(2, Volatile.Read(ref calls));
        await h.Stop();
    }

    // A failed push keeps the redeemed token and retries delivering it.
    [Fact]
    public async Task PushFailureRetried()
    {
        var h = NewHarness(Min(15), 0, 1);
        h.Start(FirstToken());
        await h.WaitPending(1);
        h.Clock.FireNextDelay();
        await h.WaitPending(1);
        h.Clock.FireNextDelay();
        var tok = await h.WaitPushed();
        var s = h.Rec.Snapshot();
        Assert.Equal([Min(7), Min(7) + Sec(5)], s.PushAt);
        Assert.Equal(2, s.Pushes.Length);
        Assert.Equal(s.Pushes[0].Value, s.Pushes[1].Value); // a retried push must not change the token
        Assert.Equal("tok1", tok.Value);
        Assert.Single(s.Redeems);
        await h.Stop();
    }

    [Fact]
    public async Task RetireCalledAfterOverlap()
    {
        var h = NewHarness(Min(60), 0, 0);
        var retired = Channel.CreateUnbounded<RotationToken>();
        h.Config = h.Config with
        {
            Overlap = Min(8),
            Retire = (old, _) =>
            {
                h.Rec.AddRetire(old, h.Clock.GetUtcNow() - T0);
                retired.Writer.TryWrite(old);
                return Task.CompletedTask;
            },
        };
        h.Start(FirstToken());
        await h.WaitPending(1);
        h.Clock.Advance(Min(7));
        await h.WaitPushed();

        // Next rotation (59m) and the retire (15m) are both pending.
        await h.WaitPending(2);
        h.Clock.Advance(Min(8) - Tick);
        Assert.Empty(h.Rec.Snapshot().Retired); // retired before overlap
        h.Clock.Advance(Tick);
        try
        {
            var old = await retired.Reader.ReadAsync().AsTask().WaitAsync(FiveSeconds);
            Assert.Equal("tok0", old.Value);
        }
        catch (TimeoutException)
        {
            Assert.Fail("Retire not called after overlap");
        }

        var s = h.Rec.Snapshot();
        Assert.Single(s.Retired);
        Assert.Equal(Min(15), s.RetAt[0]);
        await h.Stop();
        Assert.Single(h.Rec.Snapshot().Retired); // Retire called once
    }

    [Fact]
    public async Task StopsOnContextCancel()
    {
        var h = NewHarness(Min(15), 0, 0);
        h.Start(FirstToken());
        await h.WaitPending(1);
        await h.Stop(); // Run = nil
        Assert.Empty(h.Rec.Snapshot().Redeems); // redeemed after cancel
    }

    [Fact]
    public async Task FirstWithoutExpiry()
    {
        var h = NewHarness(Min(15), 0, 0);

        // Run must not accept a first token without expiry (real clock, as in Go).
        await Assert.ThrowsAsync<ArgumentException>(() => TokenRotator.RunAsync(h.Config, new RotationToken { Value = "tok0" }));
    }

    // A Redeem that hangs is cut at the give-up point; Run fails then instead of blocking
    // past it.
    [Fact]
    public async Task HungRedeemGivesUp()
    {
        var h = NewHarness(Min(15), 0, 0);
        var attemptEnded = Channel.CreateUnbounded<bool>();
        h.Config = h.Config with
        {
            Redeem = async (_, ct) =>
            {
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                finally
                {
                    attemptEnded.Writer.TryWrite(ct.IsCancellationRequested);
                }

                throw new UnreachableException();
            },
        };
        h.Start(FirstToken());
        await h.WaitPending(1);
        h.Clock.Advance(Min(7));

        // The attempt's give-up bound is armed once the rotator is inside Redeem.
        var started = Stopwatch.GetTimestamp();
        while (h.Clock.PendingCallbacks == 0 && Stopwatch.GetElapsedTime(started) < Sec(2))
        {
            await Task.Delay(1);
        }

        Assert.True(h.Clock.PendingCallbacks > 0, "the hung attempt has no give-up bound on the clock");
        h.Clock.Advance(Min(6)); // reach expiry - GiveUpBefore
        Assert.True(await h.Returned(Sec(2)), "Run still blocked in a hung Redeem past the give-up point");
        await Assert.ThrowsAsync<TokenRotationFailedException>(() => h.Done!);
        Assert.True(await attemptEnded.Reader.ReadAsync().AsTask().WaitAsync(FiveSeconds), "Redeem token not cancelled");
        Assert.Equal(Min(13), h.Elapsed);
    }

    // Run returns only after a Retire in progress has finished; a retire whose overlap has
    // not elapsed is dropped.
    [Fact]
    public async Task RunWaitsForRetires()
    {
        var h = NewHarness(Min(60), 0, 0);
        var started = Channel.CreateUnbounded<RotationToken>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Config = h.Config with
        {
            Overlap = Min(8),
            Retire = async (old, _) =>
            {
                started.Writer.TryWrite(old);
                await release.Task;
            },
        };
        try
        {
            h.Start(FirstToken());
            await h.WaitPending(1);
            h.Clock.Advance(Min(7)); // rotate tok0 → tok1
            await h.WaitPushed();
            await h.WaitPending(2);
            h.Clock.Advance(Min(8)); // tok0 overlap ends
            try
            {
                var old = await started.Reader.ReadAsync().AsTask().WaitAsync(FiveSeconds);
                Assert.Equal("tok0", old.Value);
            }
            catch (TimeoutException)
            {
                Assert.Fail("Retire not started");
            }

            await h.WaitPending(1);
            h.Clock.Advance(Min(44)); // 59m: rotate tok1 → tok2
            await h.WaitPushed();
            await h.WaitPending(2); // next rotation + tok1 retire (67m)

            h.Cancel();
            Assert.False(await h.Returned(TimeSpan.FromMilliseconds(100)), "Run returned while a Retire was still running");
            release.SetResult();
            Assert.True(await h.Returned(FiveSeconds), "Run did not return after the Retire finished");
            await h.Done!; // Run = nil
            Assert.False(started.Reader.TryRead(out var dropped), $"pending retire of {dropped?.Value} not dropped");
        }
        finally
        {
            release.TrySetResult();
        }
    }

    // On give-up, a token that was redeemed but never delivered is retired once.
    [Fact]
    public async Task GiveUpRetiresUndeliveredToken()
    {
        var h = NewHarness(Min(15), 0, -1);
        var retired = new List<RotationToken>();
        h.Config = h.Config with
        {
            Retire = (old, _) =>
            {
                lock (retired)
                {
                    retired.Add(old);
                }

                return Task.CompletedTask;
            },
        };
        h.Start(FirstToken());
        while (await h.WaitPending(1))
        {
            h.Clock.FireNextDelay();
        }

        await Assert.ThrowsAsync<TokenRotationFailedException>(() => h.Done!);
        lock (retired)
        {
            // the undelivered tok1, once
            Assert.Equal("tok1", Assert.Single(retired).Value);
        }

        Assert.Single(h.Rec.Snapshot().Redeems);
    }

    [Theory]
    [InlineData("nil Redeem")]
    [InlineData("nil Push")]
    [InlineData("RotateBefore == GiveUpBefore")]
    [InlineData("RotateBefore < GiveUpBefore")]
    [InlineData("negative GiveUpBefore")]
    [InlineData("zero TTL")]
    public async Task InvalidConfig(string name)
    {
        var h = NewHarness(Min(15), 0, 0);
        h.Config = name switch
        {
            "nil Redeem" => h.Config with { Redeem = null },
            "nil Push" => h.Config with { Push = null },
            "RotateBefore == GiveUpBefore" => h.Config with { GiveUpBefore = h.Config.RotateBefore },
            "RotateBefore < GiveUpBefore" => h.Config with { RotateBefore = Min(1) },
            "negative GiveUpBefore" => h.Config with { GiveUpBefore = -Sec(1) },
            "zero TTL" => h.Config with { Ttl = TimeSpan.Zero },
            _ => throw new ArgumentOutOfRangeException(nameof(name), name, "unknown case"),
        };
        h.Start(FirstToken());
        Assert.True(await h.Returned(Sec(2)), "Run did not reject the config immediately");

        // A config error (ArgumentException), not a failed rotation.
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => h.Done!);
        Assert.IsNotType<TokenRotationFailedException>(ex);
        Assert.IsType<ArgumentException>(ex);
        Assert.Empty(h.Rec.Snapshot().Redeems);
    }
}
