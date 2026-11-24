using TransactRx.NatsAgent.Progress;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/progress/watcher_test.go. The Go fake clock (fires due timers synchronously
/// inside Advance) is a <see cref="TimerTrackingTimeProvider"/>.
/// </summary>
public sealed class ProgressWatcherTests
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Second = TimeSpan.FromSeconds(1);

    private static TimerTrackingTimeProvider NewFakeClock() => new(DateTimeOffset.FromUnixTimeSeconds(1_000_000));

    [Fact]
    public void ExpiresAfterWindow()
    {
        var clk = NewFakeClock();
        using var w = new ProgressWatcher(Window, clk);
        Assert.Equal(1, clk.PendingTimers); // (the tracker sees the watcher's timer)
        clk.Advance(Window - Second);
        Assert.False(w.IsExpired, "expired before window");
        Assert.False(w.Expired.IsCompleted, "expired before window");
        clk.Advance(Second);
        Assert.True(w.IsExpired, "not expired at window");
        Assert.True(w.Expired.IsCompletedSuccessfully, "not expired at window");
        Assert.Equal(0, clk.PendingTimers); // timer left pending after expiry
    }

    [Fact]
    public void PingDoesNotExtend()
    {
        var clk = NewFakeClock();
        using var w = new ProgressWatcher(Window, clk);
        var ping = new AgentEvent { Type = EventTypes.Ping };
        var windowSeconds = (int)Window.TotalSeconds;
        for (var i = 1; i <= 2 * windowSeconds; i++)
        {
            clk.Advance(Second);
            w.Observe(ping);
            Assert.True(w.IsExpired == (i >= windowSeconds), $"at {i}s expired={w.IsExpired}");
        }
    }

    [Fact]
    public void ProgressExtends()
    {
        var clk = NewFakeClock();
        using var w = new ProgressWatcher(Window, clk);
        clk.Advance(Window - Second);
        w.Observe(new AgentEvent { Type = EventTypes.Text, TextDelta = "x" });
        clk.Advance(Second);
        Assert.False(w.IsExpired, "expired at window despite progress");
        Assert.Equal(Second, w.Idle);
        clk.Advance(Window - (2 * Second)); // 2×window−2s
        Assert.False(w.IsExpired, "expired before 2×window−1s");
        clk.Advance(Second); // 2×window−1s
        Assert.True(w.IsExpired, "not expired at 2×window−1s");
    }

    [Fact]
    public void ZeroWindowNeverExpires()
    {
        var clk = NewFakeClock();
        var w = new ProgressWatcher(TimeSpan.Zero, clk);
        clk.Advance(TimeSpan.FromHours(24));
        Assert.False(w.IsExpired, "zero window expired");
        Assert.Equal(0, clk.PendingTimers); // zero window must not start a timer
        w.Stop();
    }

    [Fact]
    public void LastStage()
    {
        var clk = NewFakeClock();
        using var w = new ProgressWatcher(Window, clk);
        var payload = WireJson.ToElement(new ProgressPayload { Stage = "onedrive", Source = "files" });
        w.Observe(new AgentEvent { Type = EventTypes.Data, Kind = DataKinds.Progress, Payload = payload });
        Assert.Equal("onedrive", w.LastStage);
        w.Observe(new AgentEvent { Type = EventTypes.Text, TextDelta = "x" });
        Assert.Equal("onedrive", w.LastStage); // a text frame must not change the stage
        w.Touch("subagent");
        Assert.Equal("subagent", w.LastStage);
    }

    [Fact]
    public void StopIsIdempotent()
    {
        var clk = NewFakeClock();
        var w = new ProgressWatcher(Window, clk);
        Assert.Equal(1, clk.PendingTimers); // (the tracker sees the watcher's timer)
        w.Stop();
        w.Stop();
        Assert.Equal(0, clk.PendingTimers); // timer still pending after Stop
        clk.Advance(2 * Window);
        Assert.False(w.IsExpired, "stopped watcher expired");
        w.Touch("x");
        w.Observe(new AgentEvent { Type = EventTypes.Text });
    }

    // Real clock, concurrent callers: must be race-free and complete Expired once.
    [Fact]
    public async Task ConcurrentUse()
    {
        var w = new ProgressWatcher(TimeSpan.FromMilliseconds(20));
        var workers = new List<Task>();
        for (var i = 0; i < 8; i++)
        {
            workers.Add(Task.Run(() =>
            {
                for (var j = 0; j < 50; j++)
                {
                    switch (j % 3)
                    {
                        case 0:
                            w.Observe(new AgentEvent { Type = EventTypes.Text });
                            break;
                        case 1:
                            w.Touch("s");
                            break;
                        default:
                            _ = w.Idle;
                            _ = w.LastStage;
                            break;
                    }
                }
            }));
        }

        await Task.WhenAll(workers);
        try
        {
            await w.Expired.WaitAsync(TimeSpan.FromSeconds(2));
        }
        catch (TimeoutException)
        {
            Assert.Fail("never expired");
        }

        w.Stop();
        w.Stop();
    }
}
