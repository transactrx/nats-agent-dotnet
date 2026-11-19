using System.Threading.Channels;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Progress;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Client;

/// <summary>
/// The per-run state of a chat turn's reader loop. The idle decision lives here so every path
/// applies the same rule: the run is cut only when the watcher has expired, no progress frame
/// was handled since, and none is queued.
/// </summary>
internal sealed class StreamReader
{
    private readonly string _runId;
    private readonly TimeSpan _window;
    private readonly ChannelReader<NatsMsg<byte[]>> _msgs;
    private readonly ChannelWriter<AgentEvent> _out;

    // Builds every watcher of this run (test seam: a manual clock).
    private readonly Func<TimeSpan, ProgressWatcher> _newWatcher;

    // One-shot; Revive replaces it. Null without an idle window.
    private ProgressWatcher? _watcher;
    private long _lastFrame;
    private TimeProvider _time = TimeProvider.System;

    public StreamReader(
        string runId, TimeSpan window, ChannelReader<NatsMsg<byte[]>> msgs, ChannelWriter<AgentEvent> output,
        Func<TimeSpan, ProgressWatcher>? newWatcher = null)
    {
        _runId = runId;
        _window = window;
        _msgs = msgs;
        _out = output;
        _newWatcher = newWatcher ?? (w => new ProgressWatcher(w));
        if (window > TimeSpan.Zero)
        {
            _watcher = _newWatcher(window);
        }
    }

    /// <summary>Best-effort cancel publish.</summary>
    public Func<string?, ValueTask> CancelRemote { get; init; } = _ => ValueTask.CompletedTask;

    /// <summary>Test seam: runs just before a frame is shown to the watcher.</summary>
    public Action? BeforeObserve { get; set; }

    /// <summary>The current watcher (tests).</summary>
    internal ProgressWatcher? Watcher => _watcher;

    /// <summary>Releases the current watcher.</summary>
    public void Stop() => _watcher?.Stop();

    /// <summary>
    /// Pumps frames into the output until the terminal event, the dead-run timeout, an idle
    /// cut, or cancellation — each of which ends the output with exactly one terminal event.
    /// </summary>
    public async Task RunAsync(TimeProvider time, TimeSpan deadRunTimeout, CancellationToken ct)
    {
        _time = time;
        _lastFrame = time.GetTimestamp();
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = ct.Register(() => cancelled.TrySetResult());
        Task<bool>? pendingRead = null;

        while (true)
        {
            // Frames already queued are handled before any timer is consulted.
            while (!ct.IsCancellationRequested && _msgs.TryRead(out var msg))
            {
                if (await OnMsg(msg).ConfigureAwait(false))
                {
                    return;
                }
            }

            if (ct.IsCancellationRequested)
            {
                // Best-effort server-side cancel, then end the local stream.
                await CancelRemote(null).ConfigureAwait(false);
                await Terminate("context cancelled: the operation was canceled", null).ConfigureAwait(false);
                return;
            }

            var deadIn = deadRunTimeout - time.GetElapsedTime(_lastFrame);
            if (deadIn <= TimeSpan.Zero)
            {
                await Terminate($"run dead: no events for {GoDuration.FormatPrecise(deadRunTimeout)}", null).ConfigureAwait(false);
                return;
            }

            pendingRead ??= _msgs.WaitToReadAsync(CancellationToken.None).AsTask();
            using var wake = new CancellationTokenSource();
            var dead = Task.Delay(deadIn, time, wake.Token);
            var idle = _watcher?.Expired ?? Task.Delay(Timeout.InfiniteTimeSpan, wake.Token);
            var first = await Task.WhenAny(pendingRead, cancelled.Task, dead, idle).ConfigureAwait(false);
            wake.Cancel();

            if (first == pendingRead)
            {
                var more = await pendingRead.ConfigureAwait(false);
                pendingRead = null;
                if (!more)
                {
                    await Terminate("run ended: the stream subscription was closed", null).ConfigureAwait(false);
                    return;
                }
            }
            else if (first == idle && await OnIdle().ConfigureAwait(false))
            {
                return;
            }

            // cancellation and the dead-run timeout are decided at the top of the loop
        }
    }

    internal bool Expired => _watcher?.IsExpired ?? false;

    /// <summary>
    /// Replaces an expired watcher with a fresh one for the same window, keeping the last
    /// stage. The one place that turns "expired, but progress arrived" back into a live window.
    /// </summary>
    private void Revive()
    {
        var old = _watcher!;
        _watcher = _newWatcher(_window);
        if (old.LastStage.Length > 0)
        {
            _watcher.Touch(old.LastStage);
        }

        old.Stop();
    }

    private ValueTask Terminate(string message, string? reason) =>
        _out.WriteAsync(new AgentEvent
        {
            RunId = _runId,
            Type = EventTypes.Error,
            Error = message,
            ErrorStatus = Codes.Upstream,
            Reason = reason,
        });

    /// <summary>
    /// Handles one stream frame; true when the run is over (terminal delivered). Progress
    /// after the watcher expired revives it.
    /// </summary>
    internal async ValueTask<bool> OnMsg(NatsMsg<byte[]> msg)
    {
        if (!WireJson.TryDeserialize<AgentEvent>(msg.Data ?? [], out var ev))
        {
            return false;
        }

        if (!string.IsNullOrEmpty(ev.RunId) && ev.RunId != _runId)
        {
            return false; // stray traffic on a shared stream subject
        }

        _lastFrame = _time.GetTimestamp();
        if (_watcher is not null)
        {
            BeforeObserve?.Invoke();
            _watcher.Observe(ev);

            // Checked after Observe: the window may end right before it, and Observe is then
            // a no-op on the expired watcher.
            if (ev.IsProgress && Expired)
            {
                Revive();
                _watcher.Observe(ev);
            }
        }

        if (ev.Type == EventTypes.Ping)
        {
            return false;
        }

        await _out.WriteAsync(ev).ConfigureAwait(false);
        return ev.IsTerminal;
    }

    /// <summary>
    /// Runs when the watcher expired. Frames already queued arrived in time (the reader was
    /// slow, not the agent), so they are handled first; a progress frame among them revives
    /// the watcher and the run goes on, a terminal ends it normally. Only with nothing (or
    /// only pings) queued is the run cut. True when the run is over.
    /// </summary>
    internal async ValueTask<bool> OnIdle()
    {
        while (Expired)
        {
            if (_msgs.TryRead(out var msg))
            {
                if (await OnMsg(msg).ConfigureAwait(false))
                {
                    return true;
                }

                continue;
            }

            var message = $"run idle: no progress for {GoDuration.FormatPrecise(_window)}";
            if (_watcher!.LastStage is { Length: > 0 } stage)
            {
                message += " (last stage: " + stage + ")";
            }

            await CancelRemote(Reasons.ProgressIdle).ConfigureAwait(false);
            await Terminate(message, Reasons.ProgressIdle).ConfigureAwait(false);
            return true;
        }

        return false;
    }
}
