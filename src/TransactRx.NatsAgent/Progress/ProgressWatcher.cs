using System.Text.Json;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Progress;

/// <summary>
/// Watches a run's event stream for progress and signals when a window passes without any.
/// Only progress extends the window: pings prove liveness, not progress, and never reset it.
/// <para>
/// Safe for concurrent use. Call <see cref="Stop"/> (or dispose) when the run ends; read
/// <see cref="Idle"/> and <see cref="LastStage"/> before that.
/// </para>
/// </summary>
public sealed class ProgressWatcher : IDisposable
{
    private readonly TimeSpan _window;
    private readonly TimeProvider _time;
    private readonly TaskCompletionSource _expired = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly object _gate = new();
    private long _last;
    private string _stage = "";
    private ITimer? _timer;
    private bool _done; // expired or stopped: no timer pending, no further updates

    /// <summary>Starts a watcher. A window of zero or less never expires.</summary>
    public ProgressWatcher(TimeSpan window, TimeProvider? timeProvider = null)
    {
        _window = window;
        _time = timeProvider ?? TimeProvider.System;
        _last = _time.GetTimestamp();
        if (window > TimeSpan.Zero)
        {
            lock (_gate)
            {
                _timer = _time.CreateTimer(_ => Fire(), null, window, Timeout.InfiniteTimeSpan);
            }
        }
    }

    /// <summary>
    /// Completes once when the window passes with no progress. It never completes after
    /// <see cref="Stop"/> or when the window is zero or less.
    /// </summary>
    public Task Expired => _expired.Task;

    /// <summary>Whether the window has passed with no progress.</summary>
    public bool IsExpired => _expired.Task.IsCompleted;

    /// <summary>The time since the last progress (or since the watcher started).</summary>
    public TimeSpan Idle
    {
        get
        {
            lock (_gate)
            {
                return _time.GetElapsedTime(_last);
            }
        }
    }

    /// <summary>The stage of the latest progress event or <see cref="Touch"/>, if any.</summary>
    public string LastStage
    {
        get
        {
            lock (_gate)
            {
                return _stage;
            }
        }
    }

    /// <summary>
    /// Records <paramref name="ev"/> as progress when it is progress; a progress data event
    /// also sets the stage.
    /// </summary>
    public void Observe(AgentEvent? ev)
    {
        if (ev is null || !ev.IsProgress)
        {
            return;
        }

        var stage = "";
        if (ev.Type == EventTypes.Data && ev.Kind == DataKinds.Progress && ev.Payload is { ValueKind: JsonValueKind.Object } payload)
        {
            try
            {
                stage = payload.Deserialize<ProgressPayload>(WireJson.Options)?.Stage ?? "";
            }
            catch (JsonException)
            {
                // malformed payload: still progress, no stage
            }
        }

        Touch(stage);
    }

    /// <summary>
    /// Records progress from a source other than a stream frame. An empty stage keeps the
    /// previous one.
    /// </summary>
    public void Touch(string? stage = null)
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            _last = _time.GetTimestamp();
            if (!string.IsNullOrEmpty(stage))
            {
                _stage = stage;
            }
        }
    }

    /// <summary>Releases the timer. Safe to call more than once and after expiry.</summary>
    public void Stop()
    {
        lock (_gate)
        {
            _done = true;
            _timer?.Dispose();
            _timer = null;
        }
    }

    public void Dispose() => Stop();

    // Runs when the latest deadline passes. Progress does not reset the timer; this re-arms
    // it for the remainder of the window instead.
    private void Fire()
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            var idle = _time.GetElapsedTime(_last);
            if (idle >= _window)
            {
                _done = true;
                _timer?.Dispose();
                _timer = null;
                _expired.TrySetResult();
                return;
            }

            _timer?.Change(_window - idle, Timeout.InfiniteTimeSpan);
        }
    }
}
