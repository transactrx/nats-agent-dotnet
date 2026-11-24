using Microsoft.Extensions.Time.Testing;

namespace TransactRx.NatsAgent.Tests.Support;

/// <summary>
/// A <see cref="FakeTimeProvider"/> that also tells a test which timers are pending — the
/// counterpart of the <c>pending()</c> / <c>pendingFuncs()</c> / <c>fireNext()</c> helpers of
/// the Go suites' fake clocks. Nothing sleeps for real: <see cref="FakeTimeProvider.Advance"/>
/// fires due timers synchronously.
/// <para>
/// Two kinds of timer are told apart, as the Go fake clocks do:
/// a <b>delay</b> is a <c>Task.Delay(d, timeProvider, …)</c> somebody is awaiting (Go's
/// <c>clock.After</c>: the code under test is asleep), everything else is a <b>callback</b>
/// timer created with <see cref="TimeProvider.CreateTimer"/> (Go's <c>clock.AfterFunc</c>).
/// </para>
/// </summary>
public sealed class TimerTrackingTimeProvider(DateTimeOffset start) : FakeTimeProvider(start)
{
    private readonly object _gate = new();
    private readonly List<Tracked> _timers = [];

    /// <summary>Timers that will still fire: armed, not disposed, and (one-shot) not fired yet.</summary>
    public int PendingTimers => Count(_ => true);

    /// <summary>Pending <c>Task.Delay</c>s on this provider (Go: <c>pending()</c>).</summary>
    public int PendingDelays => Count(t => t.IsDelay);

    /// <summary>Pending timers that are not a <c>Task.Delay</c> (Go: <c>pendingFuncs()</c>).</summary>
    public int PendingCallbacks => Count(t => !t.IsDelay);

    /// <summary>
    /// Advances to the earliest pending delay's deadline (Go: <c>fireNext()</c>); with none
    /// pending the clock does not move.
    /// </summary>
    public void FireNextDelay()
    {
        DateTimeOffset? next = null;
        lock (_gate)
        {
            foreach (var t in _timers)
            {
                if (t.IsDelay && t.DueAt is { } at && (next is null || at < next))
                {
                    next = at;
                }
            }
        }

        var now = GetUtcNow();
        Advance(next is { } due && due > now ? due - now : TimeSpan.Zero);
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        // Task.Delay(…, TimeProvider, …) passes its own promise (a Task) as the timer state.
        var tracked = new Tracked(this, callback, isDelay: state is Task);
        var now = GetUtcNow();
        lock (_gate)
        {
            tracked.Arm(now, dueTime, period);
        }

        // A zero due time fires inside this call, before Inner is known.
        tracked.Attach(base.CreateTimer(tracked.Invoke, state, dueTime, period));

        // Counted as pending only once the fake clock really holds the timer: a test that sees
        // it pending and advances the clock must not get ahead of the registration.
        lock (_gate)
        {
            if (!tracked.IsDisposed)
            {
                _timers.Add(tracked);
            }
        }

        return tracked;
    }

    private int Count(Func<Tracked, bool> which)
    {
        lock (_gate)
        {
            return _timers.Count(t => t.DueAt is not null && which(t));
        }
    }

    private sealed class Tracked(TimerTrackingTimeProvider owner, TimerCallback callback, bool isDelay) : ITimer
    {
        private ITimer? _inner;
        private TimeSpan _period;
        private bool _disposed;

        public bool IsDelay => isDelay;

        // Read under the owner's gate.
        public bool IsDisposed => _disposed;

        /// <summary>When the timer fires next; null when it will not (fired, stopped, disposed).</summary>
        public DateTimeOffset? DueAt { get; private set; }

        // Called under the owner's gate.
        public void Arm(DateTimeOffset now, TimeSpan dueTime, TimeSpan period)
        {
            _period = period;
            DueAt = dueTime == Timeout.InfiniteTimeSpan ? null : now + dueTime;
        }

        public void Attach(ITimer inner)
        {
            bool disposed;
            lock (owner._gate)
            {
                _inner = inner;
                disposed = _disposed;
            }

            if (disposed)
            {
                inner.Dispose();
            }
        }

        public void Invoke(object? state)
        {
            lock (owner._gate)
            {
                // A one-shot timer is spent before its callback runs, so a callback that re-arms
                // it (Change) leaves it pending again.
                if (DueAt is { } at)
                {
                    DueAt = _period > TimeSpan.Zero ? at + _period : null;
                }
            }

            callback(state);
        }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            var now = owner.GetUtcNow();
            ITimer? inner;
            lock (owner._gate)
            {
                if (_disposed)
                {
                    return false;
                }

                Arm(now, dueTime, period);
                inner = _inner;
            }

            return inner?.Change(dueTime, period) ?? false;
        }

        public void Dispose()
        {
            ITimer? inner;
            lock (owner._gate)
            {
                _disposed = true;
                DueAt = null;
                owner._timers.Remove(this);
                inner = _inner;
            }

            inner?.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
