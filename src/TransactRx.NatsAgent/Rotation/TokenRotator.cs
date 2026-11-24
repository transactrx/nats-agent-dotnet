using TransactRx.NatsAgent.Internal;

namespace TransactRx.NatsAgent.Rotation;

/// <summary>One delegation token and when it expires.</summary>
public sealed record RotationToken
{
    public string Value { get; init; } = "";

    public DateTimeOffset ExpiresAt { get; init; }

    public string RunSessionId { get; init; } = "";

    // The token value never appears in logs through an accidental ToString.
    public override string ToString() => $"RotationToken {{ ExpiresAt = {ExpiresAt:O}, RunSessionId = {RunSessionId} }}";
}

/// <summary>Drives <see cref="TokenRotator.RunAsync"/>. Redeem and Push are required.</summary>
public sealed record RotationConfig
{
    /// <summary>Requested token life.</summary>
    public TimeSpan Ttl { get; init; }

    /// <summary>Rotate when the remaining life is at most this (8m).</summary>
    public TimeSpan RotateBefore { get; init; }

    /// <summary>Stop retrying when the current token has this left (2m).</summary>
    public TimeSpan GiveUpBefore { get; init; }

    /// <summary>Obtains a new token of the requested life.</summary>
    public Func<TimeSpan, CancellationToken, Task<RotationToken>>? Redeem { get; init; }

    /// <summary>Delivers a token to the agent (renew, with the token's expiry).</summary>
    public Func<RotationToken, CancellationToken, Task>? Push { get; init; }

    /// <summary>Called once the overlap of a replaced token ended; optional.</summary>
    public Func<RotationToken, CancellationToken, Task>? Retire { get; init; }

    /// <summary>How long the old token must stay untouched (= RotateBefore).</summary>
    public TimeSpan Overlap { get; init; }
}

/// <summary>
/// No new token could be redeemed and pushed before the give-up point; the inner exception is
/// the last failure.
/// </summary>
public sealed class TokenRotationFailedException(string message, Exception? inner = null)
    : Exception("token rotation failed: " + message, inner);

/// <summary>
/// Keeps a long run supplied with short-lived tokens: it redeems a new token before the
/// current one runs low, pushes it to the agent, and retires the old one once it is no longer
/// needed. It knows nothing about NATS or identity; every side effect goes through
/// <see cref="RotationConfig"/>.
/// </summary>
public static class TokenRotator
{
    // Retry backoff after a failed Redeem or Push: 5s, 10s, 20s, then 30s.
    private static readonly TimeSpan BackoffFirst = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Rotates <paramref name="first"/> and its successors until
    /// <paramref name="cancellationToken"/> is cancelled (returns normally) or a rotation
    /// fails (<see cref="TokenRotationFailedException"/>). <paramref name="first"/> is not
    /// pushed: the agent already has it. Each rotation is scheduled from the expiry the
    /// previous Redeem returned, never from the TTL. A redeemed token that does not outlive
    /// the current one counts as a failed attempt. Each Redeem/Push attempt's token is
    /// cancelled at the give-up point. The task completes only after every Retire it started
    /// has returned; retires still waiting out their overlap are dropped.
    /// </summary>
    /// <exception cref="ArgumentException">The config or first token is invalid (reported at once).</exception>
    public static async Task RunAsync(RotationConfig config, RotationToken first, CancellationToken cancellationToken = default, TimeProvider? timeProvider = null)
    {
        Validate(config);
        if (first.ExpiresAt == default)
        {
            throw new ArgumentException("rotation: first token has no expiry", nameof(first));
        }

        var time = timeProvider ?? TimeProvider.System;

        // Pending retires end with the run; running ones are waited for.
        var retires = new List<Task>();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var ct = cts.Token;
        try
        {
            var current = first;
            while (true)
            {
                if (!await Sleep(time, current.ExpiresAt - time.GetUtcNow() - config.RotateBefore, ct).ConfigureAwait(false))
                {
                    return;
                }

                RotationToken next;
                try
                {
                    next = await Rotate(config, time, current, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (TokenRotationFailedException) when (ct.IsCancellationRequested)
                {
                    return;
                }

                if (config.Retire is not null)
                {
                    var old = current;
                    retires.Add(Task.Run(() => RetireAfterOverlap(config, time, old, ct), CancellationToken.None));
                }

                current = next;
            }
        }
        finally
        {
            cts.Cancel();
            await Task.WhenAll(retires).ConfigureAwait(false);
        }
    }

    // Redeems and pushes one successor of current, retrying with backoff until
    // current.ExpiresAt - GiveUpBefore.
    private static async Task<RotationToken> Rotate(RotationConfig config, TimeProvider time, RotationToken current, CancellationToken ct)
    {
        var giveUp = current.ExpiresAt - config.GiveUpBefore;
        var backoff = BackoffFirst;
        RotationToken? pending = null; // redeemed but not yet pushed
        while (true)
        {
            Exception? error = null;
            using (var attempt = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                // The attempt ends at the give-up point.
                var untilGiveUp = giveUp - time.GetUtcNow();
                using var stop = time.CreateTimer(
                    _ =>
                    {
                        try
                        {
                            attempt.Cancel();
                        }
                        catch (ObjectDisposedException)
                        {
                            // the attempt finished at the very moment the give-up point passed
                        }
                    },
                    null, untilGiveUp > TimeSpan.Zero ? untilGiveUp : TimeSpan.Zero, Timeout.InfiniteTimeSpan);
                try
                {
                    if (pending is null)
                    {
                        var redeemed = await config.Redeem!(config.Ttl, attempt.Token).ConfigureAwait(false);
                        if (redeemed.ExpiresAt <= current.ExpiresAt)
                        {
                            throw new InvalidOperationException(
                                $"redeemed token expires {redeemed.ExpiresAt:O}, not after current {current.ExpiresAt:O}");
                        }

                        pending = redeemed;
                    }

                    await config.Push!(pending, attempt.Token).ConfigureAwait(false);
                    return pending;
                }
                catch (Exception ex)
                {
                    error = ex;
                }
            }

            ct.ThrowIfCancellationRequested();

            var left = giveUp - time.GetUtcNow();
            if (left <= TimeSpan.Zero)
            {
                await Failed().ConfigureAwait(false);
            }

            if (!await Sleep(time, backoff < left ? backoff : left, ct).ConfigureAwait(false))
            {
                throw new OperationCanceledException(ct);
            }

            if (time.GetUtcNow() >= giveUp)
            {
                await Failed().ConfigureAwait(false);
            }

            backoff = backoff * 2 < BackoffMax ? backoff * 2 : BackoffMax;

            async Task Failed()
            {
                if (pending is not null && config.Retire is not null)
                {
                    try
                    {
                        await config.Retire(pending, ct).ConfigureAwait(false); // redeemed, never delivered
                    }
                    catch (Exception)
                    {
                        // Retire is best-effort cleanup; the rotation failure is what is reported.
                    }
                }

                throw new TokenRotationFailedException(error!.Message, error);
            }
        }
    }

    // Reports a config RunAsync cannot work with.
    private static void Validate(RotationConfig config)
    {
        if (config.Redeem is null || config.Push is null)
        {
            throw new ArgumentException("rotation: Redeem and Push are required");
        }

        if (config.Ttl <= TimeSpan.Zero)
        {
            throw new ArgumentException($"rotation: TTL must be > 0 (got {GoDuration.FormatPrecise(config.Ttl)})");
        }

        if (config.GiveUpBefore < TimeSpan.Zero)
        {
            throw new ArgumentException($"rotation: GiveUpBefore must be >= 0 (got {config.GiveUpBefore})");
        }

        if (config.RotateBefore <= config.GiveUpBefore)
        {
            throw new ArgumentException(
                $"rotation: RotateBefore ({GoDuration.FormatPrecise(config.RotateBefore)}) must be > GiveUpBefore ({GoDuration.FormatPrecise(config.GiveUpBefore)})");
        }
    }

    // Calls Retire for old once Overlap has passed, unless ct ends first.
    private static async Task RetireAfterOverlap(RotationConfig config, TimeProvider time, RotationToken old, CancellationToken ct)
    {
        if (!await Sleep(time, config.Overlap, ct).ConfigureAwait(false))
        {
            return;
        }

        try
        {
            await config.Retire!(old, ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Retire is best-effort cleanup; a failing one must not end the rotation.
        }
    }

    // One timer covers at most this long; longer waits are taken in several steps.
    private static readonly TimeSpan MaxTimerStep = TimeSpan.FromDays(24);

    // Timers work in whole milliseconds: a shorter wait would return at once.
    private static readonly TimeSpan MinTimerStep = TimeSpan.FromMilliseconds(1);

    // Waits d (immediately when d <= 0); false when ct ended first.
    private static async Task<bool> Sleep(TimeProvider time, TimeSpan d, CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
        {
            return false;
        }

        if (d <= TimeSpan.Zero)
        {
            return true;
        }

        try
        {
            var until = time.GetUtcNow() + d;
            for (var left = d; left > TimeSpan.Zero; left = until - time.GetUtcNow())
            {
                var step = left > MaxTimerStep ? MaxTimerStep : left < MinTimerStep ? MinTimerStep : left;
                await Task.Delay(step, time, ct).ConfigureAwait(false);
            }

            return !ct.IsCancellationRequested;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
