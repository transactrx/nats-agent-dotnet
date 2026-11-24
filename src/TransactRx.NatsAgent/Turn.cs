using System.Text.Json;
using Microsoft.Extensions.Logging;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent;

/// <summary>One chat (or invoke) request as seen by the handler.</summary>
public sealed class Turn
{
    private readonly TimeProvider _time;

    internal Turn(ChatRequest request, string runId, ILogger logger, CallerIdentity identity, TimeProvider time)
    {
        Request = request;
        RunId = runId;
        Logger = logger;
        Identity = identity;
        _time = time;
        Token = new TokenHolder(identity.Idt, identity.UserId, identity.AccountId);
    }

    /// <summary>
    /// The request, cleaned by the runtime: the session id is always set, the user context is
    /// normalized, and the user id is the verified one when <see cref="Identity"/> is verified.
    /// </summary>
    public ChatRequest Request { get; }

    public string RunId { get; }

    /// <summary>The conversation this turn belongs to (generated when the caller sent none).</summary>
    public string SessionId => Request.SessionId ?? "";

    /// <summary>
    /// The user behind the turn: verified when <see cref="Identity"/> is verified, otherwise
    /// the caller's unauthenticated assertion.
    /// </summary>
    public string? UserId => Request.UserId;

    public Message Message => Request.Message;

    public string StreamSubject => Request.StreamSubject;

    public IReadOnlyDictionary<string, JsonElement>? Metadata => Request.Metadata;

    /// <summary>Display-only user context (normalized), or null.</summary>
    public UserContext? Context => Request.Context;

    public Budget? Budget => Request.Budget;

    /// <summary>Logger prefixed with the request's message id.</summary>
    public ILogger Logger { get; }

    /// <summary>
    /// The runtime's verdict on the caller (see <see cref="IdtValidation"/>).
    /// <c>Identity.Idt</c> keeps the first token; see <see cref="CurrentIdt"/> for renewed ones.
    /// </summary>
    public CallerIdentity Identity { get; }

    /// <summary>The user's time zone from the request context, or UTC.</summary>
    public TimeZoneInfo TimeZone => Context.Zone();

    /// <summary>The current time in the user's time zone (UTC when unknown).</summary>
    public DateTimeOffset Now => Context.ToLocalTime(_time.GetUtcNow());

    /// <summary>
    /// The latest accepted token of this run (the first one until a renew is accepted). Use it
    /// for every new onward call; calls already in flight keep the token they began with.
    /// </summary>
    public string CurrentIdt => Token.Current;

    /// <summary>
    /// The expiry of the current token as carried by the last accepted renew; null before the
    /// first renew or when it carried none.
    /// </summary>
    public DateTimeOffset? IdtExpiry => Token.Expiry;

    /// <summary>
    /// Why the run was cut, as a wire reason (<see cref="Reasons"/>): <c>time_cap</c> when the
    /// request budget ran out, <c>interrupted</c> after <see cref="Agent.InterruptAll"/>, or
    /// the reason carried by a cancel request. Null when the run was not cut — still running,
    /// or cancelled by the user.
    /// </summary>
    public string? CutReason => Run?.CutReason;

    internal TokenHolder Token { get; }

    internal AgentRun? Run { get; set; }

    /// <summary>
    /// The standard sentence to put in the system prompt so the model knows who is asking and
    /// what "today" means for them (see <see cref="UserContextExtensions.Line"/>). Display
    /// only, never authorization.
    /// </summary>
    public string ContextLine() => Context.Line(_time.GetUtcNow());

    /// <summary>
    /// Registers <paramref name="hook"/>, called with the new token after each accepted renew
    /// (before the renew is acknowledged). It must not block for long.
    /// </summary>
    public void OnRenew(Action<string> hook)
    {
        if (hook is not null)
        {
            Token.AddHook(hook);
        }
    }
}

/// <summary>A run's current IDT, swapped by accepted renews. Safe for concurrent use.</summary>
internal sealed class TokenHolder(string current, string userId, string accountId)
{
    private readonly object _gate = new();
    private readonly List<Action<string>> _hooks = [];
    private string _current = current;
    private DateTimeOffset? _expiry;
    private bool _closed; // run ended: renews are ignored

    // The run's caller as identity first resolved it (at admission or the first fresh check);
    // "" = not known yet. Renews of an unverified run are compared against these.
    private string _userId = userId;
    private string _accountId = accountId;

    public string Current
    {
        get
        {
            lock (_gate)
            {
                return _current;
            }
        }
    }

    public DateTimeOffset? Expiry
    {
        get
        {
            lock (_gate)
            {
                return _expiry;
            }
        }
    }

    public bool IsClosed
    {
        get
        {
            lock (_gate)
            {
                return _closed;
            }
        }
    }

    public void AddHook(Action<string> hook)
    {
        lock (_gate)
        {
            _hooks.Add(hook);
        }
    }

    /// <summary>Installs a renewed token and returns the hooks to call; null when the run already ended.</summary>
    public IReadOnlyList<Action<string>>? Swap(string token, long expiresAtMs)
    {
        lock (_gate)
        {
            if (_closed)
            {
                return null;
            }

            _current = token;
            _expiry = expiresAtMs > 0 ? DateTimeOffset.FromUnixTimeMilliseconds(expiresAtMs) : null;
            return _hooks.ToArray();
        }
    }

    /// <summary>The remembered caller ids ("" when unknown).</summary>
    public (string UserId, string AccountId) Caller()
    {
        lock (_gate)
        {
            return (_userId, _accountId);
        }
    }

    /// <summary>Keeps the first resolved caller; later calls are ignored.</summary>
    public void RememberCaller(string userId, string accountId)
    {
        lock (_gate)
        {
            if (_userId.Length == 0 && userId.Length > 0)
            {
                (_userId, _accountId) = (userId, accountId);
            }
        }
    }

    public void Close()
    {
        lock (_gate)
        {
            _closed = true;
        }
    }
}

/// <summary>One live run: its cancellation and why it ended. The first cause decides.</summary>
internal sealed class AgentRun
{
    private readonly CancellationTokenSource _cts = new();
    private readonly object _gate = new();
    private bool _ended;
    private ITimer? _deadline;

    public CancellationToken Token => _cts.Token;

    /// <summary>The cut reason when a limit ended the run first, else null.</summary>
    public string? CutReason { get; private set; }

    /// <summary>True when a plain (user) cancel ended the run first.</summary>
    public bool UserCancelled { get; private set; }

    /// <summary>Ends the run at <paramref name="delay"/> with reason time_cap unless something ends it first.</summary>
    public void SetDeadline(TimeProvider time, TimeSpan delay)
    {
        lock (_gate)
        {
            _deadline = time.CreateTimer(_ => Cancel(Reasons.TimeCap), null, delay, Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>
    /// Cancels the run. <paramref name="cutReason"/> null = a plain cancel. A later cancel
    /// never changes the cause recorded by the first.
    /// </summary>
    public void Cancel(string? cutReason)
    {
        lock (_gate)
        {
            if (_ended)
            {
                return;
            }

            _ended = true;
            CutReason = cutReason;
            UserCancelled = cutReason is null;
        }

        _cts.Cancel();
    }

    /// <summary>The handler returned: release the deadline and stop anything still using the token.</summary>
    public void Finish()
    {
        lock (_gate)
        {
            _ended = true;
            _deadline?.Dispose();
            _deadline = null;
        }

        _cts.Cancel();
    }
}
