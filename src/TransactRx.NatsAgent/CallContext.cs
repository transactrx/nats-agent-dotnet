using NATS.Client.Core;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent;

/// <summary>
/// Ambient values that follow the current async flow onto outbound calls — the .NET
/// counterpart of the values the Go library carries on a <c>context.Context</c>
/// (<c>agentclient.WithIDT</c>, <c>WithIDTSource</c>, <c>WithUserContext</c>).
/// <para>
/// Each <c>With…</c> method returns a scope; dispose it (a <c>using</c> block) to restore the
/// previous value. Everything awaited or started inside the scope sees the value.
/// </para>
/// </summary>
public static class CallContext
{
    private static readonly AsyncLocal<State?> Current = new();

    private sealed record State(string? Idt, Func<string?>? IdtSource, UserContext? User);

    /// <summary>
    /// The caller's Internal Delegation Token for outbound calls: from the
    /// <see cref="WithIdtSource"/> source when it returns one, else the
    /// <see cref="WithIdt"/> value, else null.
    /// </summary>
    public static string? Idt
    {
        get
        {
            var state = Current.Value;
            var fromSource = state?.IdtSource?.Invoke();
            return string.IsNullOrEmpty(fromSource) ? NullIfEmpty(state?.Idt) : fromSource;
        }
    }

    /// <summary>The user's display context set by <see cref="WithUserContext"/>, or null.</summary>
    public static UserContext? UserContext => Current.Value?.User;

    /// <summary>
    /// Every authenticated call (chat, invoke, sessions, tool runs) made inside the scope
    /// sends <paramref name="idt"/> as the X-TRX-IDT NATS header. Empty is a no-op.
    /// </summary>
    public static IDisposable WithIdt(string? idt)
    {
        if (string.IsNullOrEmpty(idt))
        {
            return NoScope.Instance;
        }

        var state = Current.Value;
        return Push(state is null ? new State(idt, null, null) : state with { Idt = idt });
    }

    /// <summary>
    /// The token is read from <paramref name="source"/> on every call, so a long turn picks up
    /// a renewed token for its next request. When the source returns nothing the
    /// <see cref="WithIdt"/> value is used. A <see cref="WithIdt"/> inside this scope does not
    /// override the source. Null is a no-op.
    /// </summary>
    public static IDisposable WithIdtSource(Func<string?>? source)
    {
        if (source is null)
        {
            return NoScope.Instance;
        }

        var state = Current.Value;
        return Push(state is null ? new State(null, source, null) : state with { IdtSource = source });
    }

    /// <summary>
    /// Chat and invoke calls made inside the scope fill <see cref="ChatRequest.Context"/> when
    /// the request leaves it null, so the user's time zone and name follow them from agent to
    /// agent. The agent runtime already does this around every handler. Display only, never
    /// authorization. Null (or nothing valid) is a no-op.
    /// </summary>
    public static IDisposable WithUserContext(UserContext? userContext)
    {
        var normalized = userContext.Normalize();
        if (normalized is null)
        {
            return NoScope.Instance;
        }

        var state = Current.Value;
        return Push(state is null ? new State(null, null, normalized) : state with { User = normalized });
    }

    /// <summary>The X-TRX-IDT request header for the ambient token, or null.</summary>
    internal static NatsHeaders? IdtHeader()
    {
        var token = Idt;
        return string.IsNullOrEmpty(token) ? null : new NatsHeaders { [Protocol.HeaderIdt] = token };
    }

    private static Scope Push(State next)
    {
        var scope = new Scope(Current.Value);
        Current.Value = next;
        return scope;
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;

    private sealed class Scope(State? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                Current.Value = previous;
            }
        }
    }

    private sealed class NoScope : IDisposable
    {
        public static readonly NoScope Instance = new();

        public void Dispose()
        {
        }
    }
}
