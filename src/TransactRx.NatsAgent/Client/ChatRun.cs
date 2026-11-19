using System.Threading.Channels;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Client;

/// <summary>
/// One live chat turn: the ack plus the event stream. <see cref="Events"/> completes after
/// the terminal event (which is always delivered last). Transport pings are consumed
/// internally for liveness and not delivered. Events must be read until the terminal event:
/// the reader waits on a full channel.
/// </summary>
public sealed class ChatRun
{
    private readonly AgentClient _client;
    private readonly string _agent;

    internal ChatRun(AgentClient client, string agent, ChatAck ack, ChannelReader<AgentEvent> events)
    {
        _client = client;
        _agent = agent;
        Ack = ack;
        Caps = ack.Caps ?? new AckCaps();
        Events = events;
    }

    public ChatAck Ack { get; }

    /// <summary>
    /// The event stream. Typical use:
    /// <c>await foreach (var ev in run.Events.ReadAllAsync()) { ... }</c>
    /// </summary>
    public ChannelReader<AgentEvent> Events { get; }

    /// <summary><c>Ack.Caps</c>, empty when the agent sent none (older agents).</summary>
    public AckCaps Caps { get; }

    /// <summary>
    /// Asks the owning instance to cancel this run. The run still ends with a done event
    /// (stopReason "cancelled") on the stream.
    /// </summary>
    public Task<bool> CancelAsync(CancellationToken cancellationToken = default) =>
        _client.CancelAsync(_agent, Ack.RunId, cancellationToken);

    /// <summary>
    /// Cancels this run as a cut by the caller's limits (<see cref="Reasons.ProgressIdle"/>,
    /// <see cref="Reasons.TokenRotation"/>, <see cref="Reasons.Interrupted"/>,
    /// <see cref="Reasons.TimeCap"/>, <see cref="Reasons.Unavailable"/>): a 1.5 agent ends it
    /// with that reason instead of a user cancel. Older agents treat it as a plain cancel.
    /// Call it BEFORE cancelling the chat's cancellation token: that path publishes a plain
    /// cancel, and the first cancel to reach the agent decides how the run ends.
    /// </summary>
    public Task<bool> CancelWithReasonAsync(string reason, CancellationToken cancellationToken = default) =>
        _client.CancelWithReasonAsync(_agent, Ack.RunId, reason, cancellationToken);

    /// <summary>
    /// Hands the running turn a fresh token, optionally with its expiry. The ack's renew
    /// subject must be a renew subject of the agent this run was started on
    /// (<see cref="AgentClient.IsValidRenewSubject"/>); otherwise
    /// <see cref="RenewUnsupportedException"/> and nothing is sent.
    /// </summary>
    /// <exception cref="RenewUnsupportedException">The agent offered no (valid) renew subject.</exception>
    /// <exception cref="RenewRejectedException">The agent refused the token.</exception>
    /// <exception cref="RenewTransportException">No well-formed reply: the outcome is unknown.</exception>
    public Task RenewAsync(string idt, DateTimeOffset? expiresAt = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(Caps.RenewSubject) || !AgentClient.IsValidRenewSubject(_agent, Caps.RenewSubject))
        {
            throw new RenewUnsupportedException();
        }

        var request = new RenewRequest { RunId = Ack.RunId, Idt = idt, ExpiresAtMs = expiresAt?.ToUnixTimeMilliseconds() ?? 0 };
        return AgentClient.RenewAsync(_client.Connection, Caps.RenewSubject, request, _client.RenewTimeoutOverride, cancellationToken);
    }
}
