using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent;

// Mid-run token renewal (protocol 1.5, SPEC §12.4).
public sealed partial class Agent
{
    // RenewResponse.Reason values when a renew is refused.
    internal const string RenewBadRequest = "bad_request";
    internal const string RenewWrongRun = "wrong_run";
    internal const string RenewInvalidToken = "invalid_token";
    internal const string RenewIdentityMismatch = "identity_mismatch";
    internal const string RenewStaleToken = "stale_token";

    /// <summary>
    /// A run's private renew subject, in the agent's own subject space (registered base name,
    /// never a regional alias): <c>trx.agent.&lt;name&gt;.renew.&lt;22 random base64url chars&gt;</c>.
    /// The agent may already subscribe there and callers publish there; other agents cannot
    /// subscribe.
    /// </summary>
    internal static string NewRenewSubject(string agentName)
    {
        var token = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        return Protocol.AgentSubject(agentName, "renew." + token);
    }

    /// <summary>
    /// Opens the turn's private renew subject on this instance's connection. The caller ends
    /// the subscription when the run ends.
    /// </summary>
    private async Task<(string Subject, INatsSub<byte[]> Sub)> SubscribeRenew(Turn turn)
    {
        var subject = NewRenewSubject(_cfg.Name);
        var sub = await _nc.SubscribeCoreAsync(subject, serializer: Framing.Raw).ConfigureAwait(false);
        _ = Background.Pump(sub, turn.Logger, async msg =>
        {
            var resp = await ApplyRenew(turn, msg.Data ?? []).ConfigureAwait(false);
            if (resp is not null && !string.IsNullOrEmpty(msg.ReplyTo))
            {
                await msg.ReplyAsync(WireJson.Serialize(resp), serializer: Framing.Raw).ConfigureAwait(false);
            }
        });
        return (subject, sub);
    }

    /// <summary>Decides one renew request. Null = the run has ended: no reply.</summary>
    internal async Task<RenewResponse?> ApplyRenew(Turn turn, byte[] body)
    {
        if (turn.Token.IsClosed)
        {
            return null;
        }

        var logger = turn.Logger;
        if (!WireJson.TryDeserialize<RenewRequest>(body, out var req))
        {
            return Reject(RenewBadRequest);
        }

        if (req.RunId != turn.RunId)
        {
            return Reject(RenewWrongRun);
        }

        var token = (req.Idt ?? "").Trim();
        if (token.Length == 0)
        {
            return Reject(RenewInvalidToken);
        }

        // Replay guard: a token expiring before the one the run holds (known from a previous
        // renew) is an older token resent. Unknown = as before.
        if (req.ExpiresAtMs > 0 && turn.IdtExpiry is { } currentExpiry
            && DateTimeOffset.FromUnixTimeMilliseconds(req.ExpiresAtMs) < currentExpiry)
        {
            return Reject(RenewStaleToken);
        }

        // Fresh check: a cached allow must not admit a token revoked since.
        CallerIdentity id;
        try
        {
            id = await _idt.AuthorizeFreshAsync(token, turn.SessionId).ConfigureAwait(false);
        }
        catch (NatsServiceException)
        {
            return Reject(RenewInvalidToken);
        }

        var original = turn.Identity;
        if (!original.Verified)
        {
            // Unverified run (observe deny, fail-open, validation off): compare with the
            // caller identity first resolved for it. When unknown, learn it once from the
            // token the run holds now and keep it, so later renews are compared even after
            // that token expires.
            if (turn.Token.Caller().UserId.Length == 0 && id.UserId.Length > 0)
            {
                try
                {
                    var current = await _idt.AuthorizeFreshAsync(turn.CurrentIdt, turn.SessionId).ConfigureAwait(false);
                    turn.Token.RememberCaller(current.UserId, current.AccountId);
                }
                catch (NatsServiceException)
                {
                    // the current token no longer resolves: nothing to learn
                }
            }

            var (userId, accountId) = turn.Token.Caller();
            original = original with { UserId = userId, AccountId = accountId };
        }

        if (!SameCaller(original, id))
        {
            return Reject(RenewIdentityMismatch);
        }

        var hooks = turn.Token.Swap(token, req.ExpiresAtMs);
        if (hooks is null)
        {
            return null;
        }

        foreach (var hook in hooks)
        {
            // A failing hook is logged, not propagated, so the remaining hooks run and the
            // renew is still answered.
            try
            {
                hook(token);
            }
            catch (Exception ex)
            {
                logger.LogError("run {RunId}: OnRenew hook failed: {Error}", turn.RunId, ex.Message);
            }
        }

        logger.LogInformation(
            "run {RunId}: renew applied user={User} account={Account} verified={Verified}",
            turn.RunId, id.UserId, id.AccountId, id.Verified ? "true" : "false");
        return new RenewResponse { Applied = true };

        RenewResponse Reject(string reason)
        {
            logger.LogInformation("run {RunId}: renew rejected reason={Reason}", turn.RunId, reason);
            return new RenewResponse { Applied = false, Reason = reason };
        }
    }

    /// <summary>
    /// Reports whether <paramref name="next"/> may replace <paramref name="original"/>'s
    /// token. A verified run needs a verified token for the same user and account. An
    /// unverified run (validation off, observe-only, fail-open) follows its admission rule,
    /// but a token that resolves to a different user or account is still refused.
    /// </summary>
    internal static bool SameCaller(CallerIdentity original, CallerIdentity next)
    {
        if (original.Verified)
        {
            return next.Verified && next.UserId == original.UserId && next.AccountId == original.AccountId;
        }

        if (original.UserId.Length > 0 && next.UserId.Length > 0)
        {
            return next.UserId == original.UserId && next.AccountId == original.AccountId;
        }

        return true;
    }
}
