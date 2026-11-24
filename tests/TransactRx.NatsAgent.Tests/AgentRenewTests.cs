using System.Collections.Concurrent;
using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>Port of pkg/agent/renew_test.go: mid-run token renewal on the agent side.</summary>
public sealed class AgentRenewTests(NatsServerFixture nats) : AgentRuntimeTestBase(nats)
{
    [Fact]
    public async Task RenewAccepted()
    {
        var subj = RenewIdentitySubject();
        await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj });
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turns);

        var prefix = Protocol.AgentSubject(a.Name, "renew.");
        var renewSubject = run.Ack.Caps?.RenewSubject;
        Assert.NotNull(renewSubject);
        Assert.True(
            renewSubject.StartsWith(prefix, StringComparison.Ordinal) && renewSubject.Length == prefix.Length + 22,
            $"renewSubject = \"{renewSubject}\", want {prefix}<22 chars>");
        Assert.True(AgentClient.IsValidRenewSubject(a.Name, renewSubject), $"renewSubject \"{renewSubject}\" rejected by the client validator");
        Assert.Equal("IDT-alice1.c", turn.CurrentIdt);

        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-alice2.c" });
        Assert.True(resp.Applied, $"renew rejected: {resp}");
        Assert.Equal("IDT-alice2.c", turn.CurrentIdt); // the new token
        Assert.Equal("IDT-alice1.c", turn.Identity.Idt); // must keep the first token
        a.Release();
        await DrainAsync(run);
    }

    [Fact]
    public async Task RenewRejectedOtherUser()
    {
        var subj = RenewIdentitySubject();
        await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj });
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turns);
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-bob.c" });
        Assert.False(resp.Applied, $"token of another user accepted: {resp}");
        Assert.False(string.IsNullOrEmpty(resp.Reason), $"token of another user refused without a reason: {resp}");
        Assert.Equal("IDT-alice1.c", turn.CurrentIdt); // unchanged on reject
        a.Release();
        await DrainAsync(run);
    }

    [Fact]
    public async Task RenewRejectedOtherAccount()
    {
        var subj = RenewIdentitySubject();
        await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj });
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turns);
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-aliceOther.c" });
        Assert.False(resp.Applied, $"token of another account accepted: {resp}");
        Assert.False(string.IsNullOrEmpty(resp.Reason), $"token of another account refused without a reason: {resp}");
        Assert.Equal("IDT-alice1.c", turn.CurrentIdt); // unchanged on reject
        a.Release();
        await DrainAsync(run);
    }

    [Fact]
    public async Task RenewRejectedInvalidToken()
    {
        var subj = RenewIdentitySubject();
        await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj });
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turns);
        foreach (var tok in new[] { "IDT-forged.c", "" })
        {
            var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = tok });
            Assert.False(resp.Applied, $"invalid token \"{tok}\" accepted: {resp}");
            Assert.False(string.IsNullOrEmpty(resp.Reason), $"invalid token \"{tok}\" refused without a reason: {resp}");
        }

        Assert.Equal("IDT-alice1.c", turn.CurrentIdt); // unchanged on reject
        a.Release();
        await DrainAsync(run);
    }

    // A token that identity allowed (and the validator cached) but that has since been
    // revoked must be rejected: renew validates fresh.
    [Fact]
    public async Task RenewValidatesFresh()
    {
        var subj = RenewIdentitySubject();
        var f = await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj });

        // Cache an allow for IDT-aliceCached.c in session s1.
        var first = await ChatAsAsync(a, "IDT-aliceCached.c");
        await WaitAsync(turns);
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turns);
        f.Revoke("IDT-aliceCached.c");
        var before = f.CallCount;
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-aliceCached.c" });
        Assert.False(resp.Applied, $"revoked token accepted from cache: {resp}");
        Assert.Equal(before + 1, f.CallCount); // renew must ask identity
        Assert.Equal("IDT-alice1.c", turn.CurrentIdt); // unchanged on reject
        a.Release();
        await DrainAsync(first);
        await DrainAsync(run);
    }

    [Fact]
    public async Task RenewRejectedWrongRun()
    {
        var subj = RenewIdentitySubject();
        await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj });
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turns);
        var resp = await RenewAsync(run, new RenewRequest { RunId = "run_other", Idt = "IDT-alice2.c" });
        Assert.False(resp.Applied, $"renew for another run accepted: {resp}");
        Assert.False(string.IsNullOrEmpty(resp.Reason), $"renew for another run refused without a reason: {resp}");
        Assert.Equal("IDT-alice1.c", turn.CurrentIdt); // unchanged on reject
        a.Release();
        await DrainAsync(run);
    }

    // Observe mode admits turns unverified; renew follows the same rule but still refuses a
    // token identity resolves to a different user.
    [Fact]
    public async Task RenewObserveMode()
    {
        var subj = RenewIdentitySubject();
        await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj, Observe = true });
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turns);
        var other = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-bob.c" });
        Assert.False(other.Applied, $"observe mode accepted another user's token: {other}");
        var same = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-alice2.c" });
        Assert.True(same.Applied, $"observe mode rejected same user's token: {same}");
        Assert.Equal("IDT-alice2.c", turn.CurrentIdt);
        a.Release();
        await DrainAsync(run);
    }

    // Validation off: the turn was admitted on its token alone; so is a renew.
    [Fact]
    public async Task RenewValidationOff()
    {
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true });
        var run = await ChatAsAsync(a, "IDT-x1.c");
        var turn = await WaitAsync(turns);
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-x2.c" });
        Assert.True(resp.Applied, $"renew rejected with validation off: {resp}");
        Assert.Equal("IDT-x2.c", turn.CurrentIdt);
        a.Release();
        await DrainAsync(run);
    }

    // The Go test counts the subscriptions of the agent's connection (nc.NumSubscriptions);
    // NATS.Net does not expose that, so the server is asked which subjects the agent's
    // connection holds.
    [Fact]
    public async Task NoRenewSubjectWithoutCapability()
    {
        var server = await UseMonitoredServerAsync();
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = false });
        var subsBefore = await server.SubscriptionsAsync(AgentConnectionName);
        Assert.NotEmpty(subsBefore); // the agent's own endpoints
        var run = await ChatAsAsync(a, "IDT-x1.c");
        await WaitAsync(turns);
        Assert.True(string.IsNullOrEmpty(run.Ack.Caps?.RenewSubject), $"ack announces renewSubject without capability: {run.Ack.Caps}");
        Assert.Equal(subsBefore, await server.SubscriptionsAsync(AgentConnectionName)); // nothing opened during a run without renew
        a.Release();
        await DrainAsync(run);
    }

    [Fact]
    public async Task RenewSubjectClosedAfterRun()
    {
        var server = await UseMonitoredServerAsync();
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true });
        var subsBefore = await server.SubscriptionsAsync(AgentConnectionName);
        var run = await ChatAsAsync(a, "IDT-x1.c");
        await WaitAsync(turns);
        var renewSubject = run.Ack.Caps?.RenewSubject;
        Assert.False(string.IsNullOrEmpty(renewSubject));

        // Exactly one more subscription: the run's renew subject.
        var during = await server.SubscriptionsAsync(AgentConnectionName);
        Assert.Equal(subsBefore.Count + 1, during.Count);
        Assert.Equal(subsBefore.Append(renewSubject).Order(StringComparer.Ordinal), during);
        a.Release();
        await DrainAsync(run);
        var deadline = DateTime.UtcNow.AddSeconds(2);
        var after = await server.SubscriptionsAsync(AgentConnectionName);
        while (after.Count != subsBefore.Count && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
            after = await server.SubscriptionsAsync(AgentConnectionName);
        }

        Assert.Equal(subsBefore, after); // the renew subscription must not leak

        // The subject lives in the agent's space, where the service stays silent on renew
        // paths: a late renew gets no reply at all (a timeout, not "no responders").
        var nc = await CallerAsync();
        var data = WireJson.Serialize(new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-x2.c" });
        await Assert.ThrowsAsync<NatsNoReplyException>(async () =>
            await nc.RequestAsync<byte[], byte[]>(
                renewSubject, data, replyOpts: new NatsSubOpts { Timeout = TimeSpan.FromMilliseconds(300) }));
    }

    [Fact]
    public async Task OnRenewCalled()
    {
        var got = new ConcurrentQueue<string>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = System.Threading.Channels.Channel.CreateUnbounded<Turn>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions
        {
            Renew = true,
            Handler = async (turn, _, _) =>
            {
                turn.OnRenew(got.Enqueue);
                ready.Writer.TryWrite(turn);
                await release.Task;
            },
        });
        var run = await ChatAsAsync(a, "IDT-x1.c");
        await WaitAsync(ready.Reader);
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-x2.c" });
        Assert.True(resp.Applied, $"renew rejected: {resp}");

        // The hook ran before the renew reply.
        Assert.True(got.TryDequeue(out var idt), "OnRenew not called before the renew reply");
        Assert.Equal("IDT-x2.c", idt);

        // A rejected renew does not call OnRenew.
        var wrong = await RenewAsync(run, new RenewRequest { RunId = "run_other", Idt = "IDT-x3.c" });
        Assert.False(wrong.Applied, $"wrong run accepted: {wrong}");
        Assert.False(got.TryDequeue(out var onReject), $"OnRenew called on reject with \"{onReject}\"");
        release.SetResult();
        await DrainAsync(run);
    }

    [Fact]
    public async Task IDTExpiryFromRenew()
    {
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true });
        var run = await ChatAsAsync(a, "IDT-x1.c");
        var turn = await WaitAsync(turns);
        Assert.Null(turn.IdtExpiry); // not set before any renew
        var exp = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddMinutes(15).ToUnixTimeMilliseconds());
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-x2.c", ExpiresAtMs = exp.ToUnixTimeMilliseconds() });
        Assert.True(resp.Applied, $"renew rejected: {resp}");
        Assert.Equal(exp, turn.IdtExpiry);

        // A renew without expiry clears it.
        resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-x3.c" });
        Assert.True(resp.Applied, $"renew rejected: {resp}");
        Assert.Null(turn.IdtExpiry); // not kept after a renew without expiresAtMs
        a.Release();
        await DrainAsync(run);
    }

    // Observe mode: identity's observe-deny (valid token, function not granted) still names
    // the user; a renew to another user is refused, whether the other side or the run itself
    // was admitted on such a token.
    [Theory]
    [InlineData("renew token not granted", "IDT-alice1.c", "IDT-bobNoFn.c", "IDT-aliceNoFn.c")]
    [InlineData("run token not granted", "IDT-aliceNoFn.c", "IDT-bob.c", "IDT-alice2.c")]
    public async Task RenewObserveModeRefusesOtherUser(string name, string first, string other, string same)
    {
        var subj = RenewIdentitySubject(name);
        await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj, Observe = true }, name);
        var run = await ChatAsAsync(a, first);
        var turn = await WaitAsync(turns);
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = other });
        Assert.False(resp.Applied, $"observe mode accepted another user's token: {resp}");
        Assert.Equal(Agent.RenewIdentityMismatch, resp.Reason);
        Assert.Equal(first, turn.CurrentIdt); // unchanged on reject
        resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = same });
        Assert.True(resp.Applied, $"observe mode rejected same user's token: {resp}");
        a.Release();
        await DrainAsync(run);
    }

    // A failing OnRenew hook (the Go test's panicking hook) is contained: later hooks still
    // run and the renew is still acknowledged.
    [Fact]
    public async Task OnRenewHookPanicRecovered()
    {
        var got = new ConcurrentQueue<string>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = System.Threading.Channels.Channel.CreateUnbounded<Turn>();
        var (a, _) = await StartRenewAgentAsync(new RenewAgentOptions
        {
            Renew = true,
            Handler = async (turn, _, _) =>
            {
                turn.OnRenew(_ => throw new InvalidOperationException("hook boom"));
                turn.OnRenew(got.Enqueue);
                ready.Writer.TryWrite(turn);
                await release.Task;
            },
        });
        var run = await ChatAsAsync(a, "IDT-x1.c");
        var turn = await WaitAsync(ready.Reader);
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-x2.c" });
        Assert.True(resp.Applied, $"renew after hook failure: {resp}");
        Assert.True(got.TryDequeue(out var idt), "hook after the failing one not called");
        Assert.Equal("IDT-x2.c", idt);
        Assert.Equal("IDT-x2.c", turn.CurrentIdt);
        release.SetResult();
        await DrainAsync(run);
    }

    // Observe mode: the caller resolved at the first fresh check is kept, so a later renew is
    // still compared after the run's current token has expired.
    [Fact]
    public async Task RenewObserveModeAfterTokenExpiry()
    {
        var subj = RenewIdentitySubject();
        var f = await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj, Observe = true });
        var run = await ChatAsAsync(a, "IDT-aliceNoFn.c"); // admitted without ids
        var turn = await WaitAsync(turns);
        var resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-alice2.c" });
        Assert.True(resp.Applied, $"same user's token rejected: {resp}");

        // Both tokens the run has held are now expired at identity.
        f.Revoke("IDT-aliceNoFn.c");
        f.Revoke("IDT-alice2.c");
        resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-bob.c" });
        Assert.False(resp.Applied, $"another user's token accepted after expiry: {resp}");
        Assert.Equal(Agent.RenewIdentityMismatch, resp.Reason);
        Assert.Equal("IDT-alice2.c", turn.CurrentIdt); // unchanged on reject
        resp = await RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-alice1.c" });
        Assert.True(resp.Applied, $"same user's token rejected after expiry: {resp}");
        a.Release();
        await DrainAsync(run);
    }

    // A renew carrying an expiry earlier than the current token's (known from a previous
    // renew) is a replay of an older token: refused as stale_token. Equal or later expiries
    // are accepted; unknown expiries behave as before.
    [Fact]
    public async Task RenewRefusesStaleToken()
    {
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true });
        var run = await ChatAsAsync(a, "IDT-x1.c");
        var turn = await WaitAsync(turns);
        var @base = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddMinutes(15).ToUnixTimeMilliseconds());
        Task<RenewResponse> Send(string tok, DateTimeOffset? exp) =>
            RenewAsync(run, new RenewRequest { RunId = run.Ack.RunId, Idt = tok, ExpiresAtMs = exp?.ToUnixTimeMilliseconds() ?? 0 });

        var resp = await Send("IDT-x2.c", @base);
        Assert.True(resp.Applied, $"first renew rejected: {resp}");
        resp = await Send("IDT-old.c", @base.AddMinutes(-1));
        Assert.False(resp.Applied, $"older token = {resp}, want refused stale_token");
        Assert.Equal("stale_token", resp.Reason);
        Assert.Equal("IDT-x2.c", turn.CurrentIdt); // unchanged on a stale renew
        resp = await Send("IDT-x3.c", @base);
        Assert.True(resp.Applied, $"equal expiry rejected: {resp}");
        resp = await Send("IDT-x4.c", @base.AddMinutes(1));
        Assert.True(resp.Applied, $"later expiry rejected: {resp}");
        resp = await Send("IDT-x5.c", null);
        Assert.True(resp.Applied, $"renew without expiry rejected: {resp}");

        // Current expiry now unknown: any expiry is accepted.
        resp = await Send("IDT-x6.c", @base.AddMinutes(-10));
        Assert.True(resp.Applied, $"renew after unknown expiry rejected: {resp}");
        a.Release();
        await DrainAsync(run);
    }
}
