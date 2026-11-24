using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/agentclient/client_test.go, part 2: renewing a running turn's token
/// (unsupported, rejected, applied, transport failures, stray and malformed replies) and the
/// renew subject check. Part 1 is <see cref="AgentClientTests"/>.
/// </summary>
public sealed class AgentClientRenewTests(NatsServerFixture nats) : AgentClientTestBase(nats)
{
    private static readonly TimeSpan FiveSeconds = TimeSpan.FromSeconds(5);

    private static RenewRequest Request() => new() { RunId = "r", Idt = "tok" };

    /// <summary>
    /// A unique subject nobody listens on (the Go tests use <c>nats.NewInbox()</c>). Not
    /// <c>NatsConnection.NewInbox()</c>: every .NET connection subscribes its whole inbox
    /// prefix, so such a subject always has a listener and never yields "no responders".
    /// </summary>
    private static string UnheardSubject() => "test.renew." + Guid.NewGuid().ToString("N");

    [Fact]
    public async Task RenewUnsupported()
    {
        var f = await StartFakeAgentAsync(null, async publish =>
        {
            await publish(new AgentEvent { Type = EventTypes.Done });
        });
        var run = await (await AgentClientAsync()).ChatAsync(f.Name, ChatReq());
        await DrainWithinAsync(run, FiveSeconds);
        await Assert.ThrowsAsync<RenewUnsupportedException>(() => run.RenewAsync("tok"));

        // The static renew (Go: the package-level Renew) with no subject.
        var nc = await ConnectAsync();
        await Assert.ThrowsAsync<RenewUnsupportedException>(() => AgentClient.RenewAsync(nc, "", Request()));
    }

    [Fact]
    public async Task RenewRejected()
    {
        var subject = Protocol.AgentSubject("agentR", "renew.Q2x0Yk9fX-abcdefghijk");
        var got = await RenewResponderAsync(subject, new RenewResponse { Applied = false, Reason = "identity_mismatch" });
        var run = DetachedRun(await AgentClientAsync(), "agentR", "run-9", subject);

        var ex = await Assert.ThrowsAsync<RenewRejectedException>(() => run.RenewAsync("new-token"));
        Assert.Equal("identity_mismatch", ex.Reason);
        Assert.Contains("identity_mismatch", ex.Message, StringComparison.Ordinal);

        var req = await ReceiveAsync(got, FiveSeconds, "no renew request received");
        Assert.Equal(("run-9", "new-token", 0L), (req.RunId, req.Idt, req.ExpiresAtMs));
    }

    [Fact]
    public async Task RenewApplied()
    {
        var subject = Protocol.AgentSubject("agentR", "renew.Q2x0Yk9fX-abcdefghijk");
        var got = await RenewResponderAsync(subject, new RenewResponse { Applied = true });
        var run = DetachedRun(await AgentClientAsync(), "agentR", "run-9", subject);

        var expiry = DateTimeOffset.FromUnixTimeMilliseconds(1_900_000_000_123);
        await run.RenewAsync("new-token", expiry);
        var req = await ReceiveAsync(got, FiveSeconds, "no renew request received");
        Assert.Equal(("run-9", "new-token", 1_900_000_000_123L), (req.RunId, req.Idt, req.ExpiresAtMs));

        // No expiry (Go: the zero time) is sent as none.
        await run.RenewAsync("t2", expiresAt: null);
        req = await ReceiveAsync(got, FiveSeconds, "no renew request received");
        Assert.Equal(0L, req.ExpiresAtMs);
    }

    [Fact]
    public async Task RenewNoResponder()
    {
        var nc = await ConnectAsync();
        var ex = await Record.ExceptionAsync(() => AgentClient.RenewAsync(nc, UnheardSubject(), Request()));
        Assert.True(
            ex is not null and not RenewRejectedException and not RenewUnsupportedException,
            $"Renew with nobody listening = {ex}, want a distinct transport error");
    }

    [Fact]
    public async Task RenewTransportError()
    {
        // No responders. (The exception types are sealed siblings, so a transport error can
        // never also match the rejected or unsupported one.)
        var nc = await ConnectAsync();
        var ex = await Assert.ThrowsAsync<RenewTransportException>(() => AgentClient.RenewAsync(nc, UnheardSubject(), Request()));
        Assert.IsType<NatsNoRespondersException>(ex.InnerException); // Go: wraps nats.ErrNoResponders

        // Connection error.
        var closed = await Nats.ConnectAsync();
        await closed.DisposeAsync();
        ex = await Assert.ThrowsAsync<RenewTransportException>(() => AgentClient.RenewAsync(closed, UnheardSubject(), Request()));
        Assert.IsType<ObjectDisposedException>(ex.InnerException); // Go: wraps nats.ErrConnectionClosed

        // Caller cancelled: the cancellation, not the transport error.
        var subject = UnheardSubject();
        var silent = await ConnectAsync();
        await SubscribeAsync(silent, subject, _ => ValueTask.CompletedTask);
        using (var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(50)))
        {
            var caller = await ConnectAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => AgentClient.RenewAsync(caller, subject, Request(), cancel.Token));
        }

        // Rejected and unsupported unchanged.
        await Assert.ThrowsAsync<RenewUnsupportedException>(() => AgentClient.RenewAsync(nc, "", new RenewRequest()));
        var rejecting = UnheardSubject();
        await RenewResponderAsync(rejecting, new RenewResponse { Applied = false, Reason = "wrong_run" });
        await Assert.ThrowsAsync<RenewRejectedException>(() => AgentClient.RenewAsync(nc, rejecting, new RenewRequest()));
    }

    [Fact]
    public void ValidRenewSubject()
    {
        const string good = "trx.agent.copay.renew.Q2x0Yk9fX-abcdefghijk";
        foreach (var (agent, subject, want) in new (string, string, bool)[]
        {
            ("copay", good, true),
            ("copay_east", good, true), // regional alias: subject built from the base name
            ("copay_assist", "trx.agent.copay_assist.renew.abc", true),
            ("other", good, false),
            ("copay", "_RENEW.Q2x0Yk9fX-abcdefghijk", false),
            ("copay", "_INBOX.Q2x0Yk9fX", false),
            ("copay", "trx.agent.copay.renew.*", false),
            ("copay", "trx.agent.copay.renew.>", false),
            ("copay", "trx.agent.copay.renew.ab*c", false),
            ("copay", "trx.agent.copay.renew.", false),
            ("copay", "", false),
            ("", good, false),
            ("copay", "trx.agent.copay.renew.abc.def", false),
            ("copay", "trx.agent.copay.renew.ab c", false),
            ("copay", "trx.agent.copay.chat", false),
            ("copay", "trx.agent.copay_east.renew.abc", false),
            ("copay", "x.trx.agent.copay.renew.abc", false),
        })
        {
            var got = AgentClient.IsValidRenewSubject(agent, subject);
            Assert.True(got == want, $"IsValidRenewSubject(\"{agent}\", \"{subject}\") = {got}, want {want}");
        }
    }

    // A run's renew goes only to a renew subject of the agent the run was started on; anything
    // else (another agent's space, old _RENEW subjects) is refused before anything is
    // published.
    [Fact]
    public async Task RunRenewRefusesForeignSubject()
    {
        var c = await AgentClientAsync();
        foreach (var subject in new[]
        {
            Protocol.AgentSubject("otherAgent", "renew.abc"),
            "_RENEW.Q2x0Yk9fX-abcdefghijk",
            "_INBOX.abc",
        })
        {
            var got = await RenewResponderAsync(subject, new RenewResponse { Applied = true });
            var run = DetachedRun(c, "agentR", "run-9", subject);
            await Assert.ThrowsAsync<RenewUnsupportedException>(() => run.RenewAsync("tok"));
            await AssertNothingAsync(got, TimeSpan.FromMilliseconds(100), req => $"subject \"{subject}\": token sent: run {req.RunId}, idt {req.Idt}");
        }
    }

    // The agent's renew handler may make two fresh identity checks (5 s each).
    [Fact]
    public void RenewTimeoutCoversTwoIdentityChecks()
    {
        Assert.Equal(TimeSpan.FromSeconds(12), AgentClient.RenewTimeout);
    }

    // A reply that is not a well-formed RenewResponse is never a rejection: the agent may have
    // applied the token. Alone, it ends as a transport error.
    [Theory]
    [InlineData("empty body")]
    [InlineData("empty object")]
    [InlineData("not json")]
    [InlineData("applied null")]
    [InlineData("404 envelope")]
    [InlineData("error status")]
    public async Task RenewMalformedReplyIsNotRejection(string name)
    {
        var subject = Protocol.AgentSubject("agentM", "renew.abcdefghijklmnopqrstuv" + name.Replace(" ", "", StringComparison.Ordinal));
        var reply = name switch
        {
            "empty body" => new RawReply(),
            "empty object" => new RawReply("{}"),
            "not json" => new RawReply("oops"),
            "applied null" => new RawReply("""{"applied":null}"""),
            "404 envelope" => NotFoundEnvelope(subject),
            "error status" => new RawReply("""{"applied":false}""", new Dictionary<string, string> { ["status"] = "500" }),
            _ => throw new ArgumentOutOfRangeException(nameof(name)),
        };
        await RawRenewResponderAsync(subject, reply);

        // The Go shortRenewTimeout: the internal overload takes the timeout.
        var nc = await ConnectAsync();
        var ex = await Record.ExceptionAsync(() =>
            AgentClient.RenewAsync(nc, subject, Request(), TimeSpan.FromMilliseconds(300), CancellationToken.None));
        Assert.True(ex is RenewTransportException, $"Renew = {ex}, want a transport error, not a rejection");
    }

    // A stray framework reply arriving before the run's own answer is skipped: the run's
    // answer decides.
    [Fact]
    public async Task RenewSkipsStrayReplyBeforeAnswer()
    {
        var nc = await ConnectAsync();
        var subject = Protocol.AgentSubject("agentM", "renew.straythenapplied00");
        await RawRenewResponderAsync(subject, NotFoundEnvelope(subject), new RawReply("""{"applied":true}"""));
        await AgentClient.RenewAsync(nc, subject, Request()); // the run applied it

        subject = Protocol.AgentSubject("agentM", "renew.straythenrefused00");
        await RawRenewResponderAsync(subject, NotFoundEnvelope(subject), new RawReply("""{"applied":false,"reason":"identity_mismatch"}"""));
        var ex = await Assert.ThrowsAsync<RenewRejectedException>(() => AgentClient.RenewAsync(nc, subject, Request()));
        Assert.Contains("identity_mismatch", ex.Message, StringComparison.Ordinal);
    }
}
