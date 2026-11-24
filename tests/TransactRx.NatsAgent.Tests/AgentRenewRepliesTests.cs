using NATS.Client.Core;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/agent/renew_replies_test.go. Renew subjects live in
/// <c>trx.agent.&lt;name&gt;.&gt;</c>, which every instance of the agent also queue-subscribes
/// through its service. These tests count every reply a renew gets, not only the first one a
/// request call returns.
/// </summary>
public sealed class AgentRenewRepliesTests(NatsServerFixture nats, ITestOutputHelper output) : AgentRuntimeTestBase(nats)
{
    // The standard endpoints of an instance built before the silent renew.> endpoint.
    private static readonly HashSet<string> OldEndpoints = ["card", "ping", "chat", "cancel"];

    /// <summary>
    /// The Go <c>startOldInstance</c>: joins <paramref name="name"/>'s service queue group the
    /// way an instance built before the silent renew.&gt; endpoint does (e.g. the previous
    /// release during a rolling deploy, or a regional twin on an older build): same base path
    /// and queue, standard endpoints only (kept silent here). It answers a renew subject with
    /// the framework's 404 envelope.
    /// </summary>
    private async Task<Counter> StartOldInstanceAsync(string name)
    {
        var basePath = Protocol.AgentPrefix + "." + name;
        var answered = new Counter();
        var nc = await ConnectAsync();
        var sub = await nc.SubscribeCoreAsync<byte[]>(basePath + ".>", queueGroup: "agent." + name);
        Cleanup(sub.DisposeAsync);
        _ = Task.Run(async () =>
        {
            await foreach (var msg in sub.Msgs.ReadAllAsync())
            {
                if (OldEndpoints.Contains(msg.Subject[(basePath.Length + 1)..]) || string.IsNullOrEmpty(msg.ReplyTo))
                {
                    continue;
                }

                // What the service framework replies for a subject no endpoint matches.
                var error = NatsServiceException.NotFound(msg.Subject).Error;
                answered.Increment();
                await msg.ReplyAsync(WireJson.Serialize(error), new NatsHeaders { ["status"] = "404" });
            }
        });
        await nc.PingAsync();
        return answered;
    }

    // Reproduction of the E2E failure: the run's instance applies the renew, but an older
    // instance in the same queue group answers first with a 404 envelope, which the caller
    // read as a rejection.
    [Fact]
    public async Task RenewWithOlderInstanceInQueueGroup()
    {
        var subj = RenewIdentitySubject();
        await StartRenewIdentityAsync(subj);
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj });
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turns);
        var applied = 0;
        turn.OnRenew(_ => Interlocked.Increment(ref applied));
        var old = await StartOldInstanceAsync(a.Name); // after the ack: the run lives on a

        var body = WireJson.Serialize(new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-alice2.c" });
        foreach (var r in await AllRepliesAsync(run.Ack.Caps!.RenewSubject!, body, TimeSpan.FromMilliseconds(300)))
        {
            output.WriteLine($"raw reply: {r}");
        }

        for (var i = 0; i < 20; i++)
        {
            try
            {
                await run.RenewAsync("IDT-alice2.c");
            }
            catch (Exception ex)
            {
                Assert.Fail($"renew {i}: {ex.Message} (the run's instance applied it: {Volatile.Read(ref applied)} so far)");
            }
        }

        // Every renew reached the run's instance, whoever else answered.
        Assert.Equal(21, Volatile.Read(ref applied));

        // The scenario is real: the queue group handed the old instance its share of the 21
        // renews (each one goes to it or to a's silent service with equal odds).
        output.WriteLine($"old instance answered {old.Value} of 21 renews with its 404 envelope");
        Assert.True(old.Value > 0, "the old instance never received a renew: it is not in the agent's queue group");
        a.Release();
        await DrainAsync(run);
    }

    // Two current instances: the run lives on a; every renew is answered once, by a's run
    // subscription; b's service stays silent.
    [Fact]
    public async Task RenewTwoInstancesOneReply()
    {
        var subj = RenewIdentitySubject();
        await StartRenewIdentityAsync(subj);
        var (a, turnsA) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj });
        var run = await ChatAsAsync(a, "IDT-alice1.c");
        var turn = await WaitAsync(turnsA);
        var applied = 0;
        turn.OnRenew(_ => Interlocked.Increment(ref applied));
        var (_, turnsB) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Identity = subj, Name = a.Name });

        var body = WireJson.Serialize(new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-alice2.c" });
        for (var i = 0; i < 20; i++)
        {
            var got = await AllRepliesAsync(run.Ack.Caps!.RenewSubject!, body, TimeSpan.FromMilliseconds(200));
            Assert.True(got.Count == 1, $"renew {i}: {got.Count} replies, want exactly 1: {string.Join(" | ", got)}");
            Assert.True(
                got[0].Body == """{"applied":true}""" && got[0].HeaderCount == 0,
                $"renew {i}: reply {got[0]}, want the run's {{\"applied\":true}}");
        }

        Assert.Equal(20, Volatile.Read(ref applied)); // instance a applied every renew
        Assert.False(turnsB.TryRead(out var tb), $"instance b ran a turn: {tb?.RunId}");
        a.Release();
        await DrainAsync(run);
    }

    // A renew to an ended run gets no reply at all from any current instance.
    [Fact]
    public async Task RenewEndedRunTwoInstancesNoReply()
    {
        var (a, turns) = await StartRenewAgentAsync(new RenewAgentOptions { Renew = true });
        var run = await ChatAsAsync(a, "IDT-x1.c");
        await WaitAsync(turns);
        await StartRenewAgentAsync(new RenewAgentOptions { Renew = true, Name = a.Name });
        a.Release();
        await DrainAsync(run);

        var body = WireJson.Serialize(new RenewRequest { RunId = run.Ack.RunId, Idt = "IDT-x2.c" });
        for (var i = 0; i < 5; i++)
        {
            var got = await AllRepliesAsync(run.Ack.Caps!.RenewSubject!, body, TimeSpan.FromMilliseconds(200));
            Assert.True(got.Count == 0, $"renew to ended run: {got.Count} replies, want none: {string.Join(" | ", got)}");
        }
    }
}
