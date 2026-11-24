using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>Port of regional_test.go: the regional alias of an agent (<c>&lt;name&gt;_&lt;region&gt;</c>).</summary>
public sealed class RegionalTests(NatsServerFixture nats) : NatsTestBase(nats)
{
    [Fact]
    public async Task RegionalRoutesKeepSessionsAndTurnsInTheirRegion()
    {
        const string name = "regionalFixture";
        foreach (var region in new[] { "east", "west" })
        {
            var store = new MemSessionStore();
            store.Put("alice", new SessionGetResponse { SessionId = "session", Title = region });
            var agent = await NewAgentAsync(new AgentConfig
            {
                Name = name, Region = region, Description = "regional test", IdtValidation = new IdtValidation { Enabled = false },
            });
            agent.UseSessionStore(store);
            agent.OnChat(async (turn, stream, ct) =>
            {
                if (turn.Message.Content[0].Text == "block")
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }

                await stream.TextAsync(region);
            });
            await agent.StartAsync();
        }

        var client = await AgentClientAsync();
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var ct = limit.Token;

        foreach (var region in new[] { "east", "west" })
        {
            var alias = name + "_" + region;

            // Ping waits for the regional subscriptions to be ready.
            var deadline = DateTime.UtcNow.AddSeconds(3);
            while (true)
            {
                try
                {
                    await client.PingAsync(alias, ct);
                    break;
                }
                catch (Exception) when (DateTime.UtcNow < deadline)
                {
                    await Task.Delay(10, ct);
                }
            }

            for (var i = 0; i < 5; i++)
            {
                var got = await client.SessionGetAsync(alias, "alice", "session", ct);
                Assert.True(got.Title == region, $"{region} session: reached {got.Title}");
            }

            // A foreign session is not accessible.
            await Assert.ThrowsAnyAsync<Exception>(() => client.SessionGetAsync(alias, "bob", "session", ct));

            var run = await client.ChatAsync(alias, UserMessage("hello", sessionId: "session", userId: "alice"), cancellationToken: ct);
            var text = string.Concat((await DrainAsync(run)).Select(ev => ev.TextDelta));
            Assert.True(text == region, $"turn reached \"{text}\", wanted \"{region}\"");
        }

        var blocked = await client.ChatAsync(name + "_west", UserMessage("block", sessionId: "session", userId: "alice"), cancellationToken: ct);
        Assert.True(await client.CancelAsync(name + "_west", blocked.Ack.RunId, ct), "regional cancel");
        await DrainAsync(blocked);

        var cards = await client.DiscoverAsync(new DiscoverFilter(), TimeSpan.FromMilliseconds(200), ct);
        Assert.DoesNotContain(cards, card => card.Name == name + "_east" || card.Name == name + "_west"); // regional route advertised as separate agent
        Assert.Equal(1, cards.Count(card => card.Name == name)); // logical agent count
    }

    [Fact]
    public async Task RegionalRoutesEnforceIdentity()
    {
        const string subject = "test.identity.validate.RegionalRoutesEnforceIdentity";
        await StartFakeIdentityAsync(subject, new Dictionary<string, string> { ["IDT-good.c"] = "alice" });
        var store = new MemSessionStore();
        store.Put("alice", new SessionGetResponse { SessionId = "alice-session" });
        var agent = await NewAgentAsync(new AgentConfig
        {
            Name = "regionalAuth",
            Region = "east",
            Description = "secured regional route",
            Access = new AgentAccess { AppId = "appTest", FunctionId = "fnTest" },
            IdtValidation = new IdtValidation { Enabled = true, Subject = subject, CacheTtl = TimeSpan.Zero },
        });
        agent.UseSessionStore(store);
        agent.OnChat((_, _, _) => Task.CompletedTask);
        await agent.StartAsync();

        var client = await AgentClientAsync();
        await AssertForbiddenAsync(() => client.SessionGetAsync("regionalAuth_east", "alice", "alice-session"), "MISSING_IDT");

        using (CallContext.WithIdt("IDT-bad.c"))
        {
            await AssertForbiddenAsync(() => client.SessionGetAsync("regionalAuth_east", "alice", "alice-session"), "DENIED_FN");
        }

        using (CallContext.WithIdt("IDT-good.c"))
        {
            // The verified user replaces the caller's identity.
            var got = await client.SessionGetAsync("regionalAuth_east", "forged-user", "alice-session");
            Assert.Equal("alice-session", got.SessionId);
        }

        await AssertForbiddenAsync(() => client.ChatAsync("regionalAuth_east", UserMessage("denied")), "MISSING_IDT");
    }
}
