using System.Text;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>Port of usercontext_test.go: the user's display context from caller to agent to agent.</summary>
public sealed class UserContextEndToEndTests(NatsServerFixture nats) : NatsTestBase(nats)
{
    /// <summary>
    /// The Go <c>startContextAgent</c>: answers with its ContextLine, or — when asked
    /// "consult" — with the answer of agent <paramref name="next"/>, consulted from inside the
    /// handler with no explicit forwarding of the context.
    /// </summary>
    private async Task StartContextAgentAsync(string name, string next)
    {
        var agent = await NewAgentAsync(new AgentConfig { Name = name, Description = "user context test agent", UserContext = true });
        agent.OnChat(async (turn, stream, ct) =>
        {
            if (turn.Message.Content[0].Text != "consult")
            {
                await stream.TextAsync(turn.ContextLine() + " | loc=" + turn.TimeZone.Id);
                return;
            }

            var c = new AgentClient(agent.Connection, Quiet);
            var run = await c.ChatAsync(next, new ChatRequest { Message = Message.UserText("line") }, cancellationToken: ct);
            await foreach (var ev in run.Events.ReadAllAsync(ct))
            {
                if (ev.Type == EventTypes.Text)
                {
                    await stream.TextAsync(ev.TextDelta ?? "");
                }
            }
        });
        await agent.StartAsync();
    }

    /// <summary>The Go <c>chatText</c>: the text of one turn; an error event fails the test.</summary>
    private static async Task<string> ChatTextAsync(AgentClient c, string agent, ChatRequest request)
    {
        var run = await c.ChatAsync(agent, request);
        var text = new StringBuilder();
        foreach (var ev in await DrainAsync(run))
        {
            if (ev.Type == EventTypes.Text)
            {
                text.Append(ev.TextDelta);
            }

            Assert.False(ev.Type == EventTypes.Error, "agent error: " + ev.Error);
        }

        return text.ToString();
    }

    [Fact]
    public async Task UserContextEndToEnd()
    {
        await StartContextAgentAsync("ctxFront", "ctxBack");
        await StartContextAgentAsync("ctxBack", "");
        var c = await AgentClientAsync();

        var card = await c.CardAsync("ctxBack");
        Assert.True(card.Capabilities.UserContext);
        Assert.Equal("1.5", card.ProtocolVersion);

        // Direct: the agent sees the cleaned context.
        var got = await ChatTextAsync(c, "ctxBack", new ChatRequest
        {
            Message = Message.UserText("line"),
            Context = new UserContext { TimeZone = "America/Chicago", UserName = "Jane\nDoe <x>" },
        });
        Assert.Contains("The user asking is Jane Doe x (time zone America/Chicago)", got);
        Assert.Contains("loc=America/Chicago", got);

        // Bad zone: dropped, turn still served, UTC used.
        got = await ChatTextAsync(c, "ctxBack", new ChatRequest { Message = Message.UserText("line"), Context = new UserContext { TimeZone = "Local" } });
        Assert.Contains("time zone is unknown", got);
        Assert.Contains("loc=UTC", got);

        // Via the caller's ambient context, then agent→agent consult: forwarded with no extra code.
        using (CallContext.WithUserContext(new UserContext { TimeZone = "Europe/Madrid", UserName = "Ana" }))
        {
            got = await ChatTextAsync(c, "ctxFront", new ChatRequest { Message = Message.UserText("consult") });
            Assert.Contains("The user asking is Ana (time zone Europe/Madrid)", got);
            Assert.Contains("loc=Europe/Madrid", got);

            // An explicit request context wins over the ambient one.
            got = await ChatTextAsync(c, "ctxBack", new ChatRequest { Message = Message.UserText("line"), Context = new UserContext { TimeZone = "Asia/Tokyo" } });
            Assert.Contains("loc=Asia/Tokyo", got);
        }
    }
}
