using TransactRx.NatsAgent.Tests.Support;

namespace TransactRx.NatsAgent.Tests;

/// <summary>Port of pkg/agent/card_test.go.</summary>
public sealed class AgentCardTests(NatsServerFixture nats) : NatsTestBase(nats)
{
    // The Go test builds the Agent struct directly because New() connects to NATS; a .NET
    // agent only exists connected, so it is created (not started) on the test server.
    // Card() reads only the config and the optional handlers.
    [Fact]
    public async Task CardCarriesCategory()
    {
        var a = await NewAgentAsync(new AgentConfig { Name = "x", Description = "d", Category = "Operations" });
        var card = a.Card();
        Assert.Equal("Operations", card.Category);
        Assert.Equal("1.5", card.ProtocolVersion);
    }
}
