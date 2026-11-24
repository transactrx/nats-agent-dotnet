using System.Globalization;
using System.Text.Json;
using System.Threading.Channels;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Tools;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>Port of drain_test.go: draining an agent instance and a tool host without dropping work.</summary>
public sealed class DrainTests(NatsServerFixture nats) : NatsTestBase(nats)
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private static async Task<string> NextAsync(Channel<string> served)
    {
        using var cts = new CancellationTokenSource(Patience);
        return await served.Reader.ReadAsync(cts.Token);
    }

    /// <summary>
    /// The Go <c>drainAgent</c>: one instance of a shared agent name whose chat handler
    /// reports which instance served it and blocks until released.
    /// </summary>
    private async Task<Agent> DrainAgentAsync(string name, string instance, Task release, Channel<string> served)
    {
        var agent = await NewAgentAsync(new AgentConfig { Name = name, Description = "drain test", IdtValidation = new IdtValidation() });
        agent.OnChat(async (turn, stream, ct) =>
        {
            await served.Writer.WriteAsync(instance);
            await release.WaitAsync(ct); // released, or the run is cancelled
            await stream.TextAsync(instance);
            await stream.DoneAsync(StopReasons.EndTurn);
        });
        await agent.StartAsync();
        return agent;
    }

    [Fact]
    public async Task AgentDrainFinishesInFlightAndHandsOverNewRuns()
    {
        var name = "drainAgent" + DateTime.UtcNow.ToString("HHmmssffffff", CultureInfo.InvariantCulture);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var served = Channel.CreateBounded<string>(4);
        var first = await DrainAgentAsync(name, "first", release.Task, served);
        var c = await AgentClientAsync();

        var run = await c.ChatAsync(name, UserMessage("hi"));
        Assert.Equal("first", await NextAsync(served));
        Assert.Equal(1, first.ActiveRuns);

        // A second instance joins; the first starts draining.
        await DrainAgentAsync(name, "second", release.Task, served);
        using var drainLimit = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var drained = Task.Run(() => first.DrainAsync(drainLimit.Token));
        await Task.Delay(300);

        // New runs go to the instance that is not draining.
        var run2 = await c.ChatAsync(name, UserMessage("hi"));
        var servedBy = await NextAsync(served);
        Assert.True(servedBy == "second", $"a draining instance must not take new runs; served by {servedBy}");
        Assert.False(drained.IsCompleted, "drain returned early with active runs");

        release.SetResult();
        Assert.Equal(0, await drained.WaitAsync(TimeSpan.FromSeconds(25))); // drain should end clean
        foreach (var r in new[] { run, run2 })
        {
            var events = await DrainAsync(r);
            Assert.True(events.Any(ev => ev.Type == EventTypes.Done), "in-flight run must complete during drain");
        }

        await first.ShutdownAsync();
    }

    /// <summary>The Go <c>blockingTool</c>.</summary>
    private sealed class BlockingTool(Task release, string host, Channel<string> served) : ITool
    {
        public string Name => "drainTool";

        public string Description => "blocks";

        public JsonElement InputSchema { get; } = WireJson.Parse("""{"type":"object"}""");

        public async Task<string> RunAsync(JsonElement input, CancellationToken cancellationToken)
        {
            await served.Writer.WriteAsync(host, CancellationToken.None);
            await release.WaitAsync(cancellationToken);
            return host;
        }
    }

    [Fact]
    public async Task ToolHostDrainWaitsForInFlightRuns()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var served = Channel.CreateBounded<string>(4);
        var h1 = NewHost();
        h1.Register(new BlockingTool(release.Task, "h1", served), new ToolInfo { TimeoutSeconds = 30 });
        await h1.StartAsync();

        var tc = await ToolClientAsync();
        Task<string> Call() => Task.Run(async () =>
        {
            try
            {
                var resp = await tc.RunAsync("drainTool", new ToolRunRequest { Input = WireJson.EmptyObject }, TimeSpan.FromSeconds(30));
                return resp.Content is { Count: > 0 } content ? content[0].Text ?? "" : "error: " + resp.Error;
            }
            catch (Exception ex)
            {
                return "error: " + ex.Message;
            }
        });

        var call1 = Call();
        Assert.Equal("h1", await NextAsync(served));

        var h2 = NewHost();
        h2.Register(new BlockingTool(release.Task, "h2", served), new ToolInfo { TimeoutSeconds = 30 });
        await h2.StartAsync();

        using var drainLimit = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var drained = Task.Run(() => h1.DrainAsync(drainLimit.Token));
        await Task.Delay(300);

        var call2 = Call();
        var servedBy = await NextAsync(served);
        Assert.True(servedBy == "h2", $"a draining host must not take new runs; served by {servedBy}");

        release.SetResult();
        Assert.Equal(0, await drained.WaitAsync(TimeSpan.FromSeconds(25))); // drain should end clean
        foreach (var result in await Task.WhenAll(call1, call2).WaitAsync(TimeSpan.FromSeconds(35)))
        {
            Assert.True(result is "h1" or "h2", "in-flight tool run must complete: " + result);
        }

        await h1.ShutdownAsync();
    }
}
