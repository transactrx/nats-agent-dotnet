using System.Diagnostics;
using System.Text.Json;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Tools;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>Port of tool_idt_test.go: IDT enforcement, capacity and cancellation on tool runs.</summary>
public sealed class ToolIdtTests(NatsServerFixture nats) : NatsTestBase(nats)
{
    /// <summary>
    /// The Go <c>whoamiTool</c>, an <see cref="ICallTool"/>: it reports the call it received
    /// and the token forwarded to it as the ambient one.
    /// </summary>
    private sealed class WhoamiTool(Task? block = null) : ICallTool
    {
        private readonly List<ToolCall> _calls = [];

        public string Name => "whoami";

        public string Description => "Reports the verified caller.";

        public JsonElement InputSchema { get; } = WireJson.Parse("""{"type":"object"}""");

        public Task<string> RunAsync(JsonElement input, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Run must not be called on a CallTool");

        public async Task<IReadOnlyList<ToolResultContent>> RunCallAsync(ToolCall call, CancellationToken cancellationToken)
        {
            if (block is not null)
            {
                await block; // when set, the run waits on it (and on nothing else)
            }

            lock (_calls)
            {
                _calls.Add(call);
            }

            return
            [
                new ToolResultContent { Text = call.Identity.UserId + "|" + call.SessionId + "|" + CallContext.Idt },
                new ToolResultContent { Json = WireJson.Parse(call.Identity.Verified ? """{"verified":true}""" : """{"verified":false}""") },
            ];
        }

        public List<ToolCall> Seen()
        {
            lock (_calls)
            {
                return [.. _calls];
            }
        }
    }

    /// <summary>The Go <c>startSecuredTool</c>.</summary>
    private async Task StartSecuredToolAsync(WhoamiTool tool, string identitySubject, int maxConcurrent)
    {
        var h = NewHost();
        h.SetIdtValidation(new IdtValidation { Enabled = true, Subject = identitySubject, Timeout = TimeSpan.FromSeconds(2), CacheTtl = TimeSpan.FromMinutes(1) });
        h.Register(tool, new ToolInfo { Access = new AgentAccess { AppId = "appTest", FunctionId = "fnTest" }, MaxConcurrent = maxConcurrent });
        await h.StartAsync();
    }

    /// <summary>The Go <c>assertToolError</c>: a protocol-level refusal with the given status, code and (when set) message.</summary>
    private static async Task AssertToolErrorAsync(Func<Task> call, int status, int code, string? message = null)
    {
        var ex = await Assert.ThrowsAsync<NatsServiceException>(call);
        Assert.Equal((status, code), (ex.Status, ex.ApiStatusCode));
        if (message is not null)
        {
            Assert.Equal(message, ex.ErrorMessage);
        }
    }

    private static string IdentitySubject([System.Runtime.CompilerServices.CallerMemberName] string test = "") => "test.identity.validate." + test;

    private static ToolRunRequest EmptyInput => new() { Input = WireJson.EmptyObject };

    [Fact]
    public async Task ToolRunIdtAllowedDeniedMissing()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string> { ["IDT-good.c"] = "alice" });
        var w = new WhoamiTool();
        await StartSecuredToolAsync(w, subject, 0);
        var tc = await ToolClientAsync();

        // Allowed: the verified user replaces the caller-asserted userId, and the token reaches
        // the tool as the ambient one for onward calls.
        using (CallContext.WithIdt("IDT-good.c"))
        {
            var resp = await tc.RunAsync("whoami", new ToolRunRequest { ToolUseId = "tu1", SessionId = "s1", UserId = "mallory", Input = WireJson.EmptyObject });
            Assert.Equal(ToolStatuses.Success, resp.Status);
            Assert.NotNull(resp.Content);
            Assert.Equal(2, resp.Content.Count);
            Assert.Equal("alice|s1|IDT-good.c", resp.Content[0].Text);
            Assert.Equal("""{"verified":true}""", resp.Content[1].Json?.GetRawText());
        }

        var call = Assert.Single(w.Seen());
        Assert.Equal("tu1", call.ToolUseId);
        Assert.True(call.Identity.Verified);
        Assert.Equal("acc1", call.Identity.AccountId);
        Assert.Equal("alice", call.UserId); // .NET: the call also carries the user it is for

        foreach (var (token, reason) in new[] { ("IDT-bad.c", "DENIED_FN"), ("revoked.x", "TOKEN_REVOKED"), ("", "MISSING_IDT") })
        {
            using (CallContext.WithIdt(token))
            {
                await AssertToolErrorAsync(() => tc.RunAsync("whoami", EmptyInput), 403, Codes.Forbidden, reason);
            }
        }

        Assert.True(w.Seen().Count == 1, $"denied runs must not execute the tool, ran {w.Seen().Count} times");

        // The card is public and carries access and the protocol version.
        var card = await tc.CardAsync("whoami");
        Assert.NotNull(card.Access);
        Assert.Equal("fnTest", card.Access.FunctionId);
        Assert.Equal(Protocol.Version, card.ProtocolVersion);
    }

    [Fact]
    public async Task ToolHostRefusesValidationWithoutAccess()
    {
        var h = NewHost();
        h.SetIdtValidation(new IdtValidation { Enabled = true });
        h.Register(new EchoTool());

        // Start must refuse IDT validation for a tool without Access.
        await Assert.ThrowsAsync<InvalidOperationException>(() => h.StartAsync());

        // Register must refuse a partial Access.
        Assert.Throws<ArgumentException>(() => h.Register(new WhoamiTool(), new ToolInfo { Access = new AgentAccess { AppId = "a" } }));
    }

    [Fact]
    public async Task ToolRunBusyAndContextCancel()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string> { ["IDT-good.c"] = "alice" });

        // Continuations run inline on completion, so the first run has returned (and freed its
        // slot) by the time the test goes on after releasing it.
        var block = new TaskCompletionSource();
        var w = new WhoamiTool(block.Task);
        await StartSecuredToolAsync(w, subject, 1);
        var tc = await ToolClientAsync();

        using (CallContext.WithIdt("IDT-good.c"))
        {
            // The first run occupies the only slot. The caller gives up after 300ms: the run
            // must return promptly with the cancellation.
            using var shortLived = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
            var started = Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => tc.RunAsync("whoami", EmptyInput, cancellationToken: shortLived.Token));
            Assert.True(started.Elapsed < TimeSpan.FromSeconds(3), $"want prompt cancellation, got it after {started.Elapsed}");
            Assert.True(started.Elapsed >= TimeSpan.FromMilliseconds(250), $"the run returned before the caller gave up ({started.Elapsed})");

            // The tool is still running (it is not interrupted), so a second run is refused for
            // capacity.
            await AssertToolErrorAsync(() => tc.RunAsync("whoami", EmptyInput, TimeSpan.FromSeconds(5)), 429, Codes.Busy);

            block.SetResult();
            await EventuallyAsync(() => w.Seen().Count == 1, "the first run never finished");

            // The slot must free after the first run finishes.
            var resp = await tc.RunAsync("whoami", EmptyInput, TimeSpan.FromSeconds(5));
            Assert.Equal(ToolStatuses.Success, resp.Status);
        }
    }
}
