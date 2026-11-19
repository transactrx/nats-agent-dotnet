using System.Text.Json;
using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Tools;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>Port of e2e_test.go: agents, tools and IDT enforcement against a real server.</summary>
public sealed class E2ETests(NatsServerFixture nats) : NatsTestBase(nats)
{
    private static readonly TimeSpan Window = TimeSpan.FromMilliseconds(500);

    // ─── agent tests ──────────────────────────────────────────────────────

    [Fact]
    public async Task AgentDiscoveryAndCard()
    {
        await StartTestAgentAsync();
        var c = await AgentClientAsync();

        var cards = await c.DiscoverAsync(new DiscoverFilter(), Window);
        var found = Assert.Single(cards, card => card.Name == "testAgent");
        Assert.Equal(Protocol.KindAgent, found.Kind);
        Assert.Equal(Protocol.Version, found.ProtocolVersion);
        Assert.True(found.Capabilities.Streaming);
        Assert.True(found.Capabilities.Sessions);
        Assert.False(found.Capabilities.Sync);

        // Filters: skill match, skill mismatch.
        Assert.Single(await c.DiscoverAsync(new DiscoverFilter { Skill = "echoing" }, Window));
        Assert.Empty(await c.DiscoverAsync(new DiscoverFilter { Skill = "nope" }, Window));

        var direct = await c.CardAsync("testAgent");
        Assert.NotEmpty(direct.Description);
        Assert.NotEmpty(direct.Endpoints!);

        var ping = await c.PingAsync("testAgent");
        Assert.Equal("ok", ping.Status);
        Assert.Equal("testAgent", ping.Name);
        Assert.NotEmpty(ping.InstanceId);
    }

    [Fact]
    public async Task AgentChatStream()
    {
        await StartTestAgentAsync();
        var c = await AgentClientAsync();

        var run = await c.ChatAsync("testAgent", UserMessage("hello", userId: "tester"));
        Assert.True(run.Ack.Accepted);
        Assert.NotEmpty(run.Ack.RunId);
        Assert.NotEmpty(run.Ack.SessionId);

        var events = await DrainAsync(run);
        var lastSeq = -1;
        var text = "";
        foreach (var ev in events)
        {
            Assert.True(ev.Seq > lastSeq, $"seq not increasing: {ev.Seq} after {lastSeq} (type {ev.Type})");
            lastSeq = ev.Seq;
            Assert.Equal(run.Ack.RunId, ev.RunId);
            if (ev.Type == EventTypes.Text)
            {
                text += ev.TextDelta;
            }
        }

        Assert.Equal(
            [EventTypes.Start, EventTypes.Status, EventTypes.ToolUse, EventTypes.ToolResult, EventTypes.Text, EventTypes.Text, EventTypes.Done],
            events.Select(e => e.Type));
        Assert.Equal("you said: hello", text);
        var final = events[^1];
        Assert.Equal(StopReasons.EndTurn, final.StopReason);
        Assert.Equal(3, final.Usage?.TotalTokens);
        Assert.Equal(run.Ack.SessionId, events[0].SessionId);
    }

    [Fact]
    public async Task AgentChatValidation()
    {
        await StartTestAgentAsync();
        var c = await AgentClientAsync();

        // An empty message fails client-side.
        await Assert.ThrowsAsync<ArgumentException>(() =>
            c.ChatAsync("testAgent", new ChatRequest { Message = new Message { Role = "user", Content = [] } }));

        // Force a server-side rejection: bypass the client check.
        var nc = await ConnectAsync();
        var reply = await nc.RequestAsync<byte[], byte[]>(
            Protocol.AgentSubject("testAgent", "chat"), """{"streamSubject":"x.y"}"""u8.ToArray(),
            replyOpts: new NatsSubOpts { Timeout = TimeSpan.FromSeconds(5) });
        var error = JsonSerializer.Deserialize<NatsServiceError>(reply.Data!)!;
        Assert.Equal((400, Codes.MissingField), (error.Status, error.ApiStatusCode));
    }

    [Fact]
    public async Task AgentCancel()
    {
        await StartTestAgentAsync();
        var c = await AgentClientAsync();

        var run = await c.ChatAsync("testAgent", UserMessage("block"));

        // First event (start) proves the run is live before we cancel.
        var first = await run.Events.ReadAsync();
        Assert.Equal(EventTypes.Start, first.Type);

        Assert.True(await run.CancelAsync(), "cancel not acknowledged by owner");

        var final = (await DrainAsync(run))[^1];
        Assert.Equal((EventTypes.Done, StopReasons.Cancelled), (final.Type, final.StopReason));

        // Cancelling an unknown run returns false without error.
        Assert.False(await c.CancelAsync("testAgent", "run_does_not_exist"));
    }

    [Fact]
    public async Task AgentSessions()
    {
        var (_, store) = await StartTestAgentAsync();
        var c = await AgentClientAsync();

        store.Put("alice", new SessionGetResponse
        {
            SessionId = "s1",
            Title = "First chat",
            Messages =
            [
                new StoredMessage { Role = "user", Content = [new ContentBlock { Text = "hi" }] },
                new StoredMessage { Role = "assistant", Content = [new ContentBlock { Text = "hello" }] },
            ],
        });

        var list = await c.SessionsListAsync("testAgent", "alice");
        Assert.Equal("s1", Assert.Single(list.Sessions).SessionId);

        var got = await c.SessionGetAsync("testAgent", "alice", "s1");
        Assert.Equal(2, got.Messages.Count);
        Assert.Equal("hello", got.Messages[1].Content[0].Text);

        await c.SessionRenameAsync("testAgent", "alice", "s1", "Renamed");
        await c.SessionSetFavoriteAsync("testAgent", "alice", "s1", true);
        got = await c.SessionGetAsync("testAgent", "alice", "s1");
        Assert.Equal("Renamed", got.Title);
        Assert.True(got.Favorite);

        // Unknown session maps to 404/4041.
        var missing = await Assert.ThrowsAsync<NatsServiceException>(() => c.SessionGetAsync("testAgent", "alice", "missing"));
        Assert.Equal((404, Codes.UnknownSession), (missing.Status, missing.ApiStatusCode));

        await c.SessionDeleteAsync("testAgent", "alice", "s1");
        Assert.Empty((await c.SessionsListAsync("testAgent", "alice")).Sessions);
    }

    // ─── tool tests ───────────────────────────────────────────────────────

    [Fact]
    public async Task ToolHostAndClient()
    {
        var h = NewHost();
        h.Register(new EchoTool(), new ToolInfo { Version = "1.0.0", Tags = ["test"] });
        h.Register(new FailTool());
        await h.StartAsync();

        var tc = await ToolClientAsync();

        var cards = await tc.DiscoverAsync(new DiscoverFilter(), Window);
        Assert.All(cards, card => Assert.Equal(Protocol.KindTool, card.Kind));
        Assert.Contains(cards, card => card.Name == "echo");
        Assert.Contains(cards, card => card.Name == "alwaysfail");

        var echo = await tc.CardAsync("echo");
        Assert.NotNull(echo.InputSchema);
        Assert.Equal(30, echo.TimeoutSeconds);

        var resp = await tc.RunAsync("echo", new ToolRunRequest
        {
            ToolUseId = "tu_9",
            Input = WireJson.Parse("""{"say":"ping"}"""),
            UserId = "tester",
            Agent = "testAgent",
        });
        Assert.Equal(ToolStatuses.Success, resp.Status);
        Assert.Equal("tu_9", resp.ToolUseId);
        var content = Assert.Single(resp.Content!);
        Assert.Equal("ping", JsonDocument.Parse(content.Text!).RootElement.GetProperty("say").GetString());

        // Execution failure is model-visible data, not a protocol error.
        resp = await tc.RunAsync("alwaysfail", new ToolRunRequest());
        Assert.Equal((ToolStatuses.Error, "intentional failure"), (resp.Status, resp.Error));
    }

    [Fact]
    public async Task ToolRegistryPickup()
    {
        var h = NewHost();
        h.Register(new EchoTool());
        await h.StartAsync();

        var tc = await ToolClientAsync();
        var changes = new Counter();
        var reg = Track(new ToolRegistry(tc, new ToolRegistryOptions
        {
            Deny = ["alwaysfail"],
            SweepInterval = TimeSpan.FromHours(1), // announcements only after the initial sweep
            OnChange = changes.Increment,
        }));
        await reg.StartAsync();

        Assert.True(reg.TryGet("echo", out _), "initial sweep missed echo");

        // A tool deployed later announces itself and appears without a sweep; the denied tool
        // is filtered even though it announces too.
        var h2 = NewHost();
        h2.Register(new FailTool());
        h2.Register(new RenamedEcho("echo_late"));
        await h2.StartAsync();

        await EventuallyAsync(() => reg.TryGet("echo_late", out _), "announced tool echo_late never reached the registry", TimeSpan.FromSeconds(3));
        Assert.False(reg.TryGet("alwaysfail", out _), "denied tool leaked into the registry");
        Assert.True(changes.Value >= 2);

        // Running through the registry uses the card's advisory timeout.
        var resp = await reg.RunAsync("echo_late", new ToolRunRequest { Input = WireJson.Parse("""{"say":"hi"}""") });
        Assert.Equal(ToolStatuses.Success, resp.Status);
        await Assert.ThrowsAsync<KeyNotFoundException>(() => reg.RunAsync("unknown", new ToolRunRequest()));
    }

    // ─── IDT enforcement ──────────────────────────────────────────────────

    /// <summary>
    /// The Go <c>startSecuredAgent</c>: returns the session store and a getter for the
    /// "userId|verified|idt" the handlers observed, in call order.
    /// </summary>
    private async Task<(MemSessionStore Store, Func<List<string>> Seen)> StartSecuredAgentAsync(string name, string identitySubject, bool observe)
    {
        var store = new MemSessionStore();
        var seen = new List<string>();
        void Record(Turn turn)
        {
            lock (seen)
            {
                seen.Add($"{turn.UserId}|{(turn.Identity.Verified ? "true" : "false")}|{turn.Identity.Idt}");
            }
        }

        var agent = await NewAgentAsync(new AgentConfig
        {
            Name = name,
            Description = "Secured agent for IDT tests.",
            Access = new AgentAccess { AppId = "appTest", FunctionId = "fnTest" },
            IdtValidation = new IdtValidation
            {
                Enabled = true, ObserveOnly = observe, Subject = identitySubject, Timeout = TimeSpan.FromSeconds(2), CacheTtl = TimeSpan.FromMinutes(1),
            },
        });
        agent.UseSessionStore(store);
        agent.OnChat(async (turn, stream, ct) =>
        {
            Record(turn);
            await stream.TextAsync("ok");
            await stream.DoneAsync(StopReasons.EndTurn);
        });
        agent.OnInvoke((turn, ct) =>
        {
            Record(turn);
            return Task.FromResult(new InvokeResponse { Message = Message.AssistantText("ok") });
        });
        await agent.StartAsync();
        return (store, () =>
        {
            lock (seen)
            {
                return [.. seen];
            }
        });
    }

    private static string IdentitySubject([System.Runtime.CompilerServices.CallerMemberName] string test = "") => "test.identity.validate." + test;

    [Fact]
    public async Task IdtChatAllowedDeniedMissing()
    {
        var subject = IdentitySubject();
        var calls = await StartFakeIdentityAsync(subject, new Dictionary<string, string> { ["IDT-good.c"] = "alice" });
        var (_, seen) = await StartSecuredAgentAsync("securedAgent", subject, observe: false);
        var c = await AgentClientAsync();

        // allowed: verified user overrides caller-asserted userId
        using (CallContext.WithIdt("IDT-good.c"))
        {
            await DrainAsync(await c.ChatAsync("securedAgent", UserMessage("hi", sessionId: "s1", userId: "mallory")));
        }

        Assert.Equal(["alice|true|IDT-good.c"], seen());

        // denied function → 403 before ack
        using (CallContext.WithIdt("IDT-bad.c"))
        {
            await AssertForbiddenAsync(() => c.ChatAsync("securedAgent", UserMessage("hi")), "DENIED_FN");
        }

        // revoked token
        using (CallContext.WithIdt("revoked.x"))
        {
            await AssertForbiddenAsync(() => c.ChatAsync("securedAgent", UserMessage("hi")), "TOKEN_REVOKED");
        }

        // missing header
        await AssertForbiddenAsync(() => c.ChatAsync("securedAgent", UserMessage("hi")), "MISSING_IDT");

        // card is public and carries access
        var card = await c.CardAsync("securedAgent");
        Assert.Equal("fnTest", card.Access?.FunctionId);
        Assert.True(calls.Value >= 3, $"identity should have been consulted, calls={calls.Value}");
    }

    [Fact]
    public async Task IdtInvokeDenied()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string>());
        var (_, seen) = await StartSecuredAgentAsync("securedInvoke", subject, observe: false);
        var c = await AgentClientAsync();

        using (CallContext.WithIdt("IDT-bad.c"))
        {
            await AssertForbiddenAsync(() => c.InvokeAsync("securedInvoke", UserMessage("hi")), "DENIED_FN");
        }

        Assert.Empty(seen()); // a denied invoke never reaches the handler
    }

    [Fact]
    public async Task IdtSessionsGatedAndUserOverridden()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string> { ["IDT-good.c"] = "alice" });
        var (store, _) = await StartSecuredAgentAsync("securedSessions", subject, observe: false);
        store.Put("alice", new SessionGetResponse { SessionId = "s-alice", Title = "mine" });
        store.Put("bob", new SessionGetResponse { SessionId = "s-bob", Title = "his" });
        var c = await AgentClientAsync();

        using (CallContext.WithIdt("IDT-good.c"))
        {
            // bob's id in the body, alice's token → alice's sessions
            var resp = await c.SessionsListAsync("securedSessions", "bob");
            Assert.Equal("s-alice", Assert.Single(resp.Sessions).SessionId);

            // The override applies on a non-list handler too: body says bob, but s-alice only
            // exists under alice, and alice's token verifies.
            var got = await c.SessionGetAsync("securedSessions", "bob", "s-alice");
            Assert.Equal(("s-alice", "mine"), (got.SessionId, got.Title));
        }

        await AssertForbiddenAsync(() => c.SessionsListAsync("securedSessions", "alice"), "MISSING_IDT");
    }

    [Fact]
    public async Task IdtSessionDeleteDeniedPreservesSession()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string> { ["IDT-good.c"] = "alice" });
        var (store, _) = await StartSecuredAgentAsync("securedSessionDelete", subject, observe: false);
        store.Put("alice", new SessionGetResponse { SessionId = "s-alice", Title = "mine" });
        var c = await AgentClientAsync();

        using (CallContext.WithIdt("IDT-bad.c"))
        {
            await AssertForbiddenAsync(() => c.SessionDeleteAsync("securedSessionDelete", "alice", "s-alice"), "DENIED_FN");
        }

        // The denied delete did not touch the store.
        var got = await store.GetAsync("alice", "s-alice", CancellationToken.None);
        Assert.Equal(("s-alice", "mine"), (got.SessionId, got.Title));
    }

    [Fact]
    public async Task IdtObserveOnlyPassesThrough()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string>());
        var (_, seen) = await StartSecuredAgentAsync("observeAgent", subject, observe: true);
        var c = await AgentClientAsync();

        // observe-only never blocks, and keeps the caller's userId, unverified
        await DrainAsync(await c.ChatAsync("observeAgent", UserMessage("hi", userId: "u")));
        Assert.Equal(["u|false|"], seen());
    }

    [Fact]
    public async Task IdtMisconfigRejectedAtCreate()
    {
        // IDT_VALIDATION without Access must fail creation.
        await Assert.ThrowsAsync<ArgumentException>(() => Agent.CreateAsync(new AgentConfig
        {
            Name = "bad", Description = "d", Nats = Settings, LoggerFactory = Quiet,
            IdtValidation = new IdtValidation { Enabled = true },
        }));
    }

    /// <summary>
    /// A denied chat request never causes any event to reach the caller-provided
    /// streamSubject: the deny happens before the ack (and before the stream even exists).
    /// </summary>
    [Fact]
    public async Task IdtChatDenyDoesNotPublishToStream()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string>());
        await StartSecuredAgentAsync("securedStreamDeny", subject, observe: false);

        var nc = await ConnectAsync();
        const string streamSubject = "test.stream.IdtChatDenyDoesNotPublishToStream";
        await using var sub = await nc.SubscribeCoreAsync<byte[]>(streamSubject);
        await nc.PingAsync();

        var c = await AgentClientAsync();
        using (CallContext.WithIdt("IDT-bad.c"))
        {
            await AssertForbiddenAsync(
                () => c.ChatAsync("securedStreamDeny", UserMessage("hi") with { StreamSubject = streamSubject }), "DENIED_FN");
        }

        await Task.Delay(300);
        Assert.False(sub.Msgs.TryRead(out _), "denied chat must not publish to streamSubject");
    }

    /// <summary>
    /// discover/card/ping stay unauthenticated even when the caller holds an IDT: the client
    /// must never attach X-TRX-IDT to those requests.
    /// </summary>
    [Fact]
    public async Task IdtCardAndPingNeverSendHeader()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string> { ["IDT-good.c"] = "alice" });
        await StartSecuredAgentAsync("cardPingNoHeader", subject, observe: false);

        var nc = await ConnectAsync();
        await using var cardSub = await nc.SubscribeCoreAsync<byte[]>(Protocol.AgentSubject("cardPingNoHeader", "card"));
        await using var pingSub = await nc.SubscribeCoreAsync<byte[]>(Protocol.AgentSubject("cardPingNoHeader", "ping"));
        await nc.PingAsync();

        var c = await AgentClientAsync();
        using (CallContext.WithIdt("IDT-good.c"))
        {
            await c.CardAsync("cardPingNoHeader");
            await c.PingAsync("cardPingNoHeader");
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var cardMsg = await cardSub.Msgs.ReadAsync(cts.Token);
        Assert.False(cardMsg.Headers?.ContainsKey(Protocol.HeaderIdt) ?? false, "card must never carry X-TRX-IDT");
        var pingMsg = await pingSub.Msgs.ReadAsync(cts.Token);
        Assert.False(pingMsg.Headers?.ContainsKey(Protocol.HeaderIdt) ?? false, "ping must never carry X-TRX-IDT");
    }

    /// <summary>
    /// An AddEndpoint extension can enforce IDT itself via the Authorize helper: a raw request
    /// with no header gets the standard 403 envelope, and a request with a granted header
    /// reaches the handler.
    /// </summary>
    [Fact]
    public async Task ExtensionEndpointAuthorize()
    {
        var subject = IdentitySubject();
        await StartFakeIdentityAsync(subject, new Dictionary<string, string> { ["IDT-good.c"] = "alice" });

        var agent = await NewAgentAsync(new AgentConfig
        {
            Name = "securedExtension",
            Description = "Secured agent with an extension endpoint.",
            Access = new AgentAccess { AppId = "appTest", FunctionId = "fnTest" },
            IdtValidation = new IdtValidation { Enabled = true, Subject = subject, Timeout = TimeSpan.FromSeconds(2), CacheTtl = TimeSpan.FromMinutes(1) },
        });
        agent.OnChat((turn, stream, ct) => stream.DoneAsync(StopReasons.EndTurn).AsTask());
        agent.AddEndpoint(new EndpointRegistration
        {
            Path = "secret",
            Description = "Extension endpoint gated by Authorize.",
            Handler = async (request, ct) =>
            {
                await agent.AuthorizeAsync(request, cancellationToken: ct);
                return """{"ok":true}"""u8.ToArray();
            },
        });
        await agent.StartAsync();

        var nc = await ConnectAsync();
        var target = Protocol.AgentSubject("securedExtension", "secret");
        var timeout = new NatsSubOpts { Timeout = TimeSpan.FromSeconds(2) };

        // No IDT header at all: denied with the standard error envelope (403 / 4031), same as
        // a built-in endpoint would produce.
        var denied = await nc.RequestAsync<byte[], byte[]>(target, "{}"u8.ToArray(), replyOpts: timeout);
        var envelope = JsonSerializer.Deserialize<NatsServiceError>(denied.Data!)!;
        Assert.Equal((403, Codes.Forbidden, "MISSING_IDT"), (envelope.Status, envelope.ApiStatusCode, envelope.ErrorMessage));

        // Allowed IDT header: the handler runs and its body comes back.
        var allowed = await nc.RequestAsync<byte[], byte[]>(
            target, "{}"u8.ToArray(), headers: new NatsHeaders { [Protocol.HeaderIdt] = "IDT-good.c" }, replyOpts: timeout);
        Assert.Equal("""{"ok":true}""", System.Text.Encoding.UTF8.GetString(allowed.Data!));
    }
}
