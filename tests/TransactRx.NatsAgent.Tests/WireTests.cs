using System.Text;
using System.Text.Json;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/wire/wire_test.go: the JSON shapes of the wire types (round trips, omitted
/// optional fields, old JSON still parsing), plus a comparison of what the C# records emit with
/// what the Go structs of pkg/wire/wire.go emit for the same values.
/// </summary>
public sealed class WireTests
{
    /// <summary>The Go <c>containsKey</c>: whether the JSON object has a top-level key.</summary>
    private static bool ContainsKey(byte[] data, string key)
    {
        using var doc = JsonDocument.Parse(data);
        return doc.RootElement.TryGetProperty(key, out _);
    }

    private static T Parse<T>(string json) => WireJson.Deserialize<T>(Encoding.UTF8.GetBytes(json))!;

    [Fact]
    public void AgentCardAccessRoundTrip()
    {
        var card = new AgentCard { Name = "a", Description = "d", Access = new AgentAccess { AppId = "appX", FunctionId = "fnX" } };
        var data = WireJson.Serialize(card);
        var back = WireJson.Deserialize<AgentCard>(data)!;
        Assert.NotNull(back.Access);
        Assert.Equal(("appX", "fnX"), (back.Access.AppId, back.Access.FunctionId));

        // Absent access must marshal to no "access" key (legacy cards stay identical).
        data = WireJson.Serialize(new AgentCard { Name = "b", Description = "d" });
        Assert.NotEmpty(data);
        Assert.False(ContainsKey(data, "access"), "empty access must be omitted: " + Encoding.UTF8.GetString(data));

        Assert.Equal(("1.5", 4031, "X-TRX-IDT"), (Protocol.Version, Codes.Forbidden, Protocol.HeaderIdt));
    }

    [Fact]
    public void AgentCardCategoryJSON()
    {
        var json = WireJson.SerializeToString(new AgentCard { Name = "a", Category = "Pharmacy" });
        Assert.Contains("\"category\":\"Pharmacy\"", json);

        // Empty category must be omitted (pre-1.4 cards stay identical).
        var data = WireJson.Serialize(new AgentCard { Name = "b" });
        Assert.False(ContainsKey(data, "category"), "empty category must be omitted: " + Encoding.UTF8.GetString(data));
    }

    [Fact]
    public void ChatRequestBudgetOptional()
    {
        var data = WireJson.Serialize(new ChatRequest { StreamSubject = "s" });
        Assert.False(ContainsKey(data, "budget"), "absent budget must be omitted: " + Encoding.UTF8.GetString(data));

        var req = Parse<ChatRequest>("""{"message":{},"streamSubject":"s","budget":{"remainingMs":3000}}""");
        Assert.NotNull(req.Budget);
        Assert.Equal(3000, req.Budget.RemainingMs);
    }

    [Fact]
    public void OldJSONStillParses()
    {
        var ack = Parse<ChatAck>("""{"accepted":true,"runId":"r","sessionId":"s","agent":"a","instanceId":"i","streamSubject":"x"}""");
        Assert.Null(ack.Caps);
        Assert.True(ack.Accepted);
        Assert.Equal("r", ack.RunId);

        var req = Parse<ChatRequest>("""{"sessionId":"s","message":{"role":"user","content":[{"text":"hi"}]},"streamSubject":"x"}""");
        Assert.Null(req.Budget);
        Assert.Equal("x", req.StreamSubject);

        var ev = Parse<AgentEvent>("""{"runId":"r","seq":3,"type":"done","stopReason":"endTurn"}""");
        Assert.Null(ev.Reason); // Go: "" — an empty optional string is null here
        Assert.Equal(StopReasons.EndTurn, ev.StopReason);

        // New fields stay off the wire when unset.
        var data = WireJson.Serialize(new ChatAck { Accepted = true });
        Assert.False(ContainsKey(data, "caps"), "absent caps must be omitted: " + Encoding.UTF8.GetString(data));
        data = WireJson.Serialize(new AgentEvent { Type = EventTypes.Done });
        Assert.False(ContainsKey(data, "reason"), "empty reason must be omitted: " + Encoding.UTF8.GetString(data));
        data = WireJson.Serialize(new Capabilities());
        Assert.False(ContainsKey(data, "progress") || ContainsKey(data, "renew"), "false progress/renew must be omitted: " + Encoding.UTF8.GetString(data));
    }

    [Fact]
    public void IsProgress()
    {
        var payload = WireJson.ToElement(new ProgressPayload { Stage = "onedrive" });
        (AgentEvent Event, bool Want)[] cases =
        [
            (new AgentEvent { Type = EventTypes.Ping }, false),
            (new AgentEvent { Type = EventTypes.Text, TextDelta = "x" }, true),
            (new AgentEvent { Type = EventTypes.ToolUse }, true),
            (new AgentEvent { Type = EventTypes.ToolResult }, true),
            (new AgentEvent { Type = EventTypes.Data, Kind = DataKinds.Progress, Payload = payload }, true),
            (new AgentEvent { Type = EventTypes.Status }, true),
            (new AgentEvent { Type = EventTypes.Start }, true),
        ];
        foreach (var (ev, want) in cases)
        {
            Assert.True(ev.IsProgress == want, $"IsProgress({ev.Type}) = {ev.IsProgress}, want {want}");
        }
    }

    [Fact]
    public void StoredMessageIncomplete()
    {
        var json = WireJson.SerializeToString(new StoredMessage { Role = "assistant", Content = [new ContentBlock { Text = "partial" }] });
        Assert.DoesNotContain("\"incomplete\"", json);

        var old = Parse<StoredMessage>("""{"role":"assistant","content":[{"text":"x"}],"timestamp":"2026-11-12T10:00:00Z"}""");
        Assert.Null(old.Incomplete); // Go: ""

        var input = new StoredMessage { Role = "assistant", Content = [new ContentBlock { Text = "partial" }], Incomplete = Reasons.TimeCap };
        json = WireJson.SerializeToString(input);
        Assert.Contains("\"incomplete\":\"time_cap\"", json);

        var output = Parse<StoredMessage>(json);
        Assert.Equal(Reasons.TimeCap, output.Incomplete);
    }

    [Fact]
    public void CancelRequestReasonCompat()
    {
        var old = Parse<CancelRequest>("""{"runId":"run_1"}""");
        Assert.Equal("run_1", old.RunId);
        Assert.Null(old.Reason); // Go: ""

        Assert.Equal("""{"runId":"run_1"}""", WireJson.SerializeToString(new CancelRequest { RunId = "run_1" }));

        var json = WireJson.SerializeToString(new CancelRequest { RunId = "run_1", Reason = Reasons.ProgressIdle });
        var back = Parse<CancelRequest>(json);
        Assert.Equal(Reasons.ProgressIdle, back.Reason);
    }

    // ─── wire compatibility with the Go structs ───────────────────────────
    //
    // The expected strings below are what Go's encoding/json produces for the same values of
    // the structs in pkg/wire/wire.go (generated with a small Go program, not written by
    // hand). Field names, field order and omit-when-empty behaviour must stay identical.

    private static void AssertShapes(IEnumerable<(string Type, object Value, string GoJson)> cases)
    {
        var mismatches = new List<string>();
        foreach (var (type, value, goJson) in cases)
        {
            var json = JsonSerializer.Serialize(value, value.GetType(), WireJson.Options);
            if (json != goJson)
            {
                mismatches.Add($"{type}:\n  .NET {json}\n  Go   {goJson}");
            }
        }

        Assert.True(mismatches.Count == 0, "JSON differs from the Go structs:\n" + string.Join("\n", mismatches));
    }

    /// <summary>
    /// Zero values: only the fields Go marks without <c>omitempty</c> are emitted. The one
    /// tolerated difference: a Go nil slice without omitempty is <c>null</c>, where the C#
    /// records default to an empty list (<c>[]</c>) — both decode to an empty slice in Go.
    /// </summary>
    [Fact]
    public void ZeroValuesOmitTheSameFieldsAsGo()
    {
        AssertShapes(
        [
            ("Message", new Message(), """{"role":"","content":[]}"""), // Go: "content":null
            ("ContentBlock", new ContentBlock(), "{}"),
            ("ImageBlock", new ImageBlock(), """{"format":"","base64":""}"""),
            ("ToolUseBlock", new ToolUseBlock(), """{"toolUseId":"","name":""}"""),
            ("ToolResultBlock", new ToolResultBlock(), """{"toolUseId":"","content":[]}"""), // Go: "content":null
            ("ToolResultContent", new ToolResultContent(), "{}"),
            ("Usage", new Usage(), """{"inputTokens":0,"outputTokens":0,"totalTokens":0}"""),
            ("DiscoverFilter", new DiscoverFilter(), "{}"),
            ("AgentCard", new AgentCard(), """{"protocolVersion":"","kind":"","name":"","description":"","capabilities":{"streaming":false,"sync":false,"sessions":false,"attachments":false}}"""),
            ("Capabilities", new Capabilities(), """{"streaming":false,"sync":false,"sessions":false,"attachments":false}"""),
            ("AgentAccess", new AgentAccess(), """{"appId":"","functionId":""}"""),
            ("Skill", new Skill(), """{"name":"","description":""}"""),
            ("PingResponse", new PingResponse(), """{"status":"","name":"","instanceId":"","uptimeSeconds":0}"""),
            ("ChatRequest", new ChatRequest(), """{"message":{"role":"","content":[]},"streamSubject":""}"""), // Go: "content":null
            ("Budget", new Budget(), """{"remainingMs":0}"""),
            ("AckCaps", new AckCaps(), "{}"),
            ("ChatAck", new ChatAck(), """{"accepted":false,"runId":"","sessionId":"","agent":"","instanceId":"","streamSubject":""}"""),
            ("ProgressPayload", new ProgressPayload(), """{"stage":""}"""),
            ("RenewRequest", new RenewRequest(), """{"runId":"","idt":""}"""),
            ("RenewResponse", new RenewResponse(), """{"applied":false}"""),
            ("AgentEvent", new AgentEvent(), """{"runId":"","seq":0,"type":""}"""),
            ("CancelRequest", new CancelRequest(), """{"runId":""}"""),
            ("CancelResponse", new CancelResponse(), """{"cancelled":false}"""),
            ("InvokeResponse", new InvokeResponse(), """{"runId":"","sessionId":"","message":{"role":"","content":[]},"stopReason":""}"""), // Go: "content":null
            ("SessionMeta", new SessionMeta(), """{"sessionId":""}"""),
            ("StoredMessage", new StoredMessage(), """{"role":"","content":[]}"""), // Go: "content":null
            ("SessionsListRequest", new SessionsListRequest(), """{"userId":""}"""),
            ("SessionsListResponse", new SessionsListResponse(), """{"sessions":[]}"""), // Go: "sessions":null
            ("SessionGetRequest", new SessionGetRequest(), """{"userId":"","sessionId":""}"""),
            ("SessionGetResponse", new SessionGetResponse(), """{"sessionId":"","messages":[]}"""), // Go: "messages":null
            ("SessionDeleteRequest", new SessionDeleteRequest(), """{"userId":"","sessionId":""}"""),
            ("SessionDeleteResponse", new SessionDeleteResponse(), """{"sessionId":"","deleted":false}"""),
            ("SessionRenameRequest", new SessionRenameRequest(), """{"userId":"","sessionId":"","title":""}"""),
            ("SessionRenameResponse", new SessionRenameResponse(), """{"sessionId":"","title":""}"""),
            ("SessionSetFavoriteRequest", new SessionSetFavoriteRequest(), """{"userId":"","sessionId":"","favorite":false}"""),
            ("SessionSetFavoriteResponse", new SessionSetFavoriteResponse(), """{"sessionId":"","favorite":false}"""),
            ("ToolCard", new ToolCard(), """{"protocolVersion":"","kind":"","name":"","description":"","inputSchema":null}"""),
            ("ToolRunRequest", new ToolRunRequest(), """{"input":null}"""),
            ("ToolRunResponse", new ToolRunResponse(), """{"status":"","latencyMs":0}"""),
            ("UserContext", new UserContext(), "{}"),
        ]);
    }

    /// <summary>Every field set: names and order are the Go struct tags'.</summary>
    [Fact]
    public void PopulatedValuesMatchGo()
    {
        var usage = new Usage { InputTokens = 1, OutputTokens = 2, TotalTokens = 3 };
        var access = new AgentAccess { AppId = "app", FunctionId = "fn" };
        var msg = new Message
        {
            Role = "user",
            Content =
            [
                new ContentBlock { Text = "t" },
                new ContentBlock { Image = new ImageBlock { Format = "png", Base64 = "QUJD" } },
                new ContentBlock { ToolUse = new ToolUseBlock { ToolUseId = "tu", Name = "n", Input = WireJson.Parse("""{"a":1}""") } },
                new ContentBlock
                {
                    ToolResult = new ToolResultBlock
                    {
                        ToolUseId = "tu",
                        Status = "success",
                        Content = [new ToolResultContent { Text = "x" }, new ToolResultContent { Json = WireJson.Parse("""{"b":2}""") }],
                    },
                },
            ],
        };
        const string msgJson = """{"role":"user","content":[{"text":"t"},{"image":{"format":"png","base64":"QUJD"}},{"toolUse":{"toolUseId":"tu","name":"n","input":{"a":1}}},{"toolResult":{"toolUseId":"tu","content":[{"text":"x"},{"json":{"b":2}}],"status":"success"}}]}""";
        var stored = new StoredMessage { Role = "assistant", Content = [new ContentBlock { Text = "t" }], Timestamp = "2026-01-01T00:00:00Z", Incomplete = "time_cap" };
        var meta = new Dictionary<string, JsonElement> { ["k"] = WireJson.ToElement("v") };

        AssertShapes(
        [
            ("Message", msg, msgJson),
            ("DiscoverFilter", new DiscoverFilter { Name = "n", Skill = "s", Tags = ["a", "b"] }, """{"name":"n","skill":"s","tags":["a","b"]}"""),
            ("AgentCard",
                new AgentCard
                {
                    ProtocolVersion = "1.5", Kind = "agent", Name = "n", DisplayName = "dn", Description = "d", Version = "v", RepositoryUrl = "r",
                    Tags = ["t"], Category = "c",
                    Capabilities = new Capabilities { Streaming = true, Sync = true, Sessions = true, Attachments = true, UserContext = true, Progress = true, Renew = true },
                    InputModalities = ["text"], OutputModalities = ["text"],
                    Skills = [new Skill { Name = "s", Description = "sd", Examples = ["e"] }],
                    Endpoints = ["card"], Metadata = meta, Access = access,
                },
                """{"protocolVersion":"1.5","kind":"agent","name":"n","displayName":"dn","description":"d","version":"v","repositoryUrl":"r","tags":["t"],"category":"c","capabilities":{"streaming":true,"sync":true,"sessions":true,"attachments":true,"userContext":true,"progress":true,"renew":true},"inputModalities":["text"],"outputModalities":["text"],"skills":[{"name":"s","description":"sd","examples":["e"]}],"endpoints":["card"],"metadata":{"k":"v"},"access":{"appId":"app","functionId":"fn"}}"""),
            ("PingResponse", new PingResponse { Status = "ok", Name = "n", InstanceId = "i", Version = "v", UptimeSeconds = 7 }, """{"status":"ok","name":"n","instanceId":"i","version":"v","uptimeSeconds":7}"""),
            ("ChatRequest",
                new ChatRequest
                {
                    SessionId = "s", UserId = "u", Message = msg, StreamSubject = "ss",
                    Metadata = new Dictionary<string, JsonElement> { ["k"] = WireJson.ToElement(1) },
                    Context = new UserContext { TimeZone = "UTC", UserName = "A", Locale = "en-US" },
                    Budget = new Budget { RemainingMs = 3000 },
                },
                """{"sessionId":"s","userId":"u","message":""" + msgJson + ""","streamSubject":"ss","metadata":{"k":1},"context":{"timeZone":"UTC","userName":"A","locale":"en-US"},"budget":{"remainingMs":3000}}"""),
            ("ChatAck",
                new ChatAck { Accepted = true, RunId = "r", SessionId = "s", Agent = "a", InstanceId = "i", StreamSubject = "ss", Caps = new AckCaps { Progress = true, RenewSubject = "rs" } },
                """{"accepted":true,"runId":"r","sessionId":"s","agent":"a","instanceId":"i","streamSubject":"ss","caps":{"progress":true,"renewSubject":"rs"}}"""),
            ("ProgressPayload", new ProgressPayload { Stage = "st", Source = "so", Detail = "de" }, """{"stage":"st","source":"so","detail":"de"}"""),
            ("RenewRequest", new RenewRequest { RunId = "r", Idt = "t", ExpiresAtMs = 5 }, """{"runId":"r","idt":"t","expiresAtMs":5}"""),
            ("RenewResponse", new RenewResponse { Applied = true, Reason = "x" }, """{"applied":true,"reason":"x"}"""),
            ("AgentEvent",
                new AgentEvent
                {
                    RunId = "r", Seq = 3, Type = "data", SessionId = "s", TextDelta = "td", ToolUseId = "tu", ToolName = "tn",
                    ToolInput = WireJson.Parse("""{"a":1}"""), ToolResult = "tr", ToolError = "te", Kind = "progress",
                    Payload = WireJson.Parse("""{"stage":"x"}"""), StatusText = "st", StopReason = "endTurn", Usage = usage,
                    Error = "e", ErrorStatus = 5002, Reason = "time_cap",
                },
                """{"runId":"r","seq":3,"type":"data","sessionId":"s","textDelta":"td","toolUseId":"tu","toolName":"tn","toolInput":{"a":1},"toolResult":"tr","toolError":"te","kind":"progress","payload":{"stage":"x"},"statusText":"st","stopReason":"endTurn","usage":{"inputTokens":1,"outputTokens":2,"totalTokens":3},"error":"e","errorStatus":5002,"reason":"time_cap"}"""),
            ("CancelRequest", new CancelRequest { RunId = "r", Reason = "progress_idle" }, """{"runId":"r","reason":"progress_idle"}"""),
            ("CancelResponse", new CancelResponse { Cancelled = true }, """{"cancelled":true}"""),
            ("InvokeResponse",
                new InvokeResponse { RunId = "r", SessionId = "s", Message = msg, StopReason = "endTurn", Usage = usage },
                """{"runId":"r","sessionId":"s","message":""" + msgJson + ""","stopReason":"endTurn","usage":{"inputTokens":1,"outputTokens":2,"totalTokens":3}}"""),
            ("SessionMeta",
                new SessionMeta { SessionId = "s", Title = "t", Favorite = true, CreatedAt = "2026-01-01T00:00:00Z", LastMessageAt = "2026-01-02T00:00:00Z" },
                """{"sessionId":"s","title":"t","favorite":true,"createdAt":"2026-01-01T00:00:00Z","lastMessageAt":"2026-01-02T00:00:00Z"}"""),
            ("StoredMessage", stored, """{"role":"assistant","content":[{"text":"t"}],"timestamp":"2026-01-01T00:00:00Z","incomplete":"time_cap"}"""),
            ("SessionsListRequest", new SessionsListRequest { UserId = "u" }, """{"userId":"u"}"""),
            ("SessionsListResponse", new SessionsListResponse { Sessions = [new SessionMeta { SessionId = "s" }] }, """{"sessions":[{"sessionId":"s"}]}"""),
            ("SessionGetRequest", new SessionGetRequest { UserId = "u", SessionId = "s" }, """{"userId":"u","sessionId":"s"}"""),
            ("SessionGetResponse",
                new SessionGetResponse { SessionId = "s", Title = "t", Favorite = true, Messages = [stored] },
                """{"sessionId":"s","title":"t","favorite":true,"messages":[{"role":"assistant","content":[{"text":"t"}],"timestamp":"2026-01-01T00:00:00Z","incomplete":"time_cap"}]}"""),
            ("SessionDeleteRequest", new SessionDeleteRequest { UserId = "u", SessionId = "s" }, """{"userId":"u","sessionId":"s"}"""),
            ("SessionDeleteResponse", new SessionDeleteResponse { SessionId = "s", Deleted = true }, """{"sessionId":"s","deleted":true}"""),
            ("SessionRenameRequest", new SessionRenameRequest { UserId = "u", SessionId = "s", Title = "t" }, """{"userId":"u","sessionId":"s","title":"t"}"""),
            ("SessionRenameResponse", new SessionRenameResponse { SessionId = "s", Title = "t" }, """{"sessionId":"s","title":"t"}"""),
            ("SessionSetFavoriteRequest", new SessionSetFavoriteRequest { UserId = "u", SessionId = "s", Favorite = true }, """{"userId":"u","sessionId":"s","favorite":true}"""),
            ("SessionSetFavoriteResponse", new SessionSetFavoriteResponse { SessionId = "s", Favorite = true }, """{"sessionId":"s","favorite":true}"""),
            ("ToolCard",
                new ToolCard
                {
                    ProtocolVersion = "1.5", Kind = "tool", Name = "n", DisplayName = "dn", Description = "d", Version = "v", RepositoryUrl = "r",
                    Tags = ["t"], InputSchema = WireJson.Parse("""{"type":"object"}"""), TimeoutSeconds = 30, Access = access, Metadata = meta,
                },
                """{"protocolVersion":"1.5","kind":"tool","name":"n","displayName":"dn","description":"d","version":"v","repositoryUrl":"r","tags":["t"],"inputSchema":{"type":"object"},"timeoutSeconds":30,"access":{"appId":"app","functionId":"fn"},"metadata":{"k":"v"}}"""),
            ("ToolRunRequest",
                new ToolRunRequest { ToolUseId = "tu", Input = WireJson.Parse("""{"a":1}"""), UserId = "u", SessionId = "s", Agent = "a", Metadata = meta },
                """{"toolUseId":"tu","input":{"a":1},"userId":"u","sessionId":"s","agent":"a","metadata":{"k":"v"}}"""),
            ("ToolRunResponse",
                new ToolRunResponse { ToolUseId = "tu", Status = "success", Content = [new ToolResultContent { Text = "x" }], Error = "e", LatencyMs = 12 },
                """{"toolUseId":"tu","status":"success","content":[{"text":"x"}],"error":"e","latencyMs":12}"""),
        ]);
    }

    /// <summary>
    /// The other direction: what a Go peer sends for zero values parses, and a Go nil slice
    /// (<c>null</c>) reads as an empty list — never as a null the caller would trip over.
    /// </summary>
    [Fact]
    public void GoNilSlicesParseAsEmptyLists()
    {
        Assert.Empty(Parse<Message>("""{"role":"","content":null}""").Content);
        Assert.Empty(Parse<ChatRequest>("""{"message":{"role":"user","content":null},"streamSubject":""}""").Message.Content);
        Assert.Empty(Parse<StoredMessage>("""{"role":"","content":null}""").Content);
        Assert.Empty(Parse<ToolResultBlock>("""{"toolUseId":"","content":null}""").Content);
        Assert.Empty(Parse<SessionsListResponse>("""{"sessions":null}""").Sessions);
        Assert.Empty(Parse<SessionGetResponse>("""{"sessionId":"","messages":null}""").Messages);
        Assert.Empty(Parse<InvokeResponse>("""{"runId":"","sessionId":"","message":{"role":"","content":null},"stopReason":""}""").Message.Content);
    }
}
