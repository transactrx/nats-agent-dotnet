# nats-agent for .NET

.NET implementation of the **NATS Agent Protocol**: AI agents and network agentic tools that
live on NATS, discoverable the same way transactrx microservices are, speaking a common
streaming chat protocol.

This is a port of the Go reference implementation
[`transactrx/nats-agent`](https://github.com/transactrx/nats-agent) and is **wire-compatible**
with it: a .NET agent is indistinguishable from a Go agent to callers, to the `nats-agents`
CLI and to `nats-discover`, and the .NET client talks to Go agents and tools. The protocol
itself is specified in the Go repository's
[SPEC.md](https://github.com/transactrx/nats-agent/blob/Development/SPEC.md) (protocol 1.5);
read
[IDENTITY-AND-AUTHORITY.md §1](https://github.com/transactrx/nats-agent/blob/Development/IDENTITY-AND-AUTHORITY.md)
before building an agent — permission enforcement belongs in your deterministic code, never in
the model, and the `userId` field of a request is not authenticated unless the turn's identity
is verified.

| Namespace | Go package | Role |
|---|---|---|
| `TransactRx.NatsAgent.Wire` | `pkg/wire` | Wire types of the protocol (cards, events, requests) |
| `TransactRx.NatsAgent` | `pkg/agent` | Agent runtime: card/ping/chat/cancel plumbing, heartbeats, sessions, extensions |
| `TransactRx.NatsAgent.Client` | `pkg/agentclient`, `pkg/toolclient` | Discover agents and tools, run chat turns, run tools, live tool registry |
| `TransactRx.NatsAgent.Tools` | `pkg/tool` | Network-tool host: N tools from one process, each with its own subject space |
| `TransactRx.NatsAgent.Identity` | `pkg/idt` | Internal Delegation Token validation shared by agents and tool hosts |
| `TransactRx.NatsAgent.Rotation` | `pkg/rotation` | Keeps a long turn supplied with short-lived tokens |
| `TransactRx.NatsAgent.Progress` | `pkg/progress` | Watches a run for progress idleness |
| `TransactRx.NatsAgent.Service` | `transactrx/nats-service` | The service conventions underneath: error envelope, compression, chunking, discovery |

Requires .NET 10. The only third-party dependency is the official NATS client
(`NATS.Client.Core`).

## Install

```bash
dotnet add package TransactRx.NatsAgent
```

The package is published to the org's GitHub Packages feed; see
[Distribution](#distribution) for the one-time feed setup.

## A complete example

[`samples/SimpleAgent`](samples/SimpleAgent/Program.cs) is a runnable agent in one file: it
hosts a network tool, keeps sessions, and streams a reply. Run it against any NATS server and
talk to it with the CLI:

```bash
nats-server &                       # or point NATS_URL at an existing server
NATS_URL=nats://localhost:4222 dotnet run --project samples/SimpleAgent

nats-agents -s nats://localhost:4222 list
nats-agents -s nats://localhost:4222 chat simpleAgent "hello there"
nats-agents -s nats://localhost:4222 run word_count '{"text":"one two three"}'
```

## Serving an agent

```csharp
using TransactRx.NatsAgent;
using TransactRx.NatsAgent.Wire;

await using var agent = await Agent.CreateAsync(new AgentConfig
{
    Name = "copayAssistant",
    Description = "Data analyst for the copay programs platform.",
    Version = "1.0.0",
    Category = "Pharmacy",                 // optional: display-only grouping label for UIs
    Skills = [new Skill { Name = "sql", Description = "Answers questions from the copay database." }],
    Access = new AgentAccess { AppId = "copayassistanceAppId", FunctionId = "copayassistanceFnId" },
});                                        // throws at once on bad config or no NATS (fail fast)

agent.UseSessionStore(myStore);            // optional: advertises capability "sessions"

agent.OnChat(async (turn, stream, cancellationToken) =>
{
    var system = basePrompt + "\n\n" + turn.ContextLine();   // who is asking + their local time
    await stream.StatusAsync("thinking…");
    // ... call trx.inferenceGateway, run tools ...
    await stream.ToolUseAsync("tu_1", "query_copay_db", input);
    await stream.ToolResultAsync("tu_1", "query_copay_db", result);
    await stream.TextAsync("There were 42 failed claims.");
    await stream.DoneAsync(StopReasons.EndTurn, usage);
});                                        // the runtime emits done/error/cancelled if you didn't

await agent.StartAsync();
```

The runtime provides: the `trx.agent.copayAssistant.>` subject space with queue group
`agent.copayAssistant`, the agent card, discovery replies on `trx.agent.discover`, run
tracking + broadcast `cancel`, stream heartbeats (≤15s), and terminal-event guarantees (an
exception thrown by the handler becomes an `error` event).

NATS connection settings come from the org environment conventions — `NATS_URL`, `NATS_JWT`,
`NATS_KEY` — or from `AgentConfig.Nats`, or pass an existing `NatsConnection` in
`AgentConfig.Connection`.

`Access` registers the agent with the identity model — leave it null to fall back to
`APP_ID`/`APP_FUNCTION_ID` from the environment. `AgentConfig.IdtValidation` turns on inbound
`X-TRX-IDT` checks on `chat`/`invoke`/`sessions*` (null → read from env); when enabled, a
request without a valid token gets the error envelope (403 / 4031) instead of an ack, and
never reaches the handler. The env contract is the Go library's:

| Env var | Meaning |
|---|---|
| `APP_ID` / `APP_FUNCTION_ID` | Fallback for `AgentConfig.Access` |
| `IDT_VALIDATION` | Enable inbound token checks |
| `IDT_OBSERVE_ONLY` | Validate + log, never block |
| `IDT_FAIL_OPEN` | Allow when identity is unreachable (never for a missing token) |
| `NATS_IDENTITY_BASE_PATH` | Identity service subject prefix (default `trx.identityservice`) |
| `NATS_IDENTITY_VALIDATE_SUBJECT` | Validate subject suffix (default `validateInternalToken`) |
| `IDT_VALIDATE_TIMEOUT_SECONDS` | Per-call timeout (default 5s) |
| `IDT_VALIDATE_CACHE_SECONDS` | Cache TTL (default 300s; denies and observe-only results are never cached) |

### Long turns (protocol 1.5)

- A request `budget` is always honoured: the handler's token is cancelled at
  `remainingMs` − 5s and `turn.CutReason` is `time_cap`. A cancel carrying a cut reason, and
  `agent.InterruptAll()`, end the turn the same way. A handler that returns without a terminal
  event then ends with an `error` carrying that `reason`.
- `AgentConfig.Progress = true` — call `stream.ProgressAsync(stage, detail)` on real work
  (model output, tool start/end); at most one frame per 15s is sent. Callers may then cut a
  turn that stops progressing.
- `AgentConfig.Renew = true` — the run accepts fresh tokens mid-turn. Use `turn.CurrentIdt`
  (not `turn.Identity.Idt`) for each new onward call, or `turn.OnRenew(...)` to be told.
- Callers: `client.ChatAsync(..., new ChatOptions { IdleWindow = ... })`, `run.Caps`,
  `run.RenewAsync`, `run.CancelWithReasonAsync` (call it before cancelling the chat's
  token); `TokenRotator` keeps a long turn supplied with short-lived tokens.

### User context (protocol 1.3)

Requests may carry `context` `{timeZone, userName, locale}` — display only, never
authorization. The runtime cleans it before the handler. Put `turn.ContextLine()` in the
system prompt so "today" means the user's day; `turn.Now` / `turn.TimeZone` give the local
clock (UTC when unknown). Calls to other agents made from inside the handler forward the
context automatically. Set `AgentConfig.UserContext = true` to advertise it.

## Talking to an agent

```csharp
using TransactRx.NatsAgent;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Wire;

await using var client = await AgentClient.ConnectAsync();         // NATS_URL / NATS_JWT / NATS_KEY
var cards = await client.DiscoverAsync();                          // every agent on the mesh

using (CallContext.WithIdt(callerIdt))                             // sent as X-TRX-IDT on chat/invoke/sessions*
using (CallContext.WithUserContext(new UserContext { TimeZone = browserTimeZone, UserName = "Jane Doe" }))
{
    var run = await client.ChatAsync("copayAssistant", new ChatRequest
    {
        UserId = "manuel",
        Message = Message.UserText("How many claims failed yesterday?"),
    });

    await foreach (var ev in run.Events.ReadAllAsync())            // completes after the terminal event
    {
        switch (ev.Type)
        {
            case EventTypes.Text: Console.Write(ev.TextDelta); break;
            case EventTypes.Done: Console.WriteLine($"\n[{ev.StopReason}]"); break;
            case EventTypes.Error: Console.WriteLine("failed: " + ev.Error); break;
        }
    }
}
```

The client generates the stream subject, filters transport pings, watches for dead runs (45s
silence), and turns cancellation of the token passed to `ChatAsync` into a best-effort
server-side cancel. A protocol-level refusal (the SPEC §9 envelope) is a
`NatsServiceException` with `Status`, `ApiStatusCode` and `ErrorMessage`.

`CallContext` is the .NET counterpart of the values the Go library carries on a
`context.Context`: the token and user context set in a `using` scope follow every call made
inside it.

## Hosting network tools

One deployable process, N tools, each a first-class citizen on the wire:

```csharp
using TransactRx.NatsAgent.Tools;

await using var host = ToolHost.FromEnvironment();
host.Register(new QuickChartTool(), new ToolInfo { Version = "1.0.0", Tags = ["visualization"] });
host.Register(new WebSearchTool(), new ToolInfo { Version = "1.0.0", TimeoutSeconds = 25 });
await host.StartAsync();
```

A tool implements `ITool` (`Name`, `Description`, `InputSchema`, `RunAsync`). Throwing from
`RunAsync` is a model-visible failure (status `"error"`), not a protocol error. Implement
`ICallTool` instead to receive the verified caller, `sessionId` and `toolUseId`; declare the
tool's identity registration in `ToolInfo.Access` and bound concurrency with
`ToolInfo.MaxConcurrent` (429 / 4291 beyond it).

## Using network tools from an agent

```csharp
var tools = new ToolRegistry(new ToolClient(agent.Connection));
await tools.StartAsync();                 // initial sweep + live announce pickup + 5m re-sweeps

foreach (var card in tools.Tools)
{
    // card.Name / Description / InputSchema go verbatim into the gateway's tools array
}

var response = await tools.RunAsync("create_chart", new ToolRunRequest { Input = input, UserId = userId, Agent = "copayAssistant" });
```

## CLI: nats-agents

The operator CLI is ported too (`src/TransactRx.NatsAgent.Cli`), as a .NET tool with the same
commands, flags and output as the Go binary:

```bash
dotnet tool install -g TransactRx.NatsAgent.Cli
nats-agents list                  # discover agents (cards table; --json for raw)
nats-agents tools                 # discover network tools
nats-agents chat copayAssistant "How many claims failed yesterday?"
nats-agents chat copayAssistant   # no text: interactive multi-turn chat
nats-agents run create_chart '{"chart":{...}}'
```

nats CLI contexts are first-class: with no options it uses the context selected with
`nats context select`. Either CLI works against agents in either language; if you already
have the Go `nats-agents` from Homebrew you do not need this one.

## Differences from the Go library

The wire behaviour is the same. The API follows .NET conventions:

| Go | .NET |
|---|---|
| `context.Context` cancellation | `CancellationToken` |
| Values on the context (`WithIDT`, `WithUserContext`) | Ambient `CallContext.WithIdt(...)` / `WithUserContext(...)` scopes |
| Returned `error`, `*ServiceError` | Exceptions; `NatsServiceException` for the protocol envelope |
| `run.Events` channel | `ChannelReader<AgentEvent>` (`ReadAllAsync()`) |
| `agent.CutReason(ctx)` | `turn.CutReason` |
| `log.Printf` | `ILogger` (`AgentConfig.LoggerFactory`); by default the same one-line-per-entry output on stdout, so `IDT_METRIC` lines are never lost |
| One NATS connection per agent / per tool | One connection shared by an agent and all of a host's tools |

Operational notes:

- A connection the library opens retries for 15 minutes when NATS is lost and then ends the
  process (exit code 1) so the orchestrator restarts it — what the Go services do. Set
  `NatsSettings.ExitWhenConnectionLost = false`, or supply your own `NatsConnection`, to
  handle it yourself.
- Time zones come from the operating system's IANA database (`TimeZoneInfo`). Minimal
  container images need the `tzdata` package installed; without it a user's time zone is
  treated as unknown and the agent answers in UTC.
- Not ported: `experiments/durable-session` (an experiment, not part of the library).

## Building and testing

```bash
dotnet build
dotnet test          # needs `nats-server` on PATH (or NATS_SERVER_PATH); it is started automatically
```

The test suite is a port of the Go suite, test for test, and runs against a real NATS
server: discovery filters, chat streaming and event ordering, cancellation, sessions, tool
hosting, identity enforcement, long-turn timing, token rotation and the CLI.

## Distribution

.NET libraries are distributed as **NuGet packages** (`.nupkg` — the equivalent of a crate or
a Go module version), fetched by name and version from a **feed** (a registry).

- `dotnet pack` produces `TransactRx.NatsAgent.<version>.nupkg` (the library) and
  `TransactRx.NatsAgent.Cli.<version>.nupkg` (the CLI tool).
- Pushing to the **`Production`** branch releases, as in the Go repository: the workflow
  runs the tests, creates the next `v*` tag and GitHub release, and publishes both packages
  with that version to the org feed at `https://nuget.pkg.github.com/transactrx/index.json`.
  If the repository has a `NUGET_API_KEY` secret they are published to nuget.org as well.

To consume from GitHub Packages, a repository adds the feed once (a `nuget.config` next to
its solution) and authenticates with a GitHub token that can read packages:

```xml
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
    <add key="transactrx" value="https://nuget.pkg.github.com/transactrx/index.json" />
  </packageSources>
</configuration>
```

```bash
dotnet nuget update source transactrx --username <github-user> --password <token> --store-password-in-clear-text
dotnet add package TransactRx.NatsAgent
```
