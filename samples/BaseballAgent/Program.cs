// A real agent: a model behind it, a tool it can use, and a card that tells other agents
// when to send questions its way. It answers baseball questions.
//
//   1. the card      what the agent is for; an orchestrator such as rsAssistant reads the
//                    description and skills to decide which questions to route here
//   2. the model     every reply comes from the org's inference gateway (InferenceGateway.cs)
//   3. the tool      web_search, found on the mesh, for anything newer than the model knows
//   4. the loop      ask the model → run the tools it asks for → ask again, until it answers
//
// Run it:        NATS_URL=nats://localhost:4222 dotnet run --project samples/BaseballAgent
// Talk to it:    nats-agents chat baseballExpert "Who holds the single-season home run record?"
//
// Environment:   NATS_URL / NATS_JWT / NATS_KEY    the NATS connection
//                AI_MODEL                          lab/family or a model id (default anthropic/sonnet)
//                APP_ID / APP_FUNCTION_ID          the identity registration shown on the card
//                IDT_VALIDATION=true               check each caller's token against that registration
using BaseballAgent;
using Microsoft.Extensions.Logging;
using TransactRx.NatsAgent;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

const string AgentName = "baseballExpert";
const int MaxRounds = 8; // model replies per turn, counting those that only ask for a tool

const string SystemPrompt = """
    You are Baseball Expert, a specialist assistant on the game of baseball: its rules and
    umpiring, history and records, players and teams, statistics and sabermetrics, strategy,
    and what is happening in the game today. You cover MLB first, and also the minor
    leagues, college, and international baseball (NPB, KBO, the World Baseball Classic).

    How to answer:
    - Lead with the answer, then give the detail that supports it. Be exact with numbers,
      names and years. If you are not sure of a figure, say so instead of guessing.
    - What you know stops at some point in the past. For anything recent or still changing
      (this season's scores, standings and statistics, trades, injuries, awards, active
      players' career totals), use the web_search tool and tell the user the date of what
      you found. If you have no search tool, say that your information may be out of date.
    - Before each search, write one short sentence saying what you are looking up.
    - Explain rules and statistics in plain words first; add the rulebook citation or the
      formula when it helps.
    - If a question is not about baseball, say in one sentence that you only cover baseball.
    """;

var model = Environment.GetEnvironmentVariable("AI_MODEL") is { Length: > 0 } configured ? configured : "anthropic/sonnet";

// ── 1. The card ───────────────────────────────────────────────────────────
// Description and skills are the routing instructions: an orchestrating agent shows them to
// its own model, which picks this agent when a question matches. Say plainly what belongs
// here, and give examples.
await using var agent = await Agent.CreateAsync(new AgentConfig
{
    Name = AgentName,
    DisplayName = "Baseball Expert",
    Description =
        "Baseball expert. Send every question about baseball here: rules and umpiring, MLB history and records, " +
        "players and teams, statistics and sabermetrics (batting average, OPS, ERA, WAR), strategy, and current " +
        "scores, standings, trades and news, which it looks up on the web. Covers MLB, the minor leagues, college " +
        "and international baseball. Does not answer questions on other sports or other subjects.",
    Version = "1.0.0",
    RepositoryUrl = "https://github.com/transactrx/nats-agent-dotnet",
    Category = "Examples",
    Tags = ["baseball", "sports"],
    Skills =
    [
        new Skill
        {
            Name = "baseball-rules",
            Description = "Explains the rules of baseball and how umpires apply them.",
            Examples = ["What is the infield fly rule?", "When is a runner out for leaving the baseline?"],
        },
        new Skill
        {
            Name = "baseball-history-and-records",
            Description = "Answers questions about baseball history, records, players, teams and awards.",
            Examples = ["Who has the most career home runs?", "Which teams have never won a World Series?"],
        },
        new Skill
        {
            Name = "baseball-statistics",
            Description = "Explains and compares baseball statistics, traditional and advanced.",
            Examples = ["What does WAR measure?", "Is OPS a better measure of a hitter than batting average?"],
        },
        new Skill
        {
            Name = "baseball-current-season",
            Description = "Looks up current scores, standings, statistics, trades and news on the web.",
            Examples = ["Who won the World Series this year?", "Who leads the American League in home runs?"],
        },
    ],
    UserContext = true, // the prompt names the user and their local date
    Nats = new NatsSettings { Url = Environment.GetEnvironmentVariable("NATS_URL") ?? "nats://localhost:4222" },
});

var sessions = new MemorySessionStore();
agent.UseSessionStore(sessions);

// ── 2 and 3. The model and the tool ───────────────────────────────────────
var gateway = new InferenceGateway(agent.Connection, model, maxTokens: 4096);

// Only web_search is admitted, whatever else is on the mesh. The registry keeps looking, so
// the tool can be deployed before or after this agent. Without it the agent answers from
// what the model knows.
await using var tools = new ToolRegistry(
    new ToolClient(agent.Connection),
    new ToolRegistryOptions { Allow = ["web_search"] });
await tools.StartAsync();

// ── 4. The loop: one call per turn ────────────────────────────────────────
agent.OnChat(async (turn, stream, cancellationToken) =>
{
    var question = string.Join("\n", turn.Message.Content.Select(block => block.Text).Where(text => !string.IsNullOrWhiteSpace(text)));
    if (question.Length == 0)
    {
        await stream.ErrorAsync("the message has no text", Codes.MissingField);
        return;
    }

    var userId = turn.UserId ?? "anonymous";
    List<Message> conversation = [.. sessions.History(userId, turn.SessionId), Message.UserText(question)];
    var system = $"{SystemPrompt}\n## The user\n{turn.ContextLine()}\nUse their local date for words like \"today\" and \"this season\".\n";
    List<ToolSpec> toolSpecs = [.. tools.Tools.Select(card => new ToolSpec(card.Name, card.Description, card.InputSchema ?? WireJson.EmptyObject))];
    turn.Logger.LogInformation("run {RunId}: model={Model} tools={Tools} history={History}", turn.RunId, model, toolSpecs.Count, conversation.Count - 1);

    var usage = new Usage();
    var wroteText = false;
    var roundStart = false;
    async ValueTask ForwardText(string text)
    {
        // Text from separate model replies (a note before a search, then the answer) would
        // otherwise run together.
        if (roundStart && wroteText)
        {
            await stream.TextAsync("\n\n");
        }

        roundStart = false;
        wroteText = true;
        await stream.TextAsync(text);
    }

    for (var round = 0; round < MaxRounds; round++)
    {
        roundStart = true;
        ModelReply reply;
        try
        {
            reply = await AskModelAsync(system, conversation, toolSpecs, ForwardText, () => roundStart, turn.Logger, cancellationToken);
        }
        catch (InferenceException ex)
        {
            await stream.ErrorAsync(ex.Message, Codes.Upstream);
            return;
        }

        usage = Add(usage, reply.Usage);
        conversation.Add(new Message { Role = "assistant", Content = reply.Content });

        if (reply.StopReason != "tool_use")
        {
            // Only the last reply is the answer; text before a tool call was a progress note.
            sessions.Append(userId, turn.SessionId, question, string.Concat(reply.Content.Select(block => block.Text)));
            await stream.DoneAsync(reply.StopReason == "max_tokens" ? StopReasons.MaxTokens : StopReasons.EndTurn, usage);
            return;
        }

        // The model asked for tools: run each, show it on the stream, and hand the results
        // back as the next user message.
        List<ContentBlock> results = [];
        foreach (var call in reply.Content.Select(block => block.ToolUse).OfType<ToolUseBlock>())
        {
            await stream.ToolUseAsync(call.ToolUseId, call.Name, call.Input);
            var (output, error) = await RunToolAsync(call, turn, cancellationToken);
            await stream.ToolResultAsync(call.ToolUseId, call.Name, output, error);
            results.Add(new ContentBlock
            {
                ToolResult = new ToolResultBlock
                {
                    ToolUseId = call.ToolUseId,
                    Status = error is null ? ToolStatuses.Success : ToolStatuses.Error,
                    Content = [new ToolResultContent { Text = error ?? output }],
                },
            });
        }

        conversation.Add(new Message { Role = "user", Content = results });
    }

    await stream.DoneAsync(StopReasons.MaxIterations, usage);
});

await agent.StartAsync();
Console.WriteLine($"{AgentName} is serving with model {model}. Press Ctrl+C to stop.");

// ── Graceful shutdown: finish in-flight turns, then leave ─────────────────
var stop = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.TrySetResult();
};
await stop.Task;

using var drainLimit = new CancellationTokenSource(TimeSpan.FromSeconds(30));
if (await agent.DrainAsync(drainLimit.Token) > 0)
{
    agent.InterruptAll(); // turns still running end with reason "interrupted"
}

return;

// One model reply. A failure the gateway may recover from is retried once, but only while
// this reply has shown the user nothing: a second attempt would repeat text already sent.
async Task<ModelReply> AskModelAsync(
    string system,
    IReadOnlyList<Message> conversation,
    IReadOnlyList<ToolSpec> toolSpecs,
    Func<string, ValueTask> onText,
    Func<bool> nothingShownYet,
    ILogger logger,
    CancellationToken cancellationToken)
{
    try
    {
        return await gateway.StreamAsync(system, conversation, toolSpecs, onText, cancellationToken);
    }
    catch (InferenceException ex) when (ex.Retryable && nothingShownYet())
    {
        logger.LogWarning("model call failed, retrying once: {Error}", ex.Message);
        await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        return await gateway.StreamAsync(system, conversation, toolSpecs, onText, cancellationToken);
    }
}

// Runs one tool call. A failure is returned as text, not thrown: the model reads it and can
// try a different search or answer without one.
async Task<(string? Output, string? Error)> RunToolAsync(ToolUseBlock call, Turn turn, CancellationToken cancellationToken)
{
    try
    {
        var response = await tools.RunAsync(
            call.Name,
            new ToolRunRequest { ToolUseId = call.ToolUseId, Input = call.Input, UserId = turn.UserId, SessionId = turn.SessionId, Agent = AgentName },
            cancellationToken);
        var output = string.Join("\n", (response.Content ?? []).Select(part => part.Text ?? part.Json?.GetRawText()));
        return response.Status == ToolStatuses.Error ? (null, response.Error ?? output) : (output, null);
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
        turn.Logger.LogWarning("tool {Tool} failed: {Error}", call.Name, ex.Message);
        return (null, ex.Message);
    }
}

static Usage Add(Usage total, Usage? more) => more is null
    ? total
    : new Usage
    {
        InputTokens = total.InputTokens + more.InputTokens,
        OutputTokens = total.OutputTokens + more.OutputTokens,
        TotalTokens = total.TotalTokens + more.TotalTokens,
    };
