// A complete, runnable agent in one file. It has no model behind it — the "thinking" is a
// word count — so it runs anywhere a NATS server does, and shows every part you wire up in a
// real agent:
//
//   1. a network tool, hosted on the mesh for any agent to use      (WordCountTool)
//   2. a session store, so conversations can be listed and reopened (MemorySessionStore)
//   3. the agent itself: a card plus a chat handler that streams events
//
// Run it:        NATS_URL=nats://localhost:4222 dotnet run --project samples/SimpleAgent
// Talk to it:    nats-agents -s nats://localhost:4222 chat simpleAgent "hello there"
//                nats-agents -s nats://localhost:4222 run word_count '{"text":"one two three"}'
using System.Text.Json;
using SimpleAgent;
using TransactRx.NatsAgent;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Tools;
using TransactRx.NatsAgent.Wire;

var nats = new NatsSettings { Url = Environment.GetEnvironmentVariable("NATS_URL") ?? "nats://localhost:4222" };

// ── 1. Host a network tool ────────────────────────────────────────────────
// One process can host any number of tools; each gets its own trx.tool.<name>.> subjects.
await using var toolHost = new ToolHost(nats);
toolHost.Register(new WordCountTool(), new ToolInfo { Version = "1.0.0", Tags = ["text"] });
await toolHost.StartAsync();

// ── 2. Create the agent ───────────────────────────────────────────────────
var sessions = new MemorySessionStore();
await using var agent = await Agent.CreateAsync(new AgentConfig
{
    Name = "simpleAgent",
    DisplayName = "Simple Agent",
    Description = "Example agent: counts the words you send it, using a network tool.",
    Version = "1.0.0",
    Category = "Examples",
    Skills = [new Skill { Name = "counting", Description = "Counts the words in a message." }],
    UserContext = true, // we read the caller's name and time zone (turn.ContextLine())
    Nats = nats,
});
agent.UseSessionStore(sessions);

// The agent discovers tools on the mesh instead of linking them in: tools deployed later
// show up here without a redeploy. It shares the agent's connection.
await using var tools = new ToolRegistry(new ToolClient(agent.Connection));
await tools.StartAsync();

// ── 3. The chat handler: one call per turn ────────────────────────────────
agent.OnChat(async (turn, stream, cancellationToken) =>
{
    var text = string.Concat(turn.Message.Content.Select(block => block.Text));
    await stream.StatusAsync("thinking…");

    // In a real agent this is where the model loop goes: send the conversation and
    // tools.Tools (name/description/inputSchema) to the inference gateway, run the tools
    // the model asks for, and stream its text. turn.ContextLine() belongs in the system
    // prompt so the model knows who is asking and what "today" means for them.
    var input = new { text };
    await stream.ToolUseAsync("tu_1", "word_count", input);
    var result = await tools.RunAsync(
        "word_count",
        new ToolRunRequest { ToolUseId = "tu_1", Input = WireJson.ToElement(input), UserId = turn.UserId, SessionId = turn.SessionId, Agent = "simpleAgent" },
        cancellationToken);
    var count = result.Content?.FirstOrDefault()?.Text ?? "?";
    await stream.ToolResultAsync("tu_1", "word_count", count, result.Error);

    // Stream the reply in pieces, the way model output arrives.
    var reply = $"You sent {count} word(s). {turn.ContextLine()}";
    foreach (var word in reply.Split(' '))
    {
        await stream.TextAsync(word + " ");
    }

    sessions.Append(turn.UserId ?? "anonymous", turn.SessionId, text, reply);
    await stream.DoneAsync(StopReasons.EndTurn, new Usage { InputTokens = text.Length, OutputTokens = reply.Length, TotalTokens = text.Length + reply.Length });
});

await agent.StartAsync();
Console.WriteLine("simpleAgent is serving. Press Ctrl+C to stop.");

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

await toolHost.DrainAsync(drainLimit.Token);

namespace SimpleAgent
{
    /// <summary>A network tool: the description and schema are what the model sees.</summary>
    internal sealed class WordCountTool : ITool
    {
        public string Name => "word_count";

        public string Description => "Count the words in a piece of text. Returns the count as a number.";

        public JsonElement InputSchema { get; } = WireJson.Parse(
            """{"type":"object","properties":{"text":{"type":"string","description":"The text to count."}},"required":["text"]}""");

        public Task<string> RunAsync(JsonElement input, CancellationToken cancellationToken)
        {
            // Throwing here is fine: the caller gets status "error" with this message, which
            // the model can read and react to.
            if (!input.TryGetProperty("text", out var text) || text.ValueKind != JsonValueKind.String)
            {
                throw new ArgumentException("input.text (string) is required");
            }

            var words = text.GetString()!.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length;
            return Task.FromResult(words.ToString());
        }
    }

    /// <summary>
    /// Sessions kept in memory, for the example only. A real agent stores them durably
    /// (DynamoDB, Postgres…) behind this same interface.
    /// </summary>
    internal sealed class MemorySessionStore : ISessionStore
    {
        private readonly object _gate = new();
        private readonly Dictionary<(string User, string Session), SessionGetResponse> _sessions = [];

        public void Append(string userId, string sessionId, string userText, string assistantText)
        {
            var now = DateTimeOffset.UtcNow.ToString("O");
            lock (_gate)
            {
                var session = _sessions.GetValueOrDefault((userId, sessionId))
                    ?? new SessionGetResponse { SessionId = sessionId, Title = userText.Length > 40 ? userText[..40] + "…" : userText };
                _sessions[(userId, sessionId)] = session with
                {
                    Messages =
                    [
                        .. session.Messages,
                        new StoredMessage { Role = "user", Content = [new ContentBlock { Text = userText }], Timestamp = now },
                        new StoredMessage { Role = "assistant", Content = [new ContentBlock { Text = assistantText }], Timestamp = now },
                    ],
                };
            }
        }

        public Task<IReadOnlyList<SessionMeta>> ListAsync(string userId, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                IReadOnlyList<SessionMeta> list = [.. _sessions
                    .Where(entry => entry.Key.User == userId)
                    .Select(entry => new SessionMeta
                    {
                        SessionId = entry.Value.SessionId,
                        Title = entry.Value.Title,
                        Favorite = entry.Value.Favorite,
                        LastMessageAt = entry.Value.Messages.LastOrDefault()?.Timestamp,
                    })
                    .OrderByDescending(meta => meta.Favorite)
                    .ThenByDescending(meta => meta.LastMessageAt)];
                return Task.FromResult(list);
            }
        }

        public Task<SessionGetResponse> GetAsync(string userId, string sessionId, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                return Task.FromResult(Find(userId, sessionId));
            }
        }

        public Task DeleteAsync(string userId, string sessionId, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                Find(userId, sessionId);
                _sessions.Remove((userId, sessionId));
                return Task.CompletedTask;
            }
        }

        public Task RenameAsync(string userId, string sessionId, string title, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _sessions[(userId, sessionId)] = Find(userId, sessionId) with { Title = title };
                return Task.CompletedTask;
            }
        }

        public Task SetFavoriteAsync(string userId, string sessionId, bool favorite, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _sessions[(userId, sessionId)] = Find(userId, sessionId) with { Favorite = favorite };
                return Task.CompletedTask;
            }
        }

        // The runtime turns this exception into the protocol's "unknown session" error (404 / 4041).
        private SessionGetResponse Find(string userId, string sessionId) =>
            _sessions.GetValueOrDefault((userId, sessionId)) ?? throw new SessionNotFoundException();
    }
}
