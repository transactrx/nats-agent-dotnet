using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using TransactRx.NatsAgent.Client;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Cli;

/// <summary>
/// nats-agents is the operator CLI for the NATS Agent Protocol: discover the agents and
/// network tools on the mesh, inspect their cards, check liveness, run a streaming chat turn
/// against an agent, or execute a tool — the same way another agent would.
/// </summary>
public static partial class NatsAgentsCli
{
    private const string UsageText = """
        nats-agents — discover and drive NATS Agent Protocol agents and tools

        Usage:
          nats-agents [flags] <command> [args]

        Commands:
          list                  Discover agents on the mesh (agent cards)
          tools                 Discover network tools on the mesh (tool cards)
          card <agent>          Fetch one agent's full card
          tool <name>           Fetch one tool's full card
          ping <agent>          Agent liveness probe
          chat <agent> [text]   Chat with an agent; prints the reply live. With no
                                text, starts an interactive multi-turn conversation
                                (the agent keeps context between turns)
          run <tool> <json>     Execute a network tool with a JSON input object

        Flags:
          -s, --server URL      NATS server URL (default: $NATS_URL)
              --context NAME    NATS CLI context (from ~/.config/nats/context);
                                the selected default context is used when nothing
                                else specifies a server
              --creds FILE      NATS .creds file (JWT + seed)
              --nkey FILE       NATS NKey seed file
              --jwt STRING      NATS user JWT (default: $NATS_JWT)
              --seed STRING     NATS user NKey seed (default: $NATS_KEY)
              --timeout DUR     Discovery window / request timeout (default 3s)
              --json            Raw JSON output instead of tables
              --session ID      Resume an existing chat session (chat); on tool run,
                                the sessionId sent with the call. Every chat turn
                                prints its session id, and interactive mode prints a
                                resume command on exit
              --user ID         User id for session scoping (chat; sent on tool run)
              --idt TOKEN       Internal Delegation Token sent as X-TRX-IDT on agent
                                chat/invoke/sessions calls and tool runs
                                (default: $TRX_IDT)
              --version         Print version and exit

        Connection resolution — nats CLI contexts are first-class: with no options,
        the selected default context is used (just like the nats CLI itself).
        Explicit -s/--server or --context win; $NATS_URL applies only when no
        context is selected. --creds/--nkey/--jwt+--seed override the context's
        credentials.

        Examples:
          nats-agents list
          nats-agents tools --json
          nats-agents chat copayAssistant "How many claims failed yesterday?"
          nats-agents chat copayAssistant                       (interactive)
          nats-agents chat copayAssistant --session 6a2f... "and the day before?"
          nats-agents run create_chart '{"chart":{"type":"bar","data":{"labels":["A"],"datasets":[{"data":[1]}]}}}'

        """;

    private static readonly JsonSerializerOptions Indented = new(WireJson.Options) { WriteIndented = true };

    private static readonly HashSet<string> BoolFlags = ["json", "version"];

    private static readonly HashSet<string> ValueFlags =
        ["s", "server", "context", "creds", "nkey", "jwt", "seed", "timeout", "session", "user", "idt"];

    [GeneratedRegex(@"(\d+(?:\.\d+)?)(ms|us|µs|ns|s|m|h)")]
    private static partial Regex DurationPart();

    /// <summary>
    /// Runs the CLI and returns its exit code: 0 success, 1 failure, 2 usage error.
    /// </summary>
    /// <param name="args">Command line; flags are accepted before or after the command.</param>
    /// <param name="stdin">Interactive chat input.</param>
    /// <param name="stdout">Replies and JSON/table output.</param>
    /// <param name="stderr">Progress, status and errors.</param>
    /// <param name="env">Environment lookup (<c>NATS_URL</c>, <c>HOME</c>, <c>TRX_IDT</c>…).</param>
    public static async Task<int> RunAsync(string[] args, TextReader stdin, TextWriter stdout, TextWriter stderr, Func<string, string?> env)
    {
        try
        {
            return await Run(args, stdin, stdout, stderr, env);
        }
        catch (CliExit exit)
        {
            if (exit.Message.Length > 0)
            {
                await stderr.WriteLineAsync(exit.Message);
            }

            return exit.Code;
        }
        catch (Exception ex)
        {
            await stderr.WriteLineAsync(ex.Message);
            return 1;
        }
    }

    private static async Task<int> Run(string[] args, TextReader stdin, TextWriter stdout, TextWriter stderr, Func<string, string?> env)
    {
        // Flags are accepted before or after the command, and after the agent name of chat
        // (`chat myAgent --session ID "text"`).
        var (flags, positional) = Parse(args);
        if (flags.ContainsKey("version"))
        {
            var version = typeof(NatsAgentsCli).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "dev";
            await stdout.WriteLineAsync("nats-agents version " + version.Split('+')[0]);
            return 0;
        }

        if (positional.Count == 0)
        {
            await stderr.WriteAsync(UsageText);
            return 2;
        }

        var cfg = new Config
        {
            Server = flags.GetValueOrDefault("server") ?? flags.GetValueOrDefault("s") ?? "",
            Context = flags.GetValueOrDefault("context") ?? "",
            Creds = flags.GetValueOrDefault("creds") ?? "",
            NKey = flags.GetValueOrDefault("nkey") ?? "",
            Jwt = flags.GetValueOrDefault("jwt") ?? "",
            Seed = flags.GetValueOrDefault("seed") ?? "",
            Timeout = flags.TryGetValue("timeout", out var rawTimeout) ? ParseDuration(rawTimeout) : TimeSpan.FromSeconds(3),
            AsJson = flags.ContainsKey("json"),
            Session = flags.GetValueOrDefault("session") ?? "",
            User = flags.GetValueOrDefault("user") ?? "",
        };
        var idt = flags.GetValueOrDefault("idt") ?? env("TRX_IDT") ?? "";

        var command = positional[0];
        var rest = positional.Skip(1).ToList();
        if (command is not ("list" or "tools" or "card" or "tool" or "ping" or "chat" or "run"))
        {
            await stderr.WriteLineAsync($"unknown command \"{command}\"\n");
            await stderr.WriteAsync(UsageText);
            return 2;
        }

        NatsConnection nc;
        try
        {
            nc = await ConnectAsync(cfg, env);
        }
        catch (Exception ex)
        {
            throw new CliExit(1, "connecting to NATS: " + ex.Message);
        }

        await using (nc)
        using (CallContext.WithIdt(idt))
        {
            var ac = new AgentClient(nc, NullLoggerFactory.Instance);
            var tc = new ToolClient(nc, NullLoggerFactory.Instance);
            var io = new Io(stdin, stdout, stderr);
            switch (command)
            {
                case "list":
                    PrintAgentCards(await ac.DiscoverAsync(new DiscoverFilter(), cfg.Timeout), cfg.AsJson, stdout);
                    break;
                case "tools":
                    PrintToolCards(await tc.DiscoverAsync(new DiscoverFilter(), cfg.Timeout), cfg.AsJson, stdout);
                    break;
                case "card":
                    RequireArgs(rest, 1, "card <agent>");
                    PrintJson(stdout, await ac.CardAsync(rest[0]));
                    break;
                case "tool":
                    RequireArgs(rest, 1, "tool <name>");
                    PrintJson(stdout, await tc.CardAsync(rest[0]));
                    break;
                case "ping":
                    RequireArgs(rest, 1, "ping <agent>");
                    PrintJson(stdout, await ac.PingAsync(rest[0]));
                    break;
                case "chat":
                    RequireArgs(rest, 1, "chat <agent> [text]");
                    if (rest.Count == 1)
                    {
                        await ChatInteractive(ac, rest[0], cfg, io);
                    }
                    else
                    {
                        var (_, error) = await ChatTurn(ac, rest[0], string.Join(' ', rest.Skip(1)), cfg.Session, cfg, io);
                        if (error is not null)
                        {
                            throw new CliExit(1, "[error] " + error);
                        }
                    }

                    break;
                case "run":
                    RequireArgs(rest, 2, "run <tool> <json-input>");
                    return await RunTool(tc, rest[0], rest[1], cfg, stdout);
            }
        }

        return 0;
    }

    // ─── arguments ────────────────────────────────────────────────────────

    /// <summary>
    /// Splits the command line into flags and positional arguments. Like Go's flag package,
    /// a flag may be written <c>-name</c> or <c>--name</c>, with its value as the next
    /// argument or after <c>=</c>; <c>--</c> ends flag parsing.
    /// </summary>
    internal static (Dictionary<string, string> Flags, List<string> Positional) Parse(string[] args)
    {
        var flags = new Dictionary<string, string>();
        var positional = new List<string>();
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg == "--")
            {
                positional.AddRange(args.Skip(i + 1));
                break;
            }

            if (arg.Length < 2 || arg[0] != '-')
            {
                positional.Add(arg);
                continue;
            }

            var name = arg.TrimStart('-');
            string? value = null;
            var eq = name.IndexOf('=');
            if (eq >= 0)
            {
                (name, value) = (name[..eq], name[(eq + 1)..]);
            }

            if (name is "h" or "help")
            {
                throw new CliExit(0, UsageText.TrimEnd());
            }

            if (BoolFlags.Contains(name))
            {
                if (value is null or "true")
                {
                    flags[name] = "true";
                }

                continue;
            }

            if (!ValueFlags.Contains(name))
            {
                throw new CliExit(2, $"flag provided but not defined: -{name}\n{UsageText.TrimEnd()}");
            }

            if (value is null)
            {
                if (i + 1 >= args.Length)
                {
                    throw new CliExit(2, $"flag needs an argument: -{name}\n{UsageText.TrimEnd()}");
                }

                value = args[++i];
            }

            flags[name] = value;
        }

        return (flags, positional);
    }

    /// <summary>Parses a Go-style duration: "3s", "700ms", "1m30s", "1.5s".</summary>
    internal static TimeSpan ParseDuration(string text)
    {
        var matches = DurationPart().Matches(text);
        if (matches.Count == 0 || matches.Sum(m => m.Length) != text.Length)
        {
            throw new CliExit(2, $"invalid value \"{text}\" for flag -timeout: expected a duration such as 3s or 700ms");
        }

        var total = TimeSpan.Zero;
        foreach (Match m in matches)
        {
            var n = double.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
            total += m.Groups[2].Value switch
            {
                "h" => TimeSpan.FromHours(n),
                "m" => TimeSpan.FromMinutes(n),
                "s" => TimeSpan.FromSeconds(n),
                "ms" => TimeSpan.FromMilliseconds(n),
                "us" or "µs" => TimeSpan.FromMicroseconds(n),
                _ => TimeSpan.FromTicks((long)(n / 100)),
            };
        }

        return total;
    }

    private static void RequireArgs(List<string> args, int n, string usage)
    {
        if (args.Count < n)
        {
            throw new CliExit(1, "usage: nats-agents " + usage);
        }
    }

    // ─── connection ───────────────────────────────────────────────────────

    /// <summary>
    /// A NATS CLI context configuration (<c>~/.config/nats/context/&lt;name&gt;.json</c>) —
    /// same shape nats-discover reads.
    /// </summary>
    internal sealed record NatsContext
    {
        [JsonPropertyName("url")]
        public string? Url { get; init; }

        [JsonPropertyName("user")]
        public string? User { get; init; }

        [JsonPropertyName("password")]
        public string? Password { get; init; }

        [JsonPropertyName("token")]
        public string? Token { get; init; }

        [JsonPropertyName("creds")]
        public string? Creds { get; init; }

        [JsonPropertyName("nkey")]
        public string? NKey { get; init; }

        [JsonPropertyName("cert")]
        public string? Cert { get; init; }

        [JsonPropertyName("key")]
        public string? Key { get; init; }

        [JsonPropertyName("ca")]
        public string? Ca { get; init; }

        [JsonPropertyName("jwt")]
        public string? Jwt { get; init; }

        [JsonPropertyName("seed")]
        public string? Seed { get; init; }
    }

    /// <summary>
    /// Resolves the server and credentials with nats CLI contexts as a first-class citizen:
    /// explicit flags win (-s / --context), otherwise the nats CLI's selected default context
    /// is used — running with no options at all behaves exactly like the nats CLI itself.
    /// $NATS_URL is the fallback when no context is selected (CI, containers, the bastion).
    /// Explicit credential flags override the context's credentials, and $NATS_JWT/$NATS_KEY
    /// fill in when nothing else supplied auth.
    /// </summary>
    internal static NatsOpts ResolveOptions(Config cfg, Func<string, string?> env)
    {
        string url;
        var auth = NatsAuthOpts.Default;
        var tls = NatsTlsOpts.Default;
        var haveAuth = false;

        void UseContext(NatsContext ctx)
        {
            url = ctx.Url ?? "";
            if (!string.IsNullOrEmpty(ctx.User) && !string.IsNullOrEmpty(ctx.Password))
            {
                auth = auth with { Username = ctx.User, Password = ctx.Password };
                haveAuth = true;
            }

            if (!string.IsNullOrEmpty(ctx.Token))
            {
                auth = auth with { Token = ctx.Token };
                haveAuth = true;
            }

            if (!string.IsNullOrEmpty(ctx.Creds))
            {
                auth = auth with { CredsFile = ctx.Creds };
                haveAuth = true;
            }

            if (!string.IsNullOrEmpty(ctx.NKey))
            {
                auth = auth with { NKeyFile = ctx.NKey };
                haveAuth = true;
            }

            if (!string.IsNullOrEmpty(ctx.Jwt) && !string.IsNullOrEmpty(ctx.Seed))
            {
                auth = auth with { Jwt = ctx.Jwt, Seed = ctx.Seed };
                haveAuth = true;
            }

            if (!string.IsNullOrEmpty(ctx.Cert) && !string.IsNullOrEmpty(ctx.Key))
            {
                tls = tls with { CertFile = ctx.Cert, KeyFile = ctx.Key };
                haveAuth = true;
            }

            if (!string.IsNullOrEmpty(ctx.Ca))
            {
                tls = tls with { CaFile = ctx.Ca };
                haveAuth = true;
            }
        }

        url = "";
        if (cfg.Server.Length > 0)
        {
            url = cfg.Server;
        }
        else if (cfg.Context.Length > 0)
        {
            UseContext(LoadContext(cfg.Context, env)
                ?? throw new InvalidOperationException($"failed to load context \"{cfg.Context}\": context \"{cfg.Context}\" not found"));
        }
        else if (LoadDefaultContext(env) is { } selected)
        {
            UseContext(selected);
        }
        else if (env("NATS_URL") is { Length: > 0 } envUrl)
        {
            url = envUrl;
        }
        else
        {
            throw new InvalidOperationException(
                "no NATS server specified: use -s, --context, select a nats CLI context, or set $NATS_URL (no default context found)");
        }

        // Explicit credential flags override whatever the context provided.
        if (cfg.Creds.Length > 0)
        {
            auth = auth with { CredsFile = cfg.Creds };
            haveAuth = true;
        }

        if (cfg.NKey.Length > 0)
        {
            auth = auth with { NKeyFile = cfg.NKey };
            haveAuth = true;
        }

        var (jwt, seed) = (cfg.Jwt, cfg.Seed);
        if (jwt.Length == 0 && seed.Length == 0 && !haveAuth)
        {
            (jwt, seed) = (env("NATS_JWT") ?? "", env("NATS_KEY") ?? "");
        }

        if (jwt.Length > 0 && seed.Length > 0)
        {
            auth = auth with { Jwt = jwt, Seed = seed };
        }

        return NatsOpts.Default with { Url = url, Name = "nats-agents-cli", AuthOpts = auth, TlsOpts = tls };
    }

    private static async Task<NatsConnection> ConnectAsync(Config cfg, Func<string, string?> env)
    {
        var nc = new NatsConnection(ResolveOptions(cfg, env));
        try
        {
            await nc.ConnectAsync();
            return nc;
        }
        catch
        {
            await nc.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Possible NATS config directories in order of preference: <c>~/.config/nats</c> (where
    /// the nats CLI stores contexts), then the platform's user config directory as fallback.
    /// </summary>
    private static IEnumerable<string> ConfigDirectories(Func<string, string?> env)
    {
        var home = env("HOME") is { Length: > 0 } h ? h : env("USERPROFILE");
        if (!string.IsNullOrEmpty(home))
        {
            yield return Path.Combine(home, ".config", "nats");
            if (OperatingSystem.IsMacOS())
            {
                yield return Path.Combine(home, "Library", "Application Support", "nats");
            }
        }

        if (env("XDG_CONFIG_HOME") is { Length: > 0 } xdg)
        {
            yield return Path.Combine(xdg, "nats");
        }

        if (env("APPDATA") is { Length: > 0 } appData)
        {
            yield return Path.Combine(appData, "nats");
        }
    }

    private static NatsContext? LoadContext(string name, Func<string, string?> env)
    {
        foreach (var dir in ConfigDirectories(env))
        {
            var path = Path.Combine(dir, "context", name + ".json");
            if (!File.Exists(path))
            {
                continue;
            }

            try
            {
                return JsonSerializer.Deserialize<NatsContext>(File.ReadAllText(path));
            }
            catch (JsonException ex)
            {
                throw new InvalidOperationException($"failed to load context \"{name}\": invalid context file: {ex.Message}");
            }
        }

        return null;
    }

    private static NatsContext? LoadDefaultContext(Func<string, string?> env)
    {
        foreach (var dir in ConfigDirectories(env))
        {
            var path = Path.Combine(dir, "context.txt");
            if (!File.Exists(path))
            {
                continue;
            }

            var name = File.ReadAllText(path).Trim();
            if (name.Length > 0)
            {
                return LoadContext(name, env);
            }
        }

        return null;
    }

    // ─── commands ─────────────────────────────────────────────────────────

    /// <summary>
    /// A multi-turn REPL: every turn is sent with the session id from the previous ack (or
    /// --session to resume an old conversation), so the agent keeps the whole conversation's
    /// context. Exit with "exit", "quit", or end of input; a turn-level error is printed but
    /// keeps the session alive.
    /// </summary>
    private static async Task ChatInteractive(AgentClient ac, string agent, Config cfg, Io io)
    {
        var sessionId = cfg.Session;
        await io.Err.WriteLineAsync(sessionId.Length > 0
            ? $"chatting with {agent} (resuming session {sessionId}) — exit, quit, or Ctrl-D to leave"
            : $"chatting with {agent} — exit, quit, or Ctrl-D to leave");

        while (true)
        {
            await io.Err.WriteAsync($"\n{agent}> ");
            var line = await io.In.ReadLineAsync();
            if (line is null)
            {
                await io.Err.WriteLineAsync();
                break;
            }

            var text = line.Trim();
            if (text.Length == 0)
            {
                continue;
            }

            if (text is "exit" or "quit")
            {
                break;
            }

            var (sid, error) = await ChatTurn(ac, agent, text, sessionId, cfg, io);
            if (error is not null)
            {
                await io.Err.WriteLineAsync("[error] " + error);
            }

            if (sid.Length > 0)
            {
                sessionId = sid;
            }
        }

        if (sessionId.Length > 0)
        {
            await io.Err.WriteLineAsync($"resume this conversation with: nats-agents chat {agent} --session {sessionId}");
        }
    }

    /// <summary>
    /// Runs one streaming turn and returns the session id from the ack so callers can chain
    /// turns. Run-level errors are returned, not fatal.
    /// </summary>
    private static async Task<(string SessionId, string? Error)> ChatTurn(AgentClient ac, string agent, string text, string sessionId, Config cfg, Io io)
    {
        ChatRun run;
        try
        {
            run = await ac.ChatAsync(agent, new ChatRequest
            {
                SessionId = sessionId.Length > 0 ? sessionId : null,
                UserId = cfg.User.Length > 0 ? cfg.User : null,
                Message = Message.UserText(text),
            });
        }
        catch (Exception ex)
        {
            return (sessionId, ex.Message);
        }

        await io.Err.WriteLineAsync($"run {run.Ack.RunId} session {run.Ack.SessionId} (instance {run.Ack.InstanceId})");

        string? runError = null;
        await foreach (var ev in run.Events.ReadAllAsync())
        {
            if (cfg.AsJson)
            {
                PrintJson(io.Out, ev);
                continue;
            }

            switch (ev.Type)
            {
                case EventTypes.Text:
                    await io.Out.WriteAsync(ev.TextDelta);
                    break;
                case EventTypes.Status:
                    await io.Err.WriteLineAsync("[status] " + ev.StatusText);
                    break;
                case EventTypes.ToolUse:
                    var input = Compact(ev.ToolInput, 120);
                    await io.Err.WriteLineAsync($"[tool] {ev.ToolName} {(input is "" or "null" ? "{}" : input)}");
                    break;
                case EventTypes.ToolResult:
                    await io.Err.WriteLineAsync($"[tool] {ev.ToolName} → {(string.IsNullOrEmpty(ev.ToolError) ? "ok" : "error: " + ev.ToolError)}");
                    break;
                case EventTypes.Data:
                    await io.Err.WriteLineAsync($"[data:{ev.Kind}] {Compact(ev.Payload, 200)}");
                    break;
                case EventTypes.Done:
                    await io.Out.WriteLineAsync();
                    await io.Err.WriteLineAsync("[done] " + ev.StopReason);
                    break;
                case EventTypes.Error:
                    await io.Out.WriteLineAsync();
                    runError = ev.Error ?? "";
                    break;
            }
        }

        await io.Out.FlushAsync();
        return (run.Ack.SessionId, runError);
    }

    private static async Task<int> RunTool(ToolClient tc, string name, string rawInput, Config cfg, TextWriter stdout)
    {
        JsonElement input;
        try
        {
            input = WireJson.Parse(rawInput);
            if (input.ValueKind != JsonValueKind.Object)
            {
                throw new JsonException("not an object");
            }
        }
        catch (JsonException ex)
        {
            throw new CliExit(1, "input is not a JSON object: " + ex.Message);
        }

        // Size the request timeout from the tool's own advertisement when we can.
        var timeout = cfg.Timeout;
        try
        {
            var card = await tc.CardAsync(name);
            if (card.TimeoutSeconds > 0)
            {
                timeout = TimeSpan.FromSeconds(card.TimeoutSeconds) + TimeSpan.FromSeconds(10);
            }
        }
        catch (Exception)
        {
            // the run below reports what is wrong
        }

        var resp = await tc.RunAsync(name, new ToolRunRequest
        {
            Input = input,
            SessionId = cfg.Session.Length > 0 ? cfg.Session : null,
            UserId = cfg.User.Length > 0 ? cfg.User : null,
            Agent = "nats-agents-cli",
        }, timeout);
        PrintJson(stdout, resp);
        return resp.Status == ToolStatuses.Error ? 1 : 0;
    }

    // ─── output ───────────────────────────────────────────────────────────

    private static void PrintAgentCards(IReadOnlyList<AgentCard> cards, bool asJson, TextWriter stdout)
    {
        var sorted = cards.OrderBy(c => c.Name, StringComparer.Ordinal).ToList();
        if (asJson)
        {
            PrintJson(stdout, sorted);
            return;
        }

        if (sorted.Count == 0)
        {
            stdout.WriteLine("no agents responded");
            return;
        }

        var rows = new List<string[]> { new[] { "AGENT", "VERSION", "CAPABILITIES", "SKILLS", "DESCRIPTION" } };
        foreach (var c in sorted)
        {
            var caps = new List<string> { "chat" };
            if (c.Capabilities.Sync)
            {
                caps.Add("sync");
            }

            if (c.Capabilities.Sessions)
            {
                caps.Add("sessions");
            }

            rows.Add([c.Name, c.Version ?? "", string.Join(',', caps), string.Join(',', (c.Skills ?? []).Select(s => s.Name)), Truncate(c.Description, 60)]);
        }

        PrintTable(stdout, rows);
    }

    private static void PrintToolCards(IReadOnlyList<ToolCard> cards, bool asJson, TextWriter stdout)
    {
        var sorted = cards.OrderBy(c => c.Name, StringComparer.Ordinal).ToList();
        if (asJson)
        {
            PrintJson(stdout, sorted);
            return;
        }

        if (sorted.Count == 0)
        {
            stdout.WriteLine("no tools responded");
            return;
        }

        var rows = new List<string[]> { new[] { "TOOL", "VERSION", "TIMEOUT", "TAGS", "DESCRIPTION" } };
        rows.AddRange(sorted.Select(c => new[] { c.Name, c.Version ?? "", c.TimeoutSeconds + "s", string.Join(',', c.Tags ?? []), Truncate(c.Description, 60) }));
        PrintTable(stdout, rows);
    }

    // Columns padded to their widest cell plus two spaces, like Go's tabwriter.
    private static void PrintTable(TextWriter stdout, List<string[]> rows)
    {
        var widths = new int[rows[0].Length];
        foreach (var row in rows)
        {
            for (var i = 0; i < row.Length; i++)
            {
                widths[i] = Math.Max(widths[i], row[i].Length);
            }
        }

        foreach (var row in rows)
        {
            stdout.WriteLine(string.Concat(row.Select((cell, i) => i == row.Length - 1 ? cell : cell.PadRight(widths[i] + 2))).TrimEnd());
        }
    }

    private static void PrintJson<T>(TextWriter stdout, T value) => stdout.WriteLine(JsonSerializer.Serialize(value, Indented));

    private static string Compact(JsonElement? raw, int n) => raw is { } element ? Truncate(element.GetRawText(), n) : "";

    private static string Truncate(string s, int n)
    {
        s = s.Replace('\n', ' ');
        return s.Length <= n ? s : s[..n] + "…";
    }

    internal sealed record Config
    {
        public string Server { get; init; } = "";

        public string Context { get; init; } = "";

        public string Creds { get; init; } = "";

        public string NKey { get; init; } = "";

        public string Jwt { get; init; } = "";

        public string Seed { get; init; } = "";

        public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(3);

        public bool AsJson { get; init; }

        public string Session { get; init; } = "";

        public string User { get; init; } = "";
    }

    private sealed record Io(TextReader In, TextWriter Out, TextWriter Err);

    private sealed class CliExit(int code, string message) : Exception(message)
    {
        public int Code { get; } = code;
    }
}
