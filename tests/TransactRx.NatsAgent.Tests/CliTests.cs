using System.Text;
using System.Text.RegularExpressions;
using TransactRx.NatsAgent.Cli;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of cli_test.go (<c>TestCLI</c> and its subtests): drives the nats-agents CLI against
/// the test server with a live agent and tool host — the CLI is exercised exactly as an
/// operator would use it. The Go test builds the binary and runs it as a process; here
/// <see cref="NatsAgentsCli.RunAsync"/> is called in-process with its own stdin, stdout, stderr
/// and environment.
/// </summary>
public sealed partial class CliTests(NatsServerFixture nats) : NatsTestBase(nats)
{
    [GeneratedRegex(@"run run_\S+ session (\S+)")]
    private static partial Regex SessionRegex();

    /// <summary>The developer's environment never leaks into the CLI under test.</summary>
    private static string? NoEnvironment(string name) => null;

    public override async ValueTask InitializeAsync()
    {
        await StartTestAgentAsync();
        var h = NewHost();
        h.Register(new EchoTool());
        await h.StartAsync();
    }

    private sealed record CliResult(int ExitCode, string Stdout, string Stderr)
    {
        /// <summary>stdout and stderr together (the Go <c>CombinedOutput</c>).</summary>
        public string Combined => Stdout + Stderr;
    }

    private static async Task<CliResult> RunCliAsync(string[] args, string stdin, Func<string, string?> env)
    {
        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var run = NatsAgentsCli.RunAsync(args, new StringReader(stdin), stdout, stderr, env);
        var code = await run.WaitAsync(TimeSpan.FromSeconds(60));
        return new CliResult(code, stdout.ToString(), stderr.ToString());
    }

    /// <summary>The Go <c>cli</c> helper: every call names the test server and a 700ms window.</summary>
    private Task<CliResult> CliAsync(params string[] args) =>
        RunCliAsync(["-s", Url, "--timeout", "700ms", .. args], "", NoEnvironment);

    private static string SessionFromStderr(string stderr)
    {
        var m = SessionRegex().Match(stderr);
        Assert.True(m.Success, "no session id in stderr:\n" + stderr);
        return m.Groups[1].Value;
    }

    [Fact]
    public async Task Cli_List()
    {
        var r = await CliAsync("list");
        Assert.True(r.ExitCode == 0, "list: exit " + r.ExitCode + "\n" + r.Stderr);
        Assert.True(r.Stdout.Contains("testAgent") && r.Stdout.Contains("sessions"), "list output missing agent info:\n" + r.Stdout);
    }

    [Fact]
    public async Task Cli_Tools()
    {
        var r = await CliAsync("tools", "--json");
        Assert.True(r.ExitCode == 0, "tools: exit " + r.ExitCode + "\n" + r.Stderr);
        var cards = WireJson.Deserialize<List<ToolCard>>(Encoding.UTF8.GetBytes(r.Stdout)); // tools --json must be parseable
        Assert.NotNull(cards);
        Assert.True(cards.Count > 0 && cards[0].Name == "echo", "echo tool not discovered: " + r.Stdout);
    }

    [Fact]
    public async Task Cli_CardAndPing()
    {
        var r = await CliAsync("card", "testAgent");
        Assert.True(r.ExitCode == 0 && r.Stdout.Contains("\"protocolVersion\""), $"card failed: exit {r.ExitCode}\n{r.Stdout}{r.Stderr}");

        r = await CliAsync("ping", "testAgent");
        Assert.True(r.ExitCode == 0 && r.Stdout.Contains("\"ok\""), $"ping failed: exit {r.ExitCode}\n{r.Stdout}{r.Stderr}");
    }

    [Fact]
    public async Task Cli_Chat()
    {
        var r = await CliAsync("chat", "testAgent", "hello cli");
        Assert.True(r.ExitCode == 0, $"chat: exit {r.ExitCode}\nstderr: {r.Stderr}");
        Assert.True(r.Stdout.Contains("you said: hello cli"), "chat reply missing:\nstdout: " + r.Stdout);
        Assert.True(r.Stderr.Contains("[tool] echo") && r.Stderr.Contains("[done] endTurn"), "chat progress events missing:\nstderr: " + r.Stderr);
    }

    [Fact]
    public async Task Cli_ChatSessionResumes()
    {
        var first = await CliAsync("chat", "testAgent", "first turn");
        Assert.True(first.ExitCode == 0, $"chat: exit {first.ExitCode}\nstderr: {first.Stderr}");
        var sid = SessionFromStderr(first.Stderr);

        var second = await CliAsync("chat", "testAgent", "--session", sid, "second turn");
        Assert.True(second.ExitCode == 0, $"chat --session: exit {second.ExitCode}\nstderr: {second.Stderr}");
        var got = SessionFromStderr(second.Stderr);
        Assert.True(got == sid, $"session not resumed: sent {sid}, ack says {got}");
    }

    [Fact]
    public async Task Cli_ChatInteractiveKeepsOneSession()
    {
        var r = await RunCliAsync(["-s", Url, "--timeout", "700ms", "chat", "testAgent"], "turn one\nturn two\nexit\n", NoEnvironment);
        Assert.True(r.ExitCode == 0, $"interactive chat: exit {r.ExitCode}\nstderr: {r.Stderr}");
        Assert.True(
            r.Stdout.Contains("you said: turn one") && r.Stdout.Contains("you said: turn two"),
            "interactive replies missing:\nstdout: " + r.Stdout);

        // Both turns must share one session, and the exit hint must name it.
        var sids = SessionRegex().Matches(r.Stderr).Select(m => m.Groups[1].Value).ToList();
        Assert.True(sids.Count == 2 && sids[0] == sids[1], $"expected 2 turns on one session, got [{string.Join(' ', sids)}]\nstderr: {r.Stderr}");
        Assert.True(r.Stderr.Contains("--session " + sids[0]), "resume hint missing:\nstderr: " + r.Stderr);
    }

    [Fact]
    public async Task Cli_NatsContextResolution()
    {
        // A fake HOME with a nats CLI context pointing at the test server; no -s flag and no
        // NATS_URL in the environment.
        var home = Path.Combine(AppContext.BaseDirectory, "cli-home-" + Guid.NewGuid().ToString("N"));
        var contextDir = Path.Combine(home, ".config", "nats", "context");
        Directory.CreateDirectory(contextDir);
        Cleanup(() =>
        {
            Directory.Delete(home, recursive: true);
            return ValueTask.CompletedTask;
        });
        await File.WriteAllTextAsync(Path.Combine(contextDir, "embedded.json"), "{\"url\":\"" + Url + "\",\"description\":\"embedded test server\"}");

        // The environment of the Go subtest: HOME and PATH, nothing else.
        var env = new Dictionary<string, string?> { ["HOME"] = home, ["PATH"] = Environment.GetEnvironmentVariable("PATH") };

        // Explicit --context
        var r = await RunCliAsync(["--context", "embedded", "--timeout", "700ms", "list"], "", name => env.GetValueOrDefault(name));
        Assert.True(r.ExitCode == 0 && r.Combined.Contains("testAgent"), $"--context list failed: exit {r.ExitCode}\n{r.Combined}");

        // The context selected via `nats context select` (context.txt) is used with no options
        // at all — and wins over $NATS_URL.
        await File.WriteAllTextAsync(Path.Combine(home, ".config", "nats", "context.txt"), "embedded\n");
        var withUrl = new Dictionary<string, string?>(env) { ["NATS_URL"] = "nats://selected-context-should-win.invalid:4222" };
        r = await RunCliAsync(["--timeout", "700ms", "tools"], "", name => withUrl.GetValueOrDefault(name));
        Assert.True(r.ExitCode == 0 && r.Combined.Contains("echo"), $"selected-context tools failed (context must beat $NATS_URL): exit {r.ExitCode}\n{r.Combined}");
    }

    [Fact]
    public async Task Cli_RunTool()
    {
        var r = await CliAsync("run", "echo", """{"say":"cli"}""");
        Assert.True(r.ExitCode == 0, $"run: exit {r.ExitCode}\n{r.Stdout}{r.Stderr}");
        var resp = WireJson.Deserialize<ToolRunResponse>(Encoding.UTF8.GetBytes(r.Stdout)); // run output must be JSON
        Assert.NotNull(resp);
        Assert.True(
            resp.Status == ToolStatuses.Success && resp.Content is { Count: > 0 } && resp.Content[0].Text!.Contains("cli"),
            "bad run response: " + r.Stdout);
    }
}
