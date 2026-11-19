using Microsoft.Extensions.Logging.Abstractions;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/agent/access_test.go: access from the environment, and configuration checks
/// that run before the agent connects.
/// </summary>
public sealed class AgentAccessTests
{
    /// <summary>
    /// The Go <c>t.Setenv</c>: sets process environment variables for the duration of a test
    /// and puts the previous values back. Tests run one at a time, so nothing else sees them.
    /// </summary>
    private sealed class EnvScope : IDisposable
    {
        private readonly List<(string Name, string? Previous)> _previous = [];

        public EnvScope Set(string name, string value)
        {
            _previous.Add((name, Environment.GetEnvironmentVariable(name)));
            Environment.SetEnvironmentVariable(name, value);
            return this;
        }

        public void Dispose()
        {
            for (var i = _previous.Count - 1; i >= 0; i--)
            {
                Environment.SetEnvironmentVariable(_previous[i].Name, _previous[i].Previous);
            }
        }
    }

    [Fact]
    public void AccessFromEnvRequiresBoth()
    {
        static AgentAccess? Access(string appId, string functionId)
        {
            var env = new Dictionary<string, string> { ["APP_ID"] = appId, ["APP_FUNCTION_ID"] = functionId };
            return Agent.AccessFromEnvironment(name => env.GetValueOrDefault(name), NullLogger.Instance);
        }

        Assert.Null(Access("", "")); // neither APP_ID nor APP_FUNCTION_ID is set
        Assert.Null(Access("x", "")); // only APP_ID is set
        Assert.Null(Access("", "y")); // only APP_FUNCTION_ID is set

        var a = Access("x", "y");
        Assert.NotNull(a); // populated when both are set
        Assert.Equal(("x", "y"), (a.AppId, a.FunctionId));

        // Unset is the same as empty.
        Assert.Null(Agent.AccessFromEnvironment(_ => null, NullLogger.Instance));
    }

    [Fact]
    public async Task NewRejectsPartialAccess()
    {
        using var env = new EnvScope().Set("NATS_URL", "").Set("APP_ID", "").Set("APP_FUNCTION_ID", "");
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => Agent.CreateAsync(new AgentConfig
        {
            Name = "x",
            Description = "d",
            Access = new AgentAccess { AppId = "a" }, // AppId set, FunctionId blank
            LoggerFactory = NullLoggerFactory.Instance,
        }));
        Assert.Contains("FunctionId", ex.Message); // the Go message says FunctionID
    }

    [Fact]
    public async Task NewRejectsIDTValidationWithoutAccessBeforeNATSConnect()
    {
        // No NATS_URL is set (or reachable) for this agent; if the misconfig check ran after
        // the connect attempt this would instead fail with a connection error (an
        // InvalidOperationException), not the intended message.
        using var env = new EnvScope().Set("NATS_URL", "").Set("APP_ID", "").Set("APP_FUNCTION_ID", "");
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => Agent.CreateAsync(new AgentConfig
        {
            Name = "x",
            Description = "d",
            IdtValidation = new IdtValidation { Enabled = true },
            LoggerFactory = NullLoggerFactory.Instance,
        }));
        Assert.Contains("APP_ID", ex.Message);
    }
}
