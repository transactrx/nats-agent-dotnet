using System.Text.Json;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent;

/// <summary>
/// Describes the agent. <see cref="Name"/> and <see cref="Description"/> are required;
/// everything else has sensible defaults. NATS connection settings fall back to the org
/// conventions: <c>NATS_URL</c>, <c>NATS_JWT</c>, <c>NATS_KEY</c>.
/// </summary>
public sealed record AgentConfig
{
    public required string Name { get; init; }

    /// <summary>Adds a separately queued regional alias without advertising a second agent.</summary>
    public string? Region { get; init; }

    public string? DisplayName { get; init; }

    public required string Description { get; init; }

    public string? Version { get; init; }

    public string? RepositoryUrl { get; init; }

    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>
    /// Optional, display-only grouping label for UIs (card "category"); short preferred,
    /// single value.
    /// </summary>
    public string? Category { get; init; }

    public IReadOnlyList<Skill>? Skills { get; init; }

    /// <summary>Default ["text"].</summary>
    public IReadOnlyList<string>? InputModalities { get; init; }

    /// <summary>Default ["text"].</summary>
    public IReadOnlyList<string>? OutputModalities { get; init; }

    public bool Attachments { get; init; }

    /// <summary>
    /// Advertises capability "userContext": this agent reads the user's time zone and name
    /// (<see cref="Turn.ContextLine"/>) and answers in local time.
    /// </summary>
    public bool UserContext { get; init; }

    /// <summary>
    /// (opt-in) Advertises capability "progress": <see cref="AgentStream.ProgressAsync"/>
    /// publishes progress frames and callers may apply their idle rules. Default false.
    /// </summary>
    public bool Progress { get; init; }

    /// <summary>
    /// (opt-in) Advertises capability "renew": the agent accepts a fresh IDT mid-run. Default
    /// false.
    /// </summary>
    public bool Renew { get; init; }

    public IReadOnlyDictionary<string, JsonElement>? Metadata { get; init; }

    /// <summary>
    /// Registers this agent with the identity model (card "access"). Null → read
    /// <c>APP_ID</c> / <c>APP_FUNCTION_ID</c> from the environment.
    /// </summary>
    public AgentAccess? Access { get; init; }

    /// <summary>Controls inbound token checks. Null → <see cref="IdtValidation.FromEnvironment(ILogger?)"/>.</summary>
    public IdtValidation? IdtValidation { get; init; }

    /// <summary>Optional NATS overrides (default: environment).</summary>
    public NatsSettings? Nats { get; init; }

    /// <summary>
    /// An existing connection to serve on instead of opening one. The caller keeps ownership:
    /// the agent never closes it.
    /// </summary>
    public NatsConnection? Connection { get; init; }

    /// <summary>Where the agent logs. Default: one line per entry on stdout.</summary>
    public ILoggerFactory? LoggerFactory { get; init; }

    /// <summary>Clock for budgets and the progress throttle (tests).</summary>
    public TimeProvider? TimeProvider { get; init; }
}
