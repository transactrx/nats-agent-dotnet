using System.Text.Json;
using System.Text.Json.Serialization;

namespace TransactRx.NatsAgent.Wire;

// ─── Chat protocol ────────────────────────────────────────────────────────

public sealed record ChatRequest
{
    [JsonPropertyName("sessionId")]
    public string? SessionId { get; init; }

    [JsonPropertyName("userId")]
    public string? UserId { get; init; }

    [JsonPropertyName("message")]
    public Message Message { get; init; } = new();

    [JsonPropertyName("streamSubject")]
    public string StreamSubject { get; init; } = "";

    [JsonPropertyName("metadata")]
    public IReadOnlyDictionary<string, JsonElement>? Metadata { get; init; }

    /// <summary>
    /// (v1.3) Display-only user context (time zone, name, locale). Never authorization.
    /// Agents read it normalized (see <see cref="UserContext"/>).
    /// </summary>
    [JsonPropertyName("context")]
    public UserContext? Context { get; init; }

    /// <summary>
    /// (v1.5) The time the caller still allows this turn. Absent = the agent's own limits
    /// apply.
    /// </summary>
    [JsonPropertyName("budget")]
    public Budget? Budget { get; init; }
}

/// <summary>(v1.5) Carries the caller's remaining time for a turn.</summary>
public sealed record Budget
{
    [JsonPropertyName("remainingMs")]
    public long RemainingMs { get; init; }
}

/// <summary>(v1.5) Tells the caller what this run of the agent supports.</summary>
public sealed record AckCaps
{
    [JsonPropertyName("progress")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Progress { get; init; }

    /// <summary>Where a <see cref="RenewRequest"/> goes.</summary>
    [JsonPropertyName("renewSubject")]
    public string? RenewSubject { get; init; }
}

public sealed record ChatAck
{
    [JsonPropertyName("accepted")]
    public bool Accepted { get; init; }

    [JsonPropertyName("runId")]
    public string RunId { get; init; } = "";

    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("agent")]
    public string Agent { get; init; } = "";

    [JsonPropertyName("instanceId")]
    public string InstanceId { get; init; } = "";

    [JsonPropertyName("streamSubject")]
    public string StreamSubject { get; init; } = "";

    /// <summary>(v1.5) Absent on older agents.</summary>
    [JsonPropertyName("caps")]
    public AckCaps? Caps { get; init; }
}

/// <summary>(v1.5) The payload of a data event of kind "progress".</summary>
public sealed record ProgressPayload
{
    [JsonPropertyName("stage")]
    public string Stage { get; init; } = "";

    [JsonPropertyName("source")]
    public string? Source { get; init; }

    [JsonPropertyName("detail")]
    public string? Detail { get; init; }
}

/// <summary>(v1.5) Hands a running turn a fresh IDT.</summary>
public sealed class RenewRequest
{
    [JsonPropertyName("runId")]
    public string RunId { get; init; } = "";

    [JsonPropertyName("idt")]
    public string Idt { get; init; } = "";

    /// <summary>Expiry of the new token, unix ms. 0 = not stated.</summary>
    [JsonPropertyName("expiresAtMs")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public long ExpiresAtMs { get; init; }
}

public sealed record RenewResponse
{
    [JsonPropertyName("applied")]
    public bool Applied { get; init; }

    [JsonPropertyName("reason")]
    public string? Reason { get; init; }
}

public sealed record AgentEvent
{
    [JsonPropertyName("runId")]
    public string RunId { get; init; } = "";

    [JsonPropertyName("seq")]
    public int Seq { get; init; }

    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    /// <summary>start</summary>
    [JsonPropertyName("sessionId")]
    public string? SessionId { get; init; }

    /// <summary>text</summary>
    [JsonPropertyName("textDelta")]
    public string? TextDelta { get; init; }

    /// <summary>toolUse, toolResult</summary>
    [JsonPropertyName("toolUseId")]
    public string? ToolUseId { get; init; }

    /// <summary>toolUse, toolResult</summary>
    [JsonPropertyName("toolName")]
    public string? ToolName { get; init; }

    /// <summary>toolUse</summary>
    [JsonPropertyName("toolInput")]
    public JsonElement? ToolInput { get; init; }

    /// <summary>toolResult</summary>
    [JsonPropertyName("toolResult")]
    public string? ToolResult { get; init; }

    /// <summary>toolResult</summary>
    [JsonPropertyName("toolError")]
    public string? ToolError { get; init; }

    /// <summary>data</summary>
    [JsonPropertyName("kind")]
    public string? Kind { get; init; }

    /// <summary>data</summary>
    [JsonPropertyName("payload")]
    public JsonElement? Payload { get; init; }

    /// <summary>status</summary>
    [JsonPropertyName("statusText")]
    public string? StatusText { get; init; }

    /// <summary>done</summary>
    [JsonPropertyName("stopReason")]
    public string? StopReason { get; init; }

    /// <summary>done</summary>
    [JsonPropertyName("usage")]
    public Usage? Usage { get; init; }

    /// <summary>error</summary>
    [JsonPropertyName("error")]
    public string? Error { get; init; }

    /// <summary>error, SPEC §9 code</summary>
    [JsonPropertyName("errorStatus")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int ErrorStatus { get; init; }

    /// <summary>done, error (v1.5 cut reason)</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; init; }

    /// <summary>
    /// Whether this event shows the run is making progress. Every event counts except pings,
    /// which only prove liveness.
    /// </summary>
    [JsonIgnore]
    public bool IsProgress => Type != EventTypes.Ping;

    /// <summary>Whether this event ends the run.</summary>
    [JsonIgnore]
    public bool IsTerminal => Type is EventTypes.Done or EventTypes.Error;
}

public sealed record CancelRequest
{
    [JsonPropertyName("runId")]
    public string RunId { get; init; } = "";

    /// <summary>
    /// (v1.5, optional) Marks the cancel as a cut by the caller's limits
    /// (<see cref="Reasons.ProgressIdle"/>, <see cref="Reasons.TokenRotation"/>,
    /// <see cref="Reasons.Interrupted"/>, <see cref="Reasons.TimeCap"/>,
    /// <see cref="Reasons.Unavailable"/>). Empty or unknown = a plain (user) cancel.
    /// </summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; init; }
}

public sealed record CancelResponse
{
    [JsonPropertyName("cancelled")]
    public bool Cancelled { get; init; }
}

/// <summary>
/// The reply of the synchronous invoke endpoint (capability "sync"). The request shape is
/// <see cref="ChatRequest"/> without streamSubject.
/// </summary>
public sealed record InvokeResponse
{
    [JsonPropertyName("runId")]
    public string RunId { get; init; } = "";

    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("message")]
    public Message Message { get; init; } = new();

    [JsonPropertyName("stopReason")]
    public string StopReason { get; init; } = "";

    [JsonPropertyName("usage")]
    public Usage? Usage { get; init; }
}
