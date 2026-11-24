using System.Text.Json;
using System.Text.Json.Serialization;

namespace TransactRx.NatsAgent.Wire;

public sealed record ToolRunRequest
{
    [JsonPropertyName("toolUseId")]
    public string? ToolUseId { get; init; }

    /// <summary>A JSON object conforming to the tool card's inputSchema.</summary>
    [JsonPropertyName("input")]
    [JsonIgnore(Condition = JsonIgnoreCondition.Never)]
    public JsonElement? Input { get; init; }

    [JsonPropertyName("userId")]
    public string? UserId { get; init; }

    /// <summary>
    /// (v1.2) Names the conversation the call belongs to, so a tool can keep per-conversation
    /// state. It is a locator, never a capability: tools bind it to the verified caller.
    /// </summary>
    [JsonPropertyName("sessionId")]
    public string? SessionId { get; init; }

    [JsonPropertyName("agent")]
    public string? Agent { get; init; }

    [JsonPropertyName("metadata")]
    public IReadOnlyDictionary<string, JsonElement>? Metadata { get; init; }
}

public sealed record ToolRunResponse
{
    [JsonPropertyName("toolUseId")]
    public string? ToolUseId { get; init; }

    /// <summary>"success" or "error" (<see cref="ToolStatuses"/>).</summary>
    [JsonPropertyName("status")]
    public string Status { get; init; } = "";

    [JsonPropertyName("content")]
    public IReadOnlyList<ToolResultContent>? Content { get; init; }

    /// <summary>On status "error": the message the model should see.</summary>
    [JsonPropertyName("error")]
    public string? Error { get; init; }

    [JsonPropertyName("latencyMs")]
    public long LatencyMs { get; init; }
}
