using System.Text.Json;
using System.Text.Json.Serialization;

namespace TransactRx.NatsAgent.Wire;

// ─── Conversation content (identical to inferenceGateway) ────────────────

public sealed record Message
{
    /// <summary>"user" or "assistant".</summary>
    [JsonPropertyName("role")]
    public string Role { get; init; } = "";

    // A Go peer writes a nil slice as "content":null; read it as empty, never as null.
    [JsonPropertyName("content")]
    public IReadOnlyList<ContentBlock> Content { get; init => field = value ?? []; } = [];

    /// <summary>A user message holding one text block.</summary>
    public static Message UserText(string text) => new() { Role = "user", Content = [new ContentBlock { Text = text }] };

    /// <summary>An assistant message holding one text block.</summary>
    public static Message AssistantText(string text) => new() { Role = "assistant", Content = [new ContentBlock { Text = text }] };
}

public sealed record ContentBlock
{
    [JsonPropertyName("text")]
    public string? Text { get; init; }

    [JsonPropertyName("image")]
    public ImageBlock? Image { get; init; }

    [JsonPropertyName("toolUse")]
    public ToolUseBlock? ToolUse { get; init; }

    [JsonPropertyName("toolResult")]
    public ToolResultBlock? ToolResult { get; init; }
}

public sealed record ImageBlock
{
    /// <summary>"png", "jpeg", "gif", "webp".</summary>
    [JsonPropertyName("format")]
    public string Format { get; init; } = "";

    [JsonPropertyName("base64")]
    public string Base64 { get; init; } = "";
}

public sealed record ToolUseBlock
{
    [JsonPropertyName("toolUseId")]
    public string ToolUseId { get; init; } = "";

    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("input")]
    public JsonElement? Input { get; init; }
}

public sealed record ToolResultBlock
{
    [JsonPropertyName("toolUseId")]
    public string ToolUseId { get; init; } = "";

    // "content":null (a Go nil slice) reads as empty.
    [JsonPropertyName("content")]
    public IReadOnlyList<ToolResultContent> Content { get; init => field = value ?? []; } = [];

    /// <summary>"success" or "error".</summary>
    [JsonPropertyName("status")]
    public string? Status { get; init; }
}

public sealed record ToolResultContent
{
    [JsonPropertyName("text")]
    public string? Text { get; init; }

    [JsonPropertyName("json")]
    public JsonElement? Json { get; init; }
}

public sealed record Usage
{
    [JsonPropertyName("inputTokens")]
    public int InputTokens { get; init; }

    [JsonPropertyName("outputTokens")]
    public int OutputTokens { get; init; }

    [JsonPropertyName("totalTokens")]
    public int TotalTokens { get; init; }
}
