using System.Text.Json.Serialization;

namespace TransactRx.NatsAgent.Wire;

// ─── Sessions (capability "sessions") ─────────────────────────────────────

public sealed record SessionMeta
{
    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("title")]
    public string? Title { get; init; }

    [JsonPropertyName("favorite")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Favorite { get; init; }

    /// <summary>RFC 3339.</summary>
    [JsonPropertyName("createdAt")]
    public string? CreatedAt { get; init; }

    /// <summary>RFC 3339.</summary>
    [JsonPropertyName("lastMessageAt")]
    public string? LastMessageAt { get; init; }
}

public sealed record StoredMessage
{
    [JsonPropertyName("role")]
    public string Role { get; init; } = "";

    // "content":null (a Go nil slice) reads as empty.
    [JsonPropertyName("content")]
    public IReadOnlyList<ContentBlock> Content { get; init => field = value ?? []; } = [];

    /// <summary>RFC 3339.</summary>
    [JsonPropertyName("timestamp")]
    public string? Timestamp { get; init; }

    /// <summary>
    /// (v1.5) Absent for a complete message; otherwise the reason the message was cut short
    /// (a <see cref="Reasons"/> value, e.g. time_cap, interrupted, or "error"). Optional;
    /// older peers ignore it.
    /// </summary>
    [JsonPropertyName("incomplete")]
    public string? Incomplete { get; init; }
}

public sealed record SessionsListRequest
{
    [JsonPropertyName("userId")]
    public string UserId { get; init; } = "";
}

public sealed record SessionsListResponse
{
    // "sessions":null (a Go nil slice) reads as empty.
    [JsonPropertyName("sessions")]
    public IReadOnlyList<SessionMeta> Sessions { get; init => field = value ?? []; } = [];
}

public sealed record SessionGetRequest
{
    [JsonPropertyName("userId")]
    public string UserId { get; init; } = "";

    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";
}

public sealed record SessionGetResponse
{
    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("title")]
    public string? Title { get; init; }

    [JsonPropertyName("favorite")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Favorite { get; init; }

    // "messages":null (a Go nil slice) reads as empty.
    [JsonPropertyName("messages")]
    public IReadOnlyList<StoredMessage> Messages { get; init => field = value ?? []; } = [];
}

public sealed record SessionDeleteRequest
{
    [JsonPropertyName("userId")]
    public string UserId { get; init; } = "";

    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";
}

public sealed record SessionDeleteResponse
{
    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("deleted")]
    public bool Deleted { get; init; }
}

public sealed record SessionRenameRequest
{
    [JsonPropertyName("userId")]
    public string UserId { get; init; } = "";

    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("title")]
    public string Title { get; init; } = "";
}

public sealed record SessionRenameResponse
{
    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("title")]
    public string Title { get; init; } = "";
}

public sealed record SessionSetFavoriteRequest
{
    [JsonPropertyName("userId")]
    public string UserId { get; init; } = "";

    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("favorite")]
    public bool Favorite { get; init; }
}

public sealed record SessionSetFavoriteResponse
{
    [JsonPropertyName("sessionId")]
    public string SessionId { get; init; } = "";

    [JsonPropertyName("favorite")]
    public bool Favorite { get; init; }
}
