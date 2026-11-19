using System.Text.Json;
using System.Text.Json.Serialization;

namespace TransactRx.NatsAgent.Wire;

// ─── Discovery ────────────────────────────────────────────────────────────

/// <summary>
/// The request body on the discover subjects. An empty filter matches everything. Agents and
/// tools that do not match stay silent.
/// </summary>
public sealed record DiscoverFilter
{
    /// <summary>Exact name match.</summary>
    [JsonPropertyName("name")]
    public string? Name { get; init; }

    /// <summary>Agents only: matches any skill name.</summary>
    [JsonPropertyName("skill")]
    public string? Skill { get; init; }

    /// <summary>All must be present.</summary>
    [JsonPropertyName("tags")]
    public IReadOnlyList<string>? Tags { get; init; }
}

public sealed record AgentCard
{
    [JsonPropertyName("protocolVersion")]
    public string ProtocolVersion { get; init; } = "";

    /// <summary>Always "agent".</summary>
    [JsonPropertyName("kind")]
    public string Kind { get; init; } = "";

    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("displayName")]
    public string? DisplayName { get; init; }

    [JsonPropertyName("description")]
    public string Description { get; init; } = "";

    [JsonPropertyName("version")]
    public string? Version { get; init; }

    [JsonPropertyName("repositoryUrl")]
    public string? RepositoryUrl { get; init; }

    [JsonPropertyName("tags")]
    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>
    /// Optional, display-only grouping label chosen by the agent (e.g. "Pharmacy"); short
    /// preferred. Never used for routing or authorization (SPEC v1.4).
    /// </summary>
    [JsonPropertyName("category")]
    public string? Category { get; init; }

    [JsonPropertyName("capabilities")]
    public Capabilities Capabilities { get; init; } = new();

    [JsonPropertyName("inputModalities")]
    public IReadOnlyList<string>? InputModalities { get; init; }

    [JsonPropertyName("outputModalities")]
    public IReadOnlyList<string>? OutputModalities { get; init; }

    [JsonPropertyName("skills")]
    public IReadOnlyList<Skill>? Skills { get; init; }

    [JsonPropertyName("endpoints")]
    public IReadOnlyList<string>? Endpoints { get; init; }

    [JsonPropertyName("metadata")]
    public IReadOnlyDictionary<string, JsonElement>? Metadata { get; init; }

    /// <summary>
    /// The agent's identity registration: the application id and the single RBAC function a
    /// user must hold to talk to this agent (SPEC §4.2). Absent = undeclared.
    /// </summary>
    [JsonPropertyName("access")]
    public AgentAccess? Access { get; init; }

    /// <summary>Reports whether this card matches a discovery filter.</summary>
    public bool MatchesFilter(DiscoverFilter? filter)
    {
        if (filter is null)
        {
            return true;
        }

        if (!string.IsNullOrEmpty(filter.Name) && filter.Name != Name)
        {
            return false;
        }

        if (!string.IsNullOrEmpty(filter.Skill) && !(Skills?.Any(s => s.Name == filter.Skill) ?? false))
        {
            return false;
        }

        return CardFilters.ContainsAllTags(Tags, filter.Tags);
    }
}

public sealed record Capabilities
{
    [JsonPropertyName("streaming")]
    public bool Streaming { get; init; }

    [JsonPropertyName("sync")]
    public bool Sync { get; init; }

    [JsonPropertyName("sessions")]
    public bool Sessions { get; init; }

    [JsonPropertyName("attachments")]
    public bool Attachments { get; init; }

    /// <summary>
    /// (v1.3) The agent reads <see cref="ChatRequest.Context"/> (user's time zone and name)
    /// and answers in the user's local time.
    /// </summary>
    [JsonPropertyName("userContext")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool UserContext { get; init; }

    /// <summary>
    /// (v1.5) The agent emits progress data events and honors <see cref="ChatRequest.Budget"/>,
    /// so callers may cut a turn on progress idleness.
    /// </summary>
    [JsonPropertyName("progress")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Progress { get; init; }

    /// <summary>(v1.5) The agent accepts a fresh IDT mid-run on the ack's <c>caps.renewSubject</c>.</summary>
    [JsonPropertyName("renew")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Renew { get; init; }
}

/// <summary>
/// Maps an agent (or tool) onto the identity model: it IS an application (<see cref="AppId"/>)
/// exposing one access function (<see cref="FunctionId"/>).
/// </summary>
public sealed record AgentAccess
{
    [JsonPropertyName("appId")]
    public string AppId { get; init; } = "";

    [JsonPropertyName("functionId")]
    public string FunctionId { get; init; } = "";
}

public sealed record Skill
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("description")]
    public string Description { get; init; } = "";

    [JsonPropertyName("examples")]
    public IReadOnlyList<string>? Examples { get; init; }
}

public sealed record PingResponse
{
    /// <summary>"ok".</summary>
    [JsonPropertyName("status")]
    public string Status { get; init; } = "";

    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("instanceId")]
    public string InstanceId { get; init; } = "";

    [JsonPropertyName("version")]
    public string? Version { get; init; }

    [JsonPropertyName("uptimeSeconds")]
    public long UptimeSeconds { get; init; }
}

// ─── Network tools ────────────────────────────────────────────────────────

public sealed record ToolCard
{
    [JsonPropertyName("protocolVersion")]
    public string ProtocolVersion { get; init; } = "";

    /// <summary>Always "tool".</summary>
    [JsonPropertyName("kind")]
    public string Kind { get; init; } = "";

    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("displayName")]
    public string? DisplayName { get; init; }

    /// <summary>Written for the model: it becomes the tool description the LLM sees.</summary>
    [JsonPropertyName("description")]
    public string Description { get; init; } = "";

    [JsonPropertyName("version")]
    public string? Version { get; init; }

    [JsonPropertyName("repositoryUrl")]
    public string? RepositoryUrl { get; init; }

    [JsonPropertyName("tags")]
    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>A JSON Schema object, handed verbatim to the inference gateway.</summary>
    [JsonPropertyName("inputSchema")]
    [JsonIgnore(Condition = JsonIgnoreCondition.Never)]
    public JsonElement? InputSchema { get; init; }

    [JsonPropertyName("timeoutSeconds")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int TimeoutSeconds { get; init; }

    /// <summary>
    /// (v1.2) Registers the tool with the identity model, like an agent card's access: a
    /// caller may run the tool iff the user behind their token holds functionId. Absent = runs
    /// are not authenticated.
    /// </summary>
    [JsonPropertyName("access")]
    public AgentAccess? Access { get; init; }

    [JsonPropertyName("metadata")]
    public IReadOnlyDictionary<string, JsonElement>? Metadata { get; init; }

    /// <summary>
    /// Reports whether this card matches a discovery filter. The skill field never matches
    /// tools.
    /// </summary>
    public bool MatchesFilter(DiscoverFilter? filter)
    {
        if (filter is null)
        {
            return true;
        }

        if (!string.IsNullOrEmpty(filter.Name) && filter.Name != Name)
        {
            return false;
        }

        if (!string.IsNullOrEmpty(filter.Skill))
        {
            return false;
        }

        return CardFilters.ContainsAllTags(Tags, filter.Tags);
    }
}

internal static class CardFilters
{
    public static bool ContainsAllTags(IReadOnlyList<string>? have, IReadOnlyList<string>? want)
    {
        if (want is null)
        {
            return true;
        }

        foreach (var w in want)
        {
            if (have is null || !have.Contains(w))
            {
                return false;
            }
        }

        return true;
    }
}
