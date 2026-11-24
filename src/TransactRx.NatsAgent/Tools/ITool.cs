using System.Text.Json;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tools;

/// <summary>
/// The contract a network tool implements. It mirrors the local-tool contract (name,
/// description, input schema, run), so an in-process tool can be lifted onto the network
/// unchanged.
/// </summary>
public interface ITool
{
    string Name { get; }

    /// <summary>Written for the model: it becomes the tool description the LLM sees.</summary>
    string Description { get; }

    /// <summary>A JSON Schema object describing <c>input</c>.</summary>
    JsonElement InputSchema { get; }

    /// <summary>
    /// Runs the tool and returns the text result. A thrown exception is a model-visible
    /// failure (status "error" with the exception message), not a protocol error.
    /// </summary>
    Task<string> RunAsync(JsonElement input, CancellationToken cancellationToken);
}

/// <summary>Everything the host knows about one run request (v1.2).</summary>
public sealed record ToolCall
{
    public string? ToolUseId { get; init; }

    public string? SessionId { get; init; }

    public string? Agent { get; init; }

    /// <summary>A JSON object (<c>{}</c> when the caller sent none).</summary>
    public JsonElement Input { get; init; }

    public IReadOnlyDictionary<string, JsonElement>? Metadata { get; init; }

    /// <summary>
    /// The host's verdict on the caller. When <c>Identity.Verified</c>, UserId came from the
    /// token; otherwise it is the caller's assertion.
    /// </summary>
    public CallerIdentity Identity { get; init; } = new();

    /// <summary>
    /// The user the call is for: the verified one when <see cref="Identity"/> is verified,
    /// else the caller's unauthenticated assertion.
    /// </summary>
    public string? UserId { get; init; }
}

/// <summary>
/// An optional extension of <see cref="ITool"/>. A tool implementing it receives the full
/// request (caller identity, session, correlation id) and may return structured content; the
/// host then calls <see cref="RunCallAsync"/> instead of <see cref="ITool.RunAsync"/>. While
/// it runs, the caller's token is the ambient one (<see cref="CallContext.Idt"/>), so onward
/// authenticated calls forward it.
/// </summary>
public interface ICallTool : ITool
{
    Task<IReadOnlyList<ToolResultContent>> RunCallAsync(ToolCall call, CancellationToken cancellationToken);
}

/// <summary>
/// The deployment metadata a tool implementation doesn't know about itself. All fields are
/// optional.
/// </summary>
public sealed record ToolInfo
{
    public string? DisplayName { get; init; }

    public string? Version { get; init; }

    public string? RepositoryUrl { get; init; }

    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>Advisory worst-case runtime; default 30.</summary>
    public int TimeoutSeconds { get; init; }

    public IReadOnlyDictionary<string, JsonElement>? Metadata { get; init; }

    /// <summary>
    /// Registers the tool with the identity model (card "access"). Required when the host
    /// validates tokens.
    /// </summary>
    public AgentAccess? Access { get; init; }

    /// <summary>
    /// Bounds simultaneous runs of this tool per host process; excess requests get 429/4291.
    /// 0 = unbounded.
    /// </summary>
    public int MaxConcurrent { get; init; }
}
