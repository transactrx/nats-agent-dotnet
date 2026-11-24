namespace TransactRx.NatsAgent.Wire;

/// <summary>
/// Constants of the NATS Agent Protocol (SPEC.md in the Go repository). Everything in this
/// namespace is language-neutral: the Go and Rust bindings serialize the same shapes.
/// </summary>
public static class Protocol
{
    /// <summary>The version of the agent protocol this library speaks.</summary>
    public const string Version = "1.5";

    // Subject layout. Agents own <AgentPrefix>.<name>.>, tools own <ToolPrefix>.<name>.>.
    public const string AgentPrefix = "trx.agent";
    public const string ToolPrefix = "trx.tool";
    public const string AgentDiscoverSubject = AgentPrefix + ".discover";
    public const string ToolDiscoverSubject = ToolPrefix + ".discover";
    public const string ToolAnnounceSubject = ToolPrefix + ".announce";

    /// <summary>
    /// NATS message header carrying the caller's Internal Delegation Token on
    /// chat/invoke/sessions requests (SPEC §5.1) and tool runs (SPEC §8.4).
    /// </summary>
    public const string HeaderIdt = "X-TRX-IDT";

    // Card kinds, so a mixed consumer can browse agents and tools through the same machinery.
    public const string KindAgent = "agent";
    public const string KindTool = "tool";

    public static string AgentSubject(string name, string endpoint) => AgentPrefix + "." + name + "." + endpoint;

    public static string ToolSubject(string name, string endpoint) => ToolPrefix + "." + name + "." + endpoint;
}

/// <summary>Event types published to the stream subject (<see cref="AgentEvent.Type"/>).</summary>
public static class EventTypes
{
    public const string Start = "start";
    public const string Text = "text";
    public const string ToolUse = "toolUse";
    public const string ToolResult = "toolResult";
    public const string Data = "data";
    public const string Status = "status";
    public const string Ping = "ping";
    public const string Done = "done";
    public const string Error = "error";
}

/// <summary>Stop reasons carried by the done event.</summary>
public static class StopReasons
{
    public const string EndTurn = "endTurn";
    public const string MaxTokens = "maxTokens";
    public const string Cancelled = "cancelled";
    public const string MaxIterations = "maxIterations";
}

/// <summary>
/// Cut reasons (v1.5) carried by <see cref="AgentEvent.Reason"/> on done/error events when a
/// turn was ended by a limit rather than by the model.
/// </summary>
public static class Reasons
{
    public const string TimeCap = "time_cap";
    public const string ProgressIdle = "progress_idle";
    public const string Interrupted = "interrupted";
    public const string MaxIterations = "max_iterations";
    public const string SubagentTimeout = "subagent_timeout";
    public const string SubagentIdle = "subagent_idle";
    public const string TokenRotation = "token_rotation_failed";

    /// <summary>An operational stop: a service the turn needs was unavailable.</summary>
    public const string Unavailable = "unavailable";
}

/// <summary>Data event kinds defined by the protocol.</summary>
public static class DataKinds
{
    /// <summary>(v1.5) The data event kind whose payload is a <see cref="ProgressPayload"/>.</summary>
    public const string Progress = "progress";
}

/// <summary>
/// Tool run statuses. Execution errors are model-visible data ("error" status), not protocol
/// failures.
/// </summary>
public static class ToolStatuses
{
    public const string Success = "success";
    public const string Error = "error";
}

/// <summary>Error detail codes (SPEC §9), carried as <c>apiStatusCode</c> in the error envelope.</summary>
public static class Codes
{
    public const int InvalidBody = 4001;
    public const int MissingField = 4002;
    public const int Forbidden = 4031;
    public const int UnknownSession = 4041;
    public const int UnknownRun = 4042;
    public const int Busy = 4291;
    public const int Upstream = 5001;
    public const int Internal = 5002;
}
