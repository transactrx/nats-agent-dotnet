using System.Text.Json;
using Microsoft.Extensions.Logging;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent;

/// <summary>
/// Emits protocol events for one run. All methods are safe for concurrent use; events after
/// the terminal event are dropped.
/// </summary>
public sealed class AgentStream
{
    // Heartbeat cadence: the spec requires an event at least every 15s while a run is live;
    // we check twice as often and ping after 10s of silence.
    internal static readonly TimeSpan HeartbeatCheck = TimeSpan.FromSeconds(5);
    internal static readonly TimeSpan HeartbeatIdle = TimeSpan.FromSeconds(10);

    /// <summary>The minimum gap between two progress frames of one run.</summary>
    internal static readonly TimeSpan ProgressEvery = TimeSpan.FromSeconds(15);

    private readonly Func<string, byte[], ValueTask> _publish;
    private readonly string _subject;
    private readonly string _runId;
    private readonly ILogger _logger;
    private readonly bool _progress; // AgentConfig.Progress: ProgressAsync is a no-op when false
    private readonly TimeProvider _time;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly CancellationTokenSource _stopHeartbeat = new();
    private readonly object _progressGate = new();

    private int _seq;
    private volatile bool _terminal;
    private long _lastPublish;
    private long _lastProgress;
    private bool _progressSent;

    internal AgentStream(Func<string, byte[], ValueTask> publish, string subject, string runId, ILogger logger, bool progress, TimeProvider time)
    {
        _publish = publish;
        _subject = subject;
        _runId = runId;
        _logger = logger;
        _progress = progress;
        _time = time;
        _lastPublish = time.GetTimestamp();
    }

    /// <summary>Whether the terminal event (done or error) has been emitted.</summary>
    public bool IsTerminated => _terminal;

    /// <summary>Emits the mandatory first event (seq 0) and begins heartbeats.</summary>
    internal async ValueTask StartAsync(string sessionId)
    {
        await PublishAsync(new AgentEvent { Type = EventTypes.Start, SessionId = sessionId }).ConfigureAwait(false);
        _ = Internal.Background.Run(HeartbeatLoop);
    }

    /// <summary>Emits an incremental piece of assistant text.</summary>
    public ValueTask TextAsync(string? delta) =>
        string.IsNullOrEmpty(delta) ? ValueTask.CompletedTask : PublishAsync(new AgentEvent { Type = EventTypes.Text, TextDelta = delta });

    /// <summary>
    /// Reports that the agent is executing a tool. <paramref name="input"/> must be
    /// JSON-serializable (an object, a dictionary, a <see cref="JsonElement"/>...).
    /// </summary>
    public ValueTask ToolUseAsync(string toolUseId, string toolName, object? input)
    {
        JsonElement raw;
        try
        {
            raw = input is JsonElement element ? element : JsonSerializer.SerializeToElement(input, WireJson.Options);
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            _logger.LogWarning("run {RunId}: cannot marshal input of tool {Tool}: {Error}", _runId, toolName, ex.Message);
            raw = WireJson.EmptyObject;
        }

        return PublishAsync(new AgentEvent { Type = EventTypes.ToolUse, ToolUseId = toolUseId, ToolName = toolName, ToolInput = raw });
    }

    /// <summary>Reports a finished tool call. Leave <paramref name="toolError"/> null on success.</summary>
    public ValueTask ToolResultAsync(string toolUseId, string toolName, string? result, string? toolError = null) =>
        PublishAsync(new AgentEvent
        {
            Type = EventTypes.ToolResult,
            ToolUseId = toolUseId,
            ToolName = toolName,
            ToolResult = NullIfEmpty(result),
            ToolError = NullIfEmpty(toolError),
        });

    /// <summary>Emits an agent-specific structured event. Clients ignore unknown kinds.</summary>
    public ValueTask DataAsync(string kind, object? payload)
    {
        JsonElement raw;
        try
        {
            raw = payload is JsonElement element ? element : JsonSerializer.SerializeToElement(payload, WireJson.Options);
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            _logger.LogWarning("run {RunId}: cannot marshal \"{Kind}\" data payload: {Error}", _runId, kind, ex.Message);
            return ValueTask.CompletedTask;
        }

        return PublishAsync(new AgentEvent { Type = EventTypes.Data, Kind = kind, Payload = raw });
    }

    /// <summary>
    /// Tells the caller the run is still working (data event of kind "progress"). At most one
    /// frame per 15s per run; extra calls are dropped. No-op unless the agent opted in with
    /// <see cref="AgentConfig.Progress"/>.
    /// </summary>
    public ValueTask ProgressAsync(string stage, string? detail = null)
    {
        if (!_progress)
        {
            return ValueTask.CompletedTask;
        }

        lock (_progressGate)
        {
            if (_progressSent && _time.GetElapsedTime(_lastProgress) < ProgressEvery)
            {
                return ValueTask.CompletedTask;
            }

            _progressSent = true;
            _lastProgress = _time.GetTimestamp();
        }

        return DataAsync(DataKinds.Progress, new ProgressPayload { Stage = stage, Detail = NullIfEmpty(detail) });
    }

    /// <summary>Emits human-readable progress text.</summary>
    public ValueTask StatusAsync(string text) => PublishAsync(new AgentEvent { Type = EventTypes.Status, StatusText = text });

    /// <summary>Terminates the run successfully.</summary>
    public ValueTask DoneAsync(string stopReason, Usage? usage = null) =>
        PublishAsync(new AgentEvent { Type = EventTypes.Done, StopReason = stopReason, Usage = usage });

    /// <summary>
    /// Terminates the run successfully and says which limit ended it (<see cref="Reasons"/>).
    /// </summary>
    public ValueTask DoneWithReasonAsync(string stopReason, string reason, Usage? usage = null) =>
        PublishAsync(new AgentEvent { Type = EventTypes.Done, StopReason = stopReason, Usage = usage, Reason = NullIfEmpty(reason) });

    /// <summary>Terminates the run with a failure the client should surface.</summary>
    public ValueTask ErrorAsync(string message, int code) =>
        PublishAsync(new AgentEvent { Type = EventTypes.Error, Error = message, ErrorStatus = code });

    /// <summary>
    /// Terminates the run with a failure and says which limit ended it (<see cref="Reasons"/>).
    /// </summary>
    public ValueTask ErrorWithReasonAsync(string message, int code, string reason) =>
        PublishAsync(new AgentEvent { Type = EventTypes.Error, Error = message, ErrorStatus = code, Reason = NullIfEmpty(reason) });

    private async ValueTask PublishAsync(AgentEvent ev)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_terminal)
            {
                if (ev.Type != EventTypes.Ping)
                {
                    _logger.LogInformation("run {RunId}: dropping {Type} event after terminal", _runId, ev.Type);
                }

                return;
            }

            ev = ev with { RunId = _runId, Seq = _seq++ };
            try
            {
                await _publish(_subject, WireJson.Serialize(ev)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("run {RunId}: publish to {Subject} failed: {Error}", _runId, _subject, ex.Message);
            }

            _lastPublish = _time.GetTimestamp();
            if (ev.IsTerminal)
            {
                _terminal = true;
                _stopHeartbeat.Cancel();
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task HeartbeatLoop()
    {
        try
        {
            using var timer = new PeriodicTimer(HeartbeatCheck, _time);
            while (await timer.WaitForNextTickAsync(_stopHeartbeat.Token).ConfigureAwait(false))
            {
                if (_time.GetElapsedTime(Volatile.Read(ref _lastPublish)) >= HeartbeatIdle)
                {
                    await PublishAsync(new AgentEvent { Type = EventTypes.Ping }).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // run ended
        }
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;
}
