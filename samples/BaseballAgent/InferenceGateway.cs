using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using NATS.Client.Core;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace BaseballAgent;

/// <summary>A tool offered to the model: what it is for and the JSON Schema of its input.</summary>
internal sealed record ToolSpec(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("description")] string Description,
    [property: JsonPropertyName("inputSchema")] JsonElement InputSchema);

/// <summary>
/// One reply from the model. <see cref="StopReason"/> is <c>tool_use</c> when the model wants
/// tools run (the calls are the toolUse blocks in <see cref="Content"/>), otherwise
/// <c>end_turn</c> or <c>max_tokens</c>.
/// </summary>
internal sealed record ModelReply(IReadOnlyList<ContentBlock> Content, string StopReason, Usage? Usage);

/// <summary>The gateway could not produce a reply. <see cref="Retryable"/> failures are worth one more try.</summary>
internal sealed class InferenceException(string message, bool retryable, Exception? inner = null) : Exception(message, inner)
{
    public bool Retryable { get; } = retryable;
}

/// <summary>
/// Streaming client for the org's inference gateway (<c>trx.inferenceGateway</c>), the one
/// path to every AI model: no vendor SDK and no model version in this code. The conversation
/// types (<see cref="Message"/>, <see cref="ContentBlock"/>) are the agent protocol's own;
/// the gateway uses the same shapes.
/// </summary>
/// <param name="connection">The agent's NATS connection.</param>
/// <param name="model">
/// <c>lab/family</c> (e.g. <c>anthropic/sonnet</c>: the gateway picks the newest release) or a
/// concrete model id.
/// </param>
/// <param name="maxTokens">Upper limit on the length of one reply.</param>
internal sealed class InferenceGateway(NatsConnection connection, string model, int maxTokens)
{
    private const string InvokeStreamSubject = "trx.inferenceGateway.invokeStream";

    // The gateway acknowledges before the model starts, so this covers only the round trip.
    private static readonly TimeSpan AckTimeout = TimeSpan.FromSeconds(30);

    // A healthy stream always ends with done or error. This long without an event means the
    // gateway or the subscription died mid-reply.
    private static readonly TimeSpan SilenceLimit = TimeSpan.FromSeconds(120);

    private readonly NatsServiceClient _service = new(connection);

    /// <summary>
    /// Sends the conversation and returns the model's reply, calling <paramref name="onText"/>
    /// with each piece of text as it arrives.
    /// </summary>
    /// <exception cref="InferenceException">The gateway rejected the request or the stream failed.</exception>
    public async Task<ModelReply> StreamAsync(
        string system,
        IReadOnlyList<Message> messages,
        IReadOnlyList<ToolSpec> tools,
        Func<string, ValueTask> onText,
        CancellationToken cancellationToken)
    {
        // Subscribe before asking: events can start arriving as soon as the gateway replies.
        var inbox = connection.NewInbox();
        await using var events = await connection.SubscribeCoreAsync(
            inbox, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: cancellationToken);

        var slash = model.IndexOf('/');
        var request = new InvokeStreamRequest
        {
            Lab = slash > 0 ? model[..slash] : null,
            Family = slash > 0 ? model[(slash + 1)..] : null,
            ModelId = slash > 0 ? null : model,
            System = system,
            Messages = messages,
            MaxTokens = maxTokens,
            Tools = tools.Count > 0 ? tools : null,
            StreamSubject = inbox,
        };

        try
        {
            var ack = WireJson.Deserialize<InvokeStreamAck>(
                await _service.RequestAsync(InvokeStreamSubject, null, WireJson.Serialize(request), AckTimeout, cancellationToken));
            if (ack is not { Accepted: true })
            {
                throw new InferenceException("the inference gateway did not accept the request", retryable: false);
            }
        }
        catch (NatsServiceException ex)
        {
            // 4xx: something about this request is wrong, so asking again cannot help.
            throw new InferenceException("inference gateway: " + ex.ErrorMessage, retryable: ex.Status >= 500, ex);
        }
        catch (Exception ex) when (ex is TimeoutException or NatsException)
        {
            throw new InferenceException("inference gateway unreachable: " + ex.Message, retryable: true, ex);
        }

        return await ReadReplyAsync(events, onText, cancellationToken);
    }

    private static async Task<ModelReply> ReadReplyAsync(
        INatsSub<byte[]> events, Func<string, ValueTask> onText, CancellationToken cancellationToken)
    {
        // The model's reply arrives as numbered content blocks, each either text or one tool
        // call whose JSON input comes in fragments.
        var blocks = new SortedDictionary<int, BlockBuilder>();
        var stopReason = "";
        Usage? usage = null;
        var expectedSeq = 0;

        using var silence = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        while (true)
        {
            silence.CancelAfter(SilenceLimit);
            NatsMsg<byte[]> msg;
            try
            {
                msg = await events.Msgs.ReadAsync(silence.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new InferenceException($"the model stream went silent for {SilenceLimit.TotalSeconds:0}s", retryable: true);
            }

            if (!WireJson.TryDeserialize<StreamEvent>(msg.Data, out var ev))
            {
                throw new InferenceException("unreadable event in the model stream", retryable: true);
            }

            // seq counts up by one from 0, so a gap means an event was lost.
            if (ev.Seq != expectedSeq++)
            {
                throw new InferenceException("events were lost from the model stream", retryable: true);
            }

            switch (ev.Type)
            {
                case "toolUseStart":
                    blocks[ev.ContentIndex] = new BlockBuilder { ToolUseId = ev.ToolUseId, ToolName = ev.ToolName };
                    break;

                case "delta":
                    if (!blocks.TryGetValue(ev.ContentIndex, out var block))
                    {
                        blocks[ev.ContentIndex] = block = new BlockBuilder();
                    }

                    if (!string.IsNullOrEmpty(ev.ToolInputDelta))
                    {
                        block.ToolInput.Append(ev.ToolInputDelta);
                    }

                    if (!string.IsNullOrEmpty(ev.Text))
                    {
                        block.Text.Append(ev.Text);
                        await onText(ev.Text);
                    }

                    break;

                case "messageStop":
                    stopReason = ev.StopReason ?? "";
                    break;

                case "metadata":
                    usage = ev.Usage ?? usage;
                    break;

                case "error":
                    throw new InferenceException("the model stream failed: " + ev.Error, retryable: true);

                case "done":
                    return new ModelReply([.. blocks.Values.Select(b => b.Build()).OfType<ContentBlock>()], stopReason, usage);

                default:
                    break; // messageStart, contentBlockStop, and anything added later
            }
        }
    }

    private sealed class BlockBuilder
    {
        public string? ToolUseId { get; init; }

        public string? ToolName { get; init; }

        public StringBuilder Text { get; } = new();

        public StringBuilder ToolInput { get; } = new();

        public ContentBlock? Build()
        {
            if (ToolUseId is null)
            {
                return Text.Length > 0 ? new ContentBlock { Text = Text.ToString() } : null;
            }

            var input = ToolInput.ToString().Trim();
            try
            {
                return new ContentBlock
                {
                    ToolUse = new ToolUseBlock { ToolUseId = ToolUseId, Name = ToolName ?? "", Input = WireJson.Parse(input.Length > 0 ? input : "{}") },
                };
            }
            catch (JsonException ex)
            {
                // Usually a reply cut off at maxTokens in the middle of a tool call.
                throw new InferenceException($"the model sent incomplete input for tool {ToolName}", retryable: false, ex);
            }
        }
    }

    // ── Wire shapes: github.com/transactrx/inferenceGateway/pkg/models ─────

    private sealed record InvokeStreamRequest
    {
        [JsonPropertyName("lab")]
        public string? Lab { get; init; }

        [JsonPropertyName("family")]
        public string? Family { get; init; }

        [JsonPropertyName("modelId")]
        public string? ModelId { get; init; }

        [JsonPropertyName("system")]
        public string? System { get; init; }

        [JsonPropertyName("messages")]
        public required IReadOnlyList<Message> Messages { get; init; }

        [JsonPropertyName("maxTokens")]
        public int? MaxTokens { get; init; }

        [JsonPropertyName("tools")]
        public IReadOnlyList<ToolSpec>? Tools { get; init; }

        [JsonPropertyName("streamSubject")]
        public required string StreamSubject { get; init; }
    }

    private sealed record InvokeStreamAck
    {
        [JsonPropertyName("accepted")]
        public bool Accepted { get; init; }
    }

    private sealed record StreamEvent
    {
        [JsonPropertyName("seq")]
        public int Seq { get; init; }

        [JsonPropertyName("type")]
        public string Type { get; init; } = "";

        [JsonPropertyName("contentIndex")]
        public int ContentIndex { get; init; }

        [JsonPropertyName("text")]
        public string? Text { get; init; }

        [JsonPropertyName("toolUseId")]
        public string? ToolUseId { get; init; }

        [JsonPropertyName("toolName")]
        public string? ToolName { get; init; }

        [JsonPropertyName("toolInputDelta")]
        public string? ToolInputDelta { get; init; }

        [JsonPropertyName("stopReason")]
        public string? StopReason { get; init; }

        [JsonPropertyName("usage")]
        public Usage? Usage { get; init; }

        [JsonPropertyName("error")]
        public string? Error { get; init; }
    }
}
