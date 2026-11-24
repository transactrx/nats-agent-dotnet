using System.Text;
using System.Text.Json;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Client;

/// <summary>Base of the renew failures.</summary>
public abstract class RenewException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>The run (or agent) offers no renew subject.</summary>
public sealed class RenewUnsupportedException() : RenewException("run does not support renew");

/// <summary>
/// The agent answered and refused the token; <see cref="Reason"/> is the agent's reason.
/// Transport failures (no responders, timeout) are <see cref="RenewTransportException"/> so
/// callers can retry.
/// </summary>
public sealed class RenewRejectedException(string reason) : RenewException("renew rejected: " + reason)
{
    public string Reason { get; } = reason;
}

/// <summary>
/// The renew got no well-formed reply (no responders, timeout, connection error, or only
/// stray replies such as another instance's error envelope). The outcome is unknown (on a
/// timeout the agent may have applied the token and the reply was lost); retrying with the
/// same token is safe.
/// </summary>
public sealed class RenewTransportException(string message, Exception? inner = null)
    : RenewException("renew transport error: " + message, inner);

public sealed partial class AgentClient
{
    /// <summary>Test seam: <see cref="RenewTimeout"/> for runs started by this client.</summary>
    internal TimeSpan RenewTimeoutOverride { get; set; } = RenewTimeout;

    /// <summary>
    /// Reports whether <paramref name="subject"/> is a renew subject of
    /// <paramref name="agent"/>: exactly <c>trx.agent.&lt;name&gt;.renew.&lt;token&gt;</c>,
    /// token non-empty base64url characters (no wildcard, whitespace or further dots).
    /// <paramref name="agent"/> may be the base name or a regional alias
    /// <c>&lt;name&gt;_&lt;region&gt;</c>; the subject is built from the base name either
    /// way. Send a token only to a subject that passes.
    /// </summary>
    public static bool IsValidRenewSubject(string? agent, string? subject)
    {
        if (string.IsNullOrEmpty(agent) || string.IsNullOrEmpty(subject))
        {
            return false;
        }

        var names = new List<string> { agent };
        var underscore = agent.LastIndexOf('_');
        if (underscore > 0)
        {
            names.Add(agent[..underscore]); // regional alias → base name
        }

        foreach (var name in names)
        {
            var prefix = Protocol.AgentSubject(name, "renew.");
            if (subject.StartsWith(prefix, StringComparison.Ordinal) && RenewTokenRegex().IsMatch(subject[prefix.Length..]))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Sends <paramref name="request"/> to a run's renew subject
    /// (<c>ChatAck.Caps.RenewSubject</c>), for callers that hold the subject without a
    /// <see cref="ChatRun"/>.
    /// <para>
    /// The subject lies in the agent's subject space, which every instance of the agent also
    /// queue-subscribes. An instance without the silent renew endpoint (an older build)
    /// answers it with a framework error envelope, possibly before the run's own answer. Such
    /// replies (error status header, or a body without a boolean "applied") are skipped,
    /// never read as a rejection; if no well-formed reply follows, the outcome is unknown.
    /// A renew after the run ended gets no reply, so it also surfaces as a transport error.
    /// </para>
    /// </summary>
    /// <exception cref="RenewUnsupportedException">Empty subject.</exception>
    /// <exception cref="RenewRejectedException">An explicit <c>{"applied":false}</c>.</exception>
    /// <exception cref="RenewTransportException">No well-formed reply within <see cref="RenewTimeout"/>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static Task RenewAsync(NatsConnection connection, string subject, RenewRequest request, CancellationToken cancellationToken = default) =>
        RenewAsync(connection, subject, request, RenewTimeout, cancellationToken);

    internal static async Task RenewAsync(NatsConnection nc, string subject, RenewRequest request, TimeSpan timeout, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(subject))
        {
            throw new RenewUnsupportedException();
        }

        var data = WireJson.Serialize(request);
        INatsSub<byte[]> sub;
        var inbox = nc.NewInbox();
        try
        {
            sub = await nc.SubscribeCoreAsync(inbox, serializer: Framing.Raw, cancellationToken: ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Any failure to send (a closed connection throws ObjectDisposedException, not a
            // NatsException) is a transport error; only the caller's cancellation is not.
            throw Transport(ex.Message, ex);
        }

        try
        {
            try
            {
                await nc.PublishAsync(subject, data, replyTo: inbox, serializer: Framing.Raw, cancellationToken: ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw Transport(ex.Message, ex);
            }

            var stray = new List<string>();
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            deadline.CancelAfter(timeout);
            while (true)
            {
                NatsMsg<byte[]> msg;
                try
                {
                    msg = await sub.Msgs.ReadAsync(deadline.Token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is OperationCanceledException or System.Threading.Channels.ChannelClosedException)
                {
                    ct.ThrowIfCancellationRequested();
                    throw stray.Count > 0
                        ? Transport($"no well-formed reply (skipped {string.Join("; ", stray)}): timeout", ex)
                        : Transport("timeout", ex);
                }

                if (msg.HasNoResponders || (msg.Data is not { Length: > 0 } && msg.Headers?.Code == 503))
                {
                    throw Transport("no responders available", new NatsNoRespondersException());
                }

                if (!TryDecodeRenewResponse(msg, out var response))
                {
                    stray.Add(DescribeReply(msg));
                    continue;
                }

                if (!response.Applied)
                {
                    throw new RenewRejectedException(response.Reason ?? "");
                }

                return;
            }
        }
        finally
        {
            await Background.Close(sub).ConfigureAwait(false);
        }

        RenewTransportException Transport(string cause, Exception? inner = null) => new($"renew request to {subject}: {cause}", inner);
    }

    /// <summary>A reply's status: NATS's own (no responders) or the service envelope's; "" when none.</summary>
    internal static string ReplyStatus(NatsMsg<byte[]> msg)
    {
        if (msg.Headers is { Code: > 0 } headers)
        {
            return headers.Code.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        return Framing.Header(msg.Headers, Framing.Status);
    }

    /// <summary>
    /// Accepts only a well-formed RenewResponse: no error status header and a JSON object
    /// with a boolean "applied".
    /// </summary>
    internal static bool TryDecodeRenewResponse(NatsMsg<byte[]> msg, out RenewResponse response)
    {
        response = new RenewResponse();
        var status = ReplyStatus(msg);
        if (status.Length > 0 && status != "200")
        {
            return false;
        }

        try
        {
            using var doc = JsonDocument.Parse(msg.Data ?? []);
            if (doc.RootElement.ValueKind != JsonValueKind.Object
                || !doc.RootElement.TryGetProperty("applied", out var applied)
                || applied.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
            {
                return false;
            }

            response = doc.RootElement.Deserialize<RenewResponse>(WireJson.Options) ?? response;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>Summarizes a skipped reply for the error message.</summary>
    internal static string DescribeReply(NatsMsg<byte[]> msg)
    {
        var body = msg.Data ?? [];
        var text = "\"" + Encoding.UTF8.GetString(body, 0, Math.Min(body.Length, 120)) + "\"";
        var status = ReplyStatus(msg);
        return status.Length > 0 ? $"status {status} {text}" : text;
    }
}
