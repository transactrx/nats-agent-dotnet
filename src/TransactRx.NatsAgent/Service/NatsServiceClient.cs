using System.Globalization;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;

namespace TransactRx.NatsAgent.Service;

/// <summary>
/// The calling side of the service conventions (the Go <c>nats-service-client</c>): one
/// request/reply with correlation headers, gzip of large bodies, chunked transfer both ways,
/// and the error envelope turned into <see cref="NatsServiceException"/>.
/// </summary>
internal sealed class NatsServiceClient(
    NatsConnection nc,
    ILogger logger,
    int maxSizeBeforeCompress = Framing.DefaultMaxSizeBeforeCompress,
    int maxSizeBeforeChunk = Framing.DefaultMaxRequestSizeBeforeChunk)
{
    public NatsConnection Connection => nc;

    /// <summary>
    /// Sends <paramref name="data"/> and returns the reply body.
    /// </summary>
    /// <exception cref="NatsServiceException">The service answered with the error envelope.</exception>
    /// <exception cref="TimeoutException">No reply within <paramref name="timeout"/>.</exception>
    /// <exception cref="NatsNoRespondersException">Nothing is subscribed to the subject.</exception>
    public async Task<byte[]> RequestAsync(string subject, NatsHeaders? extraHeaders, byte[] data, TimeSpan timeout, CancellationToken ct)
    {
        var messageId = Guid.NewGuid().ToString("D");
        var headers = new NatsHeaders { [Framing.MessageId] = messageId };
        var userId = nc.Opts.AuthOpts.Username;
        if (!string.IsNullOrEmpty(userId))
        {
            headers[Framing.UserId] = userId;
        }

        if (data.Length > maxSizeBeforeCompress)
        {
            data = Framing.GZip(data);
            headers[Framing.CompressionHeader] = Framing.Gzip;
        }

        if (extraHeaders is not null)
        {
            foreach (var (key, values) in extraHeaders)
            {
                headers[key] = values;
            }
        }

        // A body too large for one message is served chunk by chunk from a temporary subject
        // for as long as the request is in flight.
        INatsSub<byte[]>? chunkSub = null;
        try
        {
            if (data.Length > maxSizeBeforeChunk)
            {
                var chunks = Framing.Chunk(data, maxSizeBeforeChunk);
                var chunkSubject = nc.NewInbox();
                chunkSub = await nc.SubscribeCoreAsync(chunkSubject, serializer: Framing.Raw, cancellationToken: ct).ConfigureAwait(false);
                _ = Background.Pump(chunkSub, logger, msg => ServeChunk(msg, chunks));
                headers[Framing.ChunkedSubject] = chunkSubject;
                headers[Framing.ChunkedLength] = chunks.Length.ToString(CultureInfo.InvariantCulture);
                headers[Framing.ChunksId] = Guid.NewGuid().ToString("D");
                data = [];
            }

            NatsMsg<byte[]> reply;
            try
            {
                reply = await nc.RequestAsync<byte[], byte[]>(
                    subject, data, headers, Framing.Raw, Framing.Raw,
                    replyOpts: new NatsSubOpts { Timeout = timeout },
                    cancellationToken: ct).ConfigureAwait(false);
            }
            catch (NatsNoReplyException ex)
            {
                throw new TimeoutException($"request to {subject} timed out after {GoDuration.FormatPrecise(timeout)}", ex);
            }

            if (Framing.Header(reply.Headers, Framing.Status) != "200")
            {
                var error = Framing.TryParseError(reply.Data)
                    ?? throw new InvalidDataException($"invalid response from service on {subject}");
                throw new NatsServiceException(error);
            }

            var body = reply.Data ?? [];
            var replyChunks = Framing.Header(reply.Headers, Framing.ChunkedSubject);
            if (replyChunks.Length > 0)
            {
                body = await Framing.DownloadChunksAsync(
                    nc, replyChunks,
                    Framing.Header(reply.Headers, Framing.ChunksId),
                    Framing.Header(reply.Headers, Framing.ChunkedLength),
                    Framing.Header(reply.Headers, Framing.MessageId),
                    userId, ct).ConfigureAwait(false);
            }

            if (Framing.Header(reply.Headers, Framing.CompressionHeader) == Framing.Gzip)
            {
                body = Framing.GUnzip(body);
            }

            return body;
        }
        finally
        {
            await Background.Close(chunkSub).ConfigureAwait(false);
        }
    }

    private static ValueTask ServeChunk(NatsMsg<byte[]> msg, byte[][] chunks)
    {
        var indexText = Framing.Header(msg.Headers, Framing.ChunkIndex);
        if (!int.TryParse(indexText, out var index) || index < 0 || index >= chunks.Length)
        {
            return Framing.ReplyChunkErrorAsync(msg, "invalid chunk index: " + indexText);
        }

        return string.IsNullOrEmpty(msg.ReplyTo)
            ? ValueTask.CompletedTask
            : msg.ReplyAsync(chunks[index], new NatsHeaders { [Framing.Status] = "200" }, serializer: Framing.Raw);
    }
}
