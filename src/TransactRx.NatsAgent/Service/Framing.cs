using System.IO.Compression;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Service;

/// <summary>
/// The transport conventions shared with the Go <c>nats-service</c> library: status and
/// correlation headers, gzip compression of large bodies, and chunked transfer of bodies too
/// large for one NATS message.
/// </summary>
internal static class Framing
{
    public const string CompressionHeader = "_Compression";
    public const string Gzip = "gzip";
    public const string Status = "status";
    public const string ChunksId = "_Chunks_Id";
    public const string ChunkedLength = "_Chunked_Length";
    public const string ChunkedSubject = "_Chunked_Subject";
    public const string ChunkIndex = "_Chunk_Index";
    public const string MessageId = "_Message_Id";
    public const string UserId = "_User_Id";

    /// <summary>Bodies above this size are gzip-compressed (both directions).</summary>
    public const int DefaultMaxSizeBeforeCompress = 1024 * 2;

    /// <summary>Requests above this size (after compression) are sent in chunks.</summary>
    public const int DefaultMaxRequestSizeBeforeChunk = 1024 * 8;

    /// <summary>Replies of this size or more (after compression) are sent in chunks.</summary>
    public const int DefaultMaxResponseSizeBeforeChunk = 1024 * 300;

    public static readonly TimeSpan ChunkRequestTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Upper bound on the chunk count a peer may announce.</summary>
    public const int MaxChunks = 65536;

    public static readonly NatsRawSerializer<byte[]> Raw = NatsRawSerializer<byte[]>.Default;

    public static string Header(NatsHeaders? headers, string name)
    {
        if (headers is null || !headers.TryGetValue(name, out var values) || values.Count == 0)
        {
            return "";
        }

        return values[0] ?? "";
    }

    public static byte[] GZip(byte[] data)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            gzip.Write(data, 0, data.Length);
        }

        return output.ToArray();
    }

    public static byte[] GUnzip(byte[] data)
    {
        using var input = new MemoryStream(data);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        return output.ToArray();
    }

    public static byte[][] Chunk(byte[] data, int chunkSize)
    {
        var count = (data.Length + chunkSize - 1) / chunkSize;
        var chunks = new byte[count][];
        for (var i = 0; i < count; i++)
        {
            var start = i * chunkSize;
            chunks[i] = data.AsSpan(start, Math.Min(chunkSize, data.Length - start)).ToArray();
        }

        return chunks;
    }

    /// <summary>
    /// Fetches the chunks a peer announced (<c>_Chunked_Subject</c>, <c>_Chunks_Id</c>,
    /// <c>_Chunked_Length</c>) one request at a time and returns them joined.
    /// </summary>
    public static async Task<byte[]> DownloadChunksAsync(
        INatsConnection nc, string subject, string chunksId, string count, string messageId, string? userId, CancellationToken ct)
    {
        if (!int.TryParse(count, out var chunkCount) || chunkCount < 0 || chunkCount > MaxChunks)
        {
            throw new InvalidDataException($"invalid chunk count: {count}");
        }

        using var buffer = new MemoryStream();
        for (var i = 0; i < chunkCount; i++)
        {
            var headers = new NatsHeaders
            {
                [ChunksId] = chunksId,
                [ChunkIndex] = i.ToString(System.Globalization.CultureInfo.InvariantCulture),
                [MessageId] = messageId,
            };
            if (!string.IsNullOrEmpty(userId))
            {
                headers[UserId] = userId;
            }

            NatsMsg<byte[]> reply;
            try
            {
                reply = await nc.RequestAsync<byte[], byte[]>(
                    subject, [], headers, Raw, Raw,
                    replyOpts: new NatsSubOpts { Timeout = ChunkRequestTimeout },
                    cancellationToken: ct).ConfigureAwait(false);
            }
            catch (NatsNoReplyException ex)
            {
                throw new TimeoutException($"timed out downloading chunk {i}", ex);
            }

            if (Header(reply.Headers, Status) != "200")
            {
                var detail = reply.Data is { Length: > 0 } d ? System.Text.Encoding.UTF8.GetString(d) : "no body";
                throw new InvalidDataException($"unable to retrieve chunk {i}: {detail}");
            }

            if (reply.Data is { } data)
            {
                buffer.Write(data, 0, data.Length);
            }
        }

        return buffer.ToArray();
    }

    /// <summary>Replies to a chunk request with the error envelope and a 400 status header.</summary>
    public static ValueTask ReplyChunkErrorAsync(NatsMsg<byte[]> request, string message)
    {
        if (string.IsNullOrEmpty(request.ReplyTo))
        {
            return ValueTask.CompletedTask;
        }

        var body = WireJson.Serialize(new NatsServiceError { Status = 400, ErrorMessage = message, ApiStatusCode = 400 });
        return request.ReplyAsync(body, new NatsHeaders { [Status] = "400" }, serializer: Raw);
    }

    /// <summary>Parses a reply body as the error envelope; null when it is not one.</summary>
    public static NatsServiceError? TryParseError(byte[]? body)
    {
        if (body is null || body.Length == 0)
        {
            return null;
        }

        try
        {
            return JsonSerializer.Deserialize<NatsServiceError>(body, WireJson.Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public static void LogDebug(ILogger logger, bool debug, string message)
    {
        if (debug)
        {
            logger.LogInformation("{Message}", message);
        }
    }
}
