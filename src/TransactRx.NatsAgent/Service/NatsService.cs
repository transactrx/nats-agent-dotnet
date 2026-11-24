using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Service;

/// <summary>
/// One service on the wire: a subject space <c>&lt;basePath&gt;.&gt;</c> served in a queue
/// group, with the conventions of the Go <c>transactrx/nats-service</c> library so that Go
/// clients and <c>nats-discover</c> treat it like any other org microservice — the error
/// envelope and status header, gzip compression, chunked transfer of large bodies,
/// <c>_discovery.all</c>, <c>&lt;base&gt;._api_docs</c> and <c>&lt;base&gt;._stats</c>.
/// </summary>
internal sealed class NatsService : IAsyncDisposable
{
    private const string DiscoverySubject = "_discovery.all";
    private const string ApiDocsSuffix = "_api_docs";
    private const string StatsSuffix = "_stats";
    private static readonly TimeSpan ChunkCacheTtl = TimeSpan.FromMinutes(3);

    private readonly NatsConnection _nc;
    private readonly string _basePath;
    private readonly string _queue;
    private readonly ILogger _logger;
    private readonly int _maxRespSizeToCompress;
    private readonly int _maxRespSizeToChunk;
    private readonly List<Endpoint> _endpoints = [];
    private readonly ConcurrentDictionary<string, EndpointStats> _stats = new();
    private readonly ConcurrentDictionary<string, (byte[][] Chunks, DateTimeOffset Expires)> _chunkCache = new();
    private readonly CancellationTokenSource _disposed = new();
    private readonly DateTimeOffset _startTime = DateTimeOffset.UtcNow;

    private INatsSub<byte[]>? _mainSub;
    private INatsSub<byte[]>? _chunkSub;
    private INatsSub<byte[]>? _discoverySub;
    private INatsSub<byte[]>? _statsSub;
    private string _chunkSubject = "";
    private int _accepting;

    public NatsService(
        NatsConnection nc, string basePath, string queue, string instanceId, ILogger logger,
        int maxRespSizeToCompress = Framing.DefaultMaxSizeBeforeCompress,
        int maxRespSizeToChunk = Framing.DefaultMaxResponseSizeBeforeChunk)
    {
        _nc = nc;
        _basePath = basePath;
        _queue = queue;
        InstanceId = instanceId;
        _logger = logger;
        _maxRespSizeToCompress = maxRespSizeToCompress;
        _maxRespSizeToChunk = maxRespSizeToChunk;
    }

    public string InstanceId { get; }

    public string? Description { get; set; }

    public string? RepositoryUrl { get; set; }

    /// <summary>A fresh instance id: <c>&lt;hostname&gt;-&lt;8 hex chars&gt;</c>.</summary>
    public static string NewInstanceId() => Environment.MachineName + "-" + Guid.NewGuid().ToString("D")[..8];

    public void AddEndpoints(IEnumerable<EndpointRegistration> registrations)
    {
        foreach (var reg in registrations)
        {
            AddEndpoint(reg, isInternal: false);
        }
    }

    private void AddEndpoint(EndpointRegistration reg, bool isInternal)
    {
        var path = reg.Path.Trim();
        if (path.Length == 0)
        {
            throw new ArgumentException("endpoint path cannot be empty");
        }

        if (!isInternal && path.EndsWith(ApiDocsSuffix, StringComparison.Ordinal))
        {
            throw new ArgumentException($"endpoint path '{path}' uses reserved suffix '{ApiDocsSuffix}'");
        }

        if (_endpoints.Any(e => e.Registration.Path == path))
        {
            throw new ArgumentException($"failed to register endpoint '{path}': endpoint already in use");
        }

        _endpoints.Add(new Endpoint(reg with { Path = path }, path.Split('.'), isInternal));
    }

    public async Task StartAsync(CancellationToken ct)
    {
        if (_endpoints.Count == 0)
        {
            throw new InvalidOperationException("no endpoints configured");
        }

        AddEndpoint(
            new EndpointRegistration { Path = ApiDocsSuffix, Handler = (_, _) => Task.FromResult(WireJson.Serialize(BuildApiDocs())) },
            isInternal: true);

        _mainSub = await _nc.SubscribeCoreAsync(_basePath + ".>", _queue, Framing.Raw, cancellationToken: ct).ConfigureAwait(false);
        Interlocked.Exchange(ref _accepting, 1);
        _ = Background.Pump(_mainSub, _logger, Dispatch);

        // Replies too large for one message are fetched from here, chunk by chunk.
        _chunkSubject = _basePath + "_.chunked_" + Guid.NewGuid().ToString("D");
        _chunkSub = await _nc.SubscribeCoreAsync(_chunkSubject, serializer: Framing.Raw, cancellationToken: ct).ConfigureAwait(false);
        _ = Background.Pump(_chunkSub, _logger, HandleChunkRequest);
        _ = Background.Run(CleanChunkCacheLoop);

        // Discovery and stats are best effort: a service without permission for them still serves.
        try
        {
            _discoverySub = await _nc.SubscribeCoreAsync(DiscoverySubject, serializer: Framing.Raw, cancellationToken: ct).ConfigureAwait(false);
            _ = Background.Pump(_discoverySub, _logger, HandleDiscovery);
            _statsSub = await _nc.SubscribeCoreAsync(_basePath + "." + StatsSuffix, serializer: Framing.Raw, cancellationToken: ct).ConfigureAwait(false);
            _ = Background.Pump(_statsSub, _logger, HandleStats);
        }
        catch (NatsException ex)
        {
            _logger.LogWarning("WARNING: Unable to subscribe to discovery/stats subjects: {Error}. Service will operate normally.", ex.Message);
        }
    }

    /// <summary>
    /// Stops taking new requests (the queue group routes them to other instances) and leaves
    /// discovery. Requests already dispatched finish and are answered. Safe to call twice.
    /// </summary>
    public async Task StopAcceptingAsync()
    {
        if (Interlocked.Exchange(ref _accepting, 0) == 0)
        {
            return;
        }

        await Drain(_discoverySub).ConfigureAwait(false);
        await Drain(_mainSub).ConfigureAwait(false);

        static async Task Drain(INatsSub<byte[]>? sub)
        {
            if (sub is null)
            {
                return;
            }

            try
            {
                await sub.DrainAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                await Background.Close(sub).ConfigureAwait(false);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed.IsCancellationRequested)
        {
            return;
        }

        await StopAcceptingAsync().ConfigureAwait(false);
        _disposed.Cancel();
        await Background.Close(_mainSub).ConfigureAwait(false);
        await Background.Close(_discoverySub).ConfigureAwait(false);
        await Background.Close(_statsSub).ConfigureAwait(false);
        await Background.Close(_chunkSub).ConfigureAwait(false);
        _chunkCache.Clear();
    }

    // ─── routing ──────────────────────────────────────────────────────────

    private ValueTask Dispatch(NatsMsg<byte[]> msg)
    {
        if (msg.Subject.Length <= _basePath.Length + 1)
        {
            return ValueTask.CompletedTask;
        }

        var tokens = msg.Subject[(_basePath.Length + 1)..].Split('.');

        // Answered by the broadcast stats subscription, not by the queue-balanced one.
        if (tokens.Length == 1 && tokens[0] == StatsSuffix && _statsSub is not null)
        {
            return ValueTask.CompletedTask;
        }

        foreach (var endpoint in _endpoints)
        {
            if (endpoint.TryMatch(tokens, out var parameters))
            {
                _ = Background.Run(() => HandleCall(endpoint, msg, parameters));
                return ValueTask.CompletedTask;
            }
        }

        return ReplyError(msg, NatsServiceException.NotFound(msg.Subject).Error, messageId: null);
    }

    private async Task HandleCall(Endpoint endpoint, NatsMsg<byte[]> msg, IReadOnlyDictionary<string, string> parameters)
    {
        var started = Stopwatch.GetTimestamp();
        var messageId = Framing.Header(msg.Headers, Framing.MessageId);
        var userId = messageId.Length > 0 ? Framing.Header(msg.Headers, Framing.UserId) : "";
        if (messageId.Length == 0)
        {
            messageId = Guid.NewGuid().ToString("D");
        }

        var logger = new PrefixLogger(_logger, messageId + " - ");
        try
        {
            byte[] body;
            try
            {
                body = await ReadBody(msg, messageId).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await ReplyError(msg, NatsServiceException.Validation("error parsing request", 400, ex).Error, messageId: null).ConfigureAwait(false);
                return;
            }

            var request = new ServiceRequest
            {
                Body = body,
                Headers = msg.Headers,
                Subject = msg.Subject,
                MessageId = messageId,
                UserId = userId,
                Parameters = parameters,
                Logger = logger,
            };

            int status;
            try
            {
                var responseBody = await endpoint.Registration.Handler(request, _disposed.Token).ConfigureAwait(false);
                if (endpoint.Registration.Silent)
                {
                    return;
                }

                await ReplySuccess(msg, request, responseBody ?? []).ConfigureAwait(false);
                status = 200;
            }
            catch (NatsServiceException ex)
            {
                await ReplyError(msg, ex.Error, messageId).ConfigureAwait(false);
                status = ex.Status;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "unhandled error in endpoint {Path}", endpoint.Registration.Path);
                await ReplyError(msg, NatsServiceException.Server("internal error", Codes.Internal, ex).Error, messageId).ConfigureAwait(false);
                status = 500;
            }

            var micros = (long)Stopwatch.GetElapsedTime(started).TotalMicroseconds;
            _stats.GetOrAdd(endpoint.Registration.Path, p => new EndpointStats(_basePath + "." + p)).Add(micros, status);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "error returning response on {Subject}", msg.Subject);
        }
    }

    /// <summary>The request body, reassembled from chunks and decompressed as its headers say.</summary>
    private async Task<byte[]> ReadBody(NatsMsg<byte[]> msg, string messageId)
    {
        var data = msg.Data ?? [];
        var chunkSubject = Framing.Header(msg.Headers, Framing.ChunkedSubject);
        if (chunkSubject.Length > 0)
        {
            data = await Framing.DownloadChunksAsync(
                _nc, chunkSubject,
                Framing.Header(msg.Headers, Framing.ChunksId),
                Framing.Header(msg.Headers, Framing.ChunkedLength),
                messageId, userId: null, _disposed.Token).ConfigureAwait(false);
        }

        if (Framing.Header(msg.Headers, Framing.CompressionHeader) == Framing.Gzip)
        {
            data = Framing.GUnzip(data);
        }

        return data;
    }

    private async ValueTask ReplySuccess(NatsMsg<byte[]> msg, ServiceRequest request, byte[] body)
    {
        if (string.IsNullOrEmpty(msg.ReplyTo))
        {
            return;
        }

        var headers = request.ResponseHeaders ?? new NatsHeaders();
        if (body.Length > _maxRespSizeToCompress)
        {
            headers[Framing.CompressionHeader] = Framing.Gzip;
            body = Framing.GZip(body);
        }

        // Handlers may set a more specific success status (201, 202, 204...).
        if (Framing.Header(headers, Framing.Status).Length == 0)
        {
            headers[Framing.Status] = "200";
        }

        headers[Framing.MessageId] = request.MessageId;

        if (body.Length >= _maxRespSizeToChunk)
        {
            var chunks = Framing.Chunk(body, _maxRespSizeToChunk);
            var chunksId = Guid.NewGuid().ToString("D");
            _chunkCache[chunksId] = (chunks, DateTimeOffset.UtcNow + ChunkCacheTtl);
            headers[Framing.ChunkedSubject] = _chunkSubject;
            headers[Framing.ChunkedLength] = chunks.Length.ToString(CultureInfo.InvariantCulture);
            headers[Framing.ChunksId] = chunksId;
            body = [];
        }

        await msg.ReplyAsync(body, headers, serializer: Framing.Raw).ConfigureAwait(false);
    }

    private static ValueTask ReplyError(NatsMsg<byte[]> msg, NatsServiceError error, string? messageId)
    {
        if (string.IsNullOrEmpty(msg.ReplyTo))
        {
            return ValueTask.CompletedTask;
        }

        var headers = new NatsHeaders { [Framing.Status] = error.Status.ToString(CultureInfo.InvariantCulture) };
        if (messageId is not null)
        {
            headers[Framing.MessageId] = messageId;
        }

        return msg.ReplyAsync(WireJson.Serialize(error), headers, serializer: Framing.Raw);
    }

    // ─── chunked replies ──────────────────────────────────────────────────

    private ValueTask HandleChunkRequest(NatsMsg<byte[]> msg)
    {
        var chunksId = Framing.Header(msg.Headers, Framing.ChunksId);
        if (chunksId.Length == 0)
        {
            return Framing.ReplyChunkErrorAsync(msg, "missing header: " + Framing.ChunksId);
        }

        if (!int.TryParse(Framing.Header(msg.Headers, Framing.ChunkIndex), out var index))
        {
            return Framing.ReplyChunkErrorAsync(msg, "missing header: " + Framing.ChunkIndex);
        }

        if (!_chunkCache.TryGetValue(chunksId, out var entry) || entry.Expires < DateTimeOffset.UtcNow)
        {
            return Framing.ReplyChunkErrorAsync(msg, $"chunksId {chunksId} is not in memory");
        }

        if (index < 0 || index >= entry.Chunks.Length)
        {
            return Framing.ReplyChunkErrorAsync(msg, $"chunkIndex {index} is out of range");
        }

        return string.IsNullOrEmpty(msg.ReplyTo)
            ? ValueTask.CompletedTask
            : msg.ReplyAsync(entry.Chunks[index], new NatsHeaders { [Framing.Status] = "200" }, serializer: Framing.Raw);
    }

    private async Task CleanChunkCacheLoop()
    {
        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(60));
            while (await timer.WaitForNextTickAsync(_disposed.Token).ConfigureAwait(false))
            {
                var now = DateTimeOffset.UtcNow;
                foreach (var (id, entry) in _chunkCache)
                {
                    if (entry.Expires < now)
                    {
                        _chunkCache.TryRemove(id, out _);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // disposed
        }
    }

    // ─── nats-discover ────────────────────────────────────────────────────

    private ValueTask HandleDiscovery(NatsMsg<byte[]> msg)
    {
        if (string.IsNullOrEmpty(msg.ReplyTo))
        {
            return ValueTask.CompletedTask;
        }

        var info = new ServiceInfo
        {
            ServiceName = _basePath,
            SubjectPrefix = _basePath,
            Description = NullIfEmpty(Description),
            RepositoryUrl = NullIfEmpty(RepositoryUrl),
            ApiDocsSubject = _basePath + "." + ApiDocsSuffix,
        };
        return msg.ReplyAsync(WireJson.Serialize(info), serializer: Framing.Raw);
    }

    private ValueTask HandleStats(NatsMsg<byte[]> msg)
    {
        if (string.IsNullOrEmpty(msg.ReplyTo))
        {
            return ValueTask.CompletedTask;
        }

        var response = new InstanceStatsResponse
        {
            ServiceName = _basePath,
            InstanceId = InstanceId,
            SubjectPrefix = _basePath,
            Uptime = GoDuration.Format(DateTimeOffset.UtcNow - _startTime),
            Endpoints = _stats.Values.Select(s => s.Snapshot()).ToList(),
        };
        return msg.ReplyAsync(WireJson.Serialize(response), serializer: Framing.Raw);
    }

    private ApiDocsResponse BuildApiDocs()
    {
        var docs = new List<EndpointDoc>();
        foreach (var endpoint in _endpoints.Where(e => !e.IsInternal))
        {
            var reg = endpoint.Registration;
            var fullSubject = _basePath + "." + reg.Path;
            var userDocs = (reg.Parameters ?? []).ToDictionary(p => p.Name);
            var parameters = endpoint.Tokens
                .Where(t => t.StartsWith(':'))
                .Select(t => t[1..])
                .Select(name => userDocs.TryGetValue(name, out var doc) ? doc : new ParameterDoc { Name = name })
                .ToList();

            string? example = null;
            if (parameters.Count > 0)
            {
                example = fullSubject;
                foreach (var p in parameters)
                {
                    example = example.Replace(":" + p.Name, string.IsNullOrEmpty(p.Example) ? "{" + p.Name + "}" : p.Example, StringComparison.Ordinal);
                }
            }

            docs.Add(new EndpointDoc
            {
                Path = reg.Path,
                FullSubject = fullSubject,
                ExampleSubject = example,
                Parameters = parameters.Count > 0 ? parameters : null,
                Headers = reg.Headers is { Count: > 0 } ? reg.Headers : null,
                Response = reg.Response,
                Description = NullIfEmpty(reg.Description),
                WildcardType = reg.Path.EndsWith('>') ? "multi" : reg.Path.Contains('*') ? "single" : null,
            });
        }

        return new ApiDocsResponse
        {
            ServiceName = _basePath,
            SubjectPrefix = _basePath,
            Description = NullIfEmpty(Description),
            RepositoryUrl = NullIfEmpty(RepositoryUrl),
            StatusCodes =
            [
                new(200, "Success"),
                new(400, "Validation error"),
                new(403, "Authorization error"),
                new(404, "Endpoint not found"),
                new(500, "Server error"),
            ],
            Endpoints = docs,
        };
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;

    private sealed record Endpoint(EndpointRegistration Registration, string[] Tokens, bool IsInternal)
    {
        private static readonly IReadOnlyDictionary<string, string> NoParameters = new Dictionary<string, string>();

        public bool TryMatch(string[] subject, out IReadOnlyDictionary<string, string> parameters)
        {
            parameters = NoParameters;
            Dictionary<string, string>? captured = null;
            for (var i = 0; i < Tokens.Length; i++)
            {
                var token = Tokens[i];
                if (token == ">" && i == Tokens.Length - 1)
                {
                    if (subject.Length <= i)
                    {
                        return false;
                    }

                    parameters = captured ?? parameters;
                    return true;
                }

                if (i >= subject.Length)
                {
                    return false;
                }

                if (token == "*")
                {
                    continue;
                }

                if (token.Length > 1 && token[0] == ':')
                {
                    (captured ??= [])[token[1..]] = subject[i];
                    continue;
                }

                if (token != subject[i])
                {
                    return false;
                }
            }

            if (subject.Length != Tokens.Length)
            {
                return false;
            }

            parameters = captured ?? parameters;
            return true;
        }
    }
}
