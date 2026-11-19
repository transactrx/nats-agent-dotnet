using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Tests.Support;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// The service layer under the agent and tool host: what the Go library gets from
/// transactrx/nats-service. These pin the conventions Go peers and nats-discover rely on —
/// the status header and error envelope, gzip of large bodies, chunked transfer in both
/// directions, routing, and the discovery documents.
/// </summary>
public sealed class ServiceLayerTests(NatsServerFixture nats) : NatsTestBase(nats)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private async Task<(NatsService Service, NatsServiceClient Client, NatsConnection Raw)> StartAsync(string basePath, params EndpointRegistration[] endpoints)
    {
        var server = await ConnectAsync();
        var service = Track(new NatsService(server, basePath, "q." + basePath, "test-instance", NullLogger.Instance)
        {
            Description = "service layer test",
            RepositoryUrl = "https://example.invalid/repo",
        });
        service.AddEndpoints(endpoints);
        await service.StartAsync(CancellationToken.None);
        await server.PingAsync();
        var raw = await ConnectAsync();
        return (service, new NatsServiceClient(raw, NullLogger.Instance), raw);
    }

    private static EndpointRegistration Echo(string path = "echo") => new()
    {
        Path = path,
        Description = "Returns the request body.",
        Handler = (request, _) => Task.FromResult(request.Body),
    };

    // Text that gzip cannot shrink much, so size thresholds are really crossed on the wire.
    private static byte[] Incompressible(int length)
    {
        var bytes = new byte[length];
        new Random(42).NextBytes(bytes);
        return Encoding.ASCII.GetBytes(Convert.ToBase64String(bytes))[..length];
    }

    [Fact]
    public async Task SmallRequestRoundTripsWithStatusAndMessageId()
    {
        var (_, _, raw) = await StartAsync("test.svc.small", Echo());

        var reply = await raw.RequestAsync<byte[], byte[]>(
            "test.svc.small.echo", "hi"u8.ToArray(), new NatsHeaders { ["_Message_Id"] = "m-1" },
            replyOpts: new NatsSubOpts { Timeout = Timeout });

        Assert.Equal("hi", Encoding.UTF8.GetString(reply.Data!));
        Assert.Equal("200", reply.Headers!["status"].ToString());
        Assert.Equal("m-1", reply.Headers["_Message_Id"].ToString());
        Assert.False(reply.Headers.ContainsKey("_Compression"));
    }

    [Fact]
    public async Task UnknownEndpointRepliesWith404Envelope()
    {
        var (_, client, raw) = await StartAsync("test.svc.notfound", Echo());

        var reply = await raw.RequestAsync<byte[], byte[]>("test.svc.notfound.nope", [], replyOpts: new NatsSubOpts { Timeout = Timeout });
        Assert.Equal("404", reply.Headers!["status"].ToString());
        var error = JsonSerializer.Deserialize<NatsServiceError>(reply.Data!)!;
        Assert.Equal((404, 404, "subject: test.svc.notfound.nope not found"), (error.Status, error.ApiStatusCode, error.ErrorMessage));

        // The client turns the envelope into an exception.
        var ex = await Assert.ThrowsAsync<NatsServiceException>(() => client.RequestAsync("test.svc.notfound.nope", null, [], Timeout, CancellationToken.None));
        Assert.Equal(404, ex.Status);

        // An endpoint name followed by extra tokens is not that endpoint.
        ex = await Assert.ThrowsAsync<NatsServiceException>(() => client.RequestAsync("test.svc.notfound.echo.extra", null, [], Timeout, CancellationToken.None));
        Assert.Equal(404, ex.Status);
    }

    [Fact]
    public async Task HandlerErrorsBecomeEnvelopes()
    {
        var (_, client, _) = await StartAsync(
            "test.svc.errors",
            new EndpointRegistration { Path = "denied", Handler = (_, _) => throw NatsServiceException.Authorization("DENIED_FN", Codes.Forbidden) },
            new EndpointRegistration { Path = "boom", Handler = (_, _) => throw new InvalidOperationException("kaput") });

        var denied = await Assert.ThrowsAsync<NatsServiceException>(() => client.RequestAsync("test.svc.errors.denied", null, [], Timeout, CancellationToken.None));
        Assert.Equal((403, Codes.Forbidden, "DENIED_FN"), (denied.Status, denied.ApiStatusCode, denied.ErrorMessage));

        // Any other exception is a 500 with the internal code; the service keeps serving.
        var boom = await Assert.ThrowsAsync<NatsServiceException>(() => client.RequestAsync("test.svc.errors.boom", null, [], Timeout, CancellationToken.None));
        Assert.Equal((500, Codes.Internal), (boom.Status, boom.ApiStatusCode));
        Assert.Equal("kaput", boom.Error.InternalError);
    }

    [Fact]
    public async Task LargeReplyIsCompressed()
    {
        var body = Encoding.UTF8.GetBytes(new string('a', 50_000));
        var (_, client, raw) = await StartAsync("test.svc.gzip", new EndpointRegistration { Path = "big", Handler = (_, _) => Task.FromResult(body) });

        // On the wire: gzip, flagged by the header a Go client looks for.
        var reply = await raw.RequestAsync<byte[], byte[]>("test.svc.gzip.big", [], replyOpts: new NatsSubOpts { Timeout = Timeout });
        Assert.Equal("gzip", reply.Headers!["_Compression"].ToString());
        Assert.True(reply.Data!.Length < 2_000, $"expected a compressed body, got {reply.Data.Length} bytes");

        Assert.Equal(body, await client.RequestAsync("test.svc.gzip.big", null, [], Timeout, CancellationToken.None));
    }

    [Fact]
    public async Task LargeRequestIsCompressedAndChunked()
    {
        var (_, client, raw) = await StartAsync("test.svc.chunkreq", Echo());

        // Observe the request as it travels: above 8KB after compression it must carry the
        // chunk headers and an empty body.
        await using var spy = await raw.SubscribeCoreAsync<byte[]>("test.svc.chunkreq.echo");
        await raw.PingAsync();

        var body = Incompressible(200_000);
        var echoed = await client.RequestAsync("test.svc.chunkreq.echo", null, body, Timeout, CancellationToken.None);
        Assert.Equal(body, echoed);

        using var cts = new CancellationTokenSource(Timeout);
        var seen = await spy.Msgs.ReadAsync(cts.Token);
        Assert.Equal("gzip", seen.Headers!["_Compression"].ToString());
        Assert.NotEmpty(seen.Headers["_Chunked_Subject"].ToString());
        Assert.True(int.Parse(seen.Headers["_Chunked_Length"].ToString()) > 1);
        Assert.NotEmpty(seen.Headers["_Chunks_Id"].ToString());
        Assert.True(seen.Data is null or { Length: 0 });
    }

    [Fact]
    public async Task LargeReplyIsChunked()
    {
        // Larger than the 300KB reply threshold even after compression, and larger than one
        // NATS message may be (1MB by default).
        var body = Incompressible(1_500_000);
        var (_, client, raw) = await StartAsync("test.svc.chunkresp", new EndpointRegistration { Path = "huge", Handler = (_, _) => Task.FromResult(body) });

        var reply = await raw.RequestAsync<byte[], byte[]>("test.svc.chunkresp.huge", [], replyOpts: new NatsSubOpts { Timeout = Timeout });
        Assert.Equal("200", reply.Headers!["status"].ToString());
        Assert.StartsWith("test.svc.chunkresp_.chunked_", reply.Headers["_Chunked_Subject"].ToString());
        Assert.True(int.Parse(reply.Headers["_Chunked_Length"].ToString()) > 1);
        Assert.True(reply.Data is null or { Length: 0 });

        Assert.Equal(body, await client.RequestAsync("test.svc.chunkresp.huge", null, [], Timeout, CancellationToken.None));
    }

    [Fact]
    public async Task ChunkResponderRejectsBadRequests()
    {
        var body = Incompressible(700_000);
        var (_, _, raw) = await StartAsync("test.svc.chunkbad", new EndpointRegistration { Path = "huge", Handler = (_, _) => Task.FromResult(body) });
        var reply = await raw.RequestAsync<byte[], byte[]>("test.svc.chunkbad.huge", [], replyOpts: new NatsSubOpts { Timeout = Timeout });
        var subject = reply.Headers!["_Chunked_Subject"].ToString();
        var id = reply.Headers["_Chunks_Id"].ToString();
        var count = int.Parse(reply.Headers["_Chunked_Length"].ToString());

        async Task<string> Status(NatsHeaders headers) =>
            (await raw.RequestAsync<byte[], byte[]>(subject, [], headers, replyOpts: new NatsSubOpts { Timeout = Timeout })).Headers!["status"].ToString();

        Assert.Equal("200", await Status(new NatsHeaders { ["_Chunks_Id"] = id, ["_Chunk_Index"] = "0" }));
        Assert.Equal("400", await Status(new NatsHeaders { ["_Chunks_Id"] = id, ["_Chunk_Index"] = count.ToString() })); // one past the end
        Assert.Equal("400", await Status(new NatsHeaders { ["_Chunks_Id"] = id, ["_Chunk_Index"] = "-1" }));
        Assert.Equal("400", await Status(new NatsHeaders { ["_Chunks_Id"] = "unknown", ["_Chunk_Index"] = "0" }));
        Assert.Equal("400", await Status(new NatsHeaders { ["_Chunk_Index"] = "0" }));
    }

    [Fact]
    public async Task PathParametersAndWildcards()
    {
        var (_, client, _) = await StartAsync(
            "test.svc.routes",
            new EndpointRegistration
            {
                Path = "user.:id.profile",
                Parameters = [new ParameterDoc { Name = "id", Example = "42" }],
                Handler = (request, _) => Task.FromResult(Encoding.UTF8.GetBytes("user=" + request.Parameters["id"])),
            },
            new EndpointRegistration { Path = "files.>", Handler = (request, _) => Task.FromResult(Encoding.UTF8.GetBytes("file:" + request.Subject)) },
            new EndpointRegistration { Path = "any.*", Handler = (_, _) => Task.FromResult("any"u8.ToArray()) });

        async Task<string> Call(string suffix) =>
            Encoding.UTF8.GetString(await client.RequestAsync("test.svc.routes." + suffix, null, [], Timeout, CancellationToken.None));

        Assert.Equal("user=7", await Call("user.7.profile"));
        Assert.Equal("file:test.svc.routes.files.a.b.c", await Call("files.a.b.c"));
        Assert.Equal("any", await Call("any.thing"));
        await Assert.ThrowsAsync<NatsServiceException>(() => Call("any.thing.more"));
        await Assert.ThrowsAsync<NatsServiceException>(() => Call("files"));
    }

    [Fact]
    public async Task DuplicateAndReservedEndpointsAreRefused()
    {
        var server = await ConnectAsync();
        var service = Track(new NatsService(server, "test.svc.dup", "q", "i", NullLogger.Instance));
        service.AddEndpoints([Echo()]);
        Assert.Throws<ArgumentException>(() => service.AddEndpoints([Echo()]));
        Assert.Throws<ArgumentException>(() => service.AddEndpoints([Echo("_api_docs")]));
        Assert.Throws<ArgumentException>(() => service.AddEndpoints([Echo(" ")]));

        var empty = Track(new NatsService(server, "test.svc.empty", "q", "i", NullLogger.Instance));
        await Assert.ThrowsAsync<InvalidOperationException>(() => empty.StartAsync(CancellationToken.None));
    }

    /// <summary>What nats-discover reads: the discovery reply, the API docs and per-instance stats.</summary>
    [Fact]
    public async Task DiscoveryDocsAndStats()
    {
        var (_, client, raw) = await StartAsync(
            "test.svc.docs",
            Echo(),
            new EndpointRegistration { Path = "user.:id", Parameters = [new ParameterDoc { Name = "id", Example = "42" }], Handler = (_, _) => Task.FromResult<byte[]>([]) },
            new EndpointRegistration { Path = "fail", Handler = (_, _) => throw NatsServiceException.Validation("bad", Codes.MissingField) });

        // _discovery.all: every service answers with its basic info.
        var inbox = raw.NewInbox();
        await using var sub = await raw.SubscribeCoreAsync<byte[]>(inbox);
        await raw.PublishAsync("_discovery.all", Array.Empty<byte>(), replyTo: inbox);
        JsonElement info = default;
        using (var cts = new CancellationTokenSource(Timeout))
        {
            await foreach (var msg in sub.Msgs.ReadAllAsync(cts.Token))
            {
                var doc = JsonDocument.Parse(msg.Data!).RootElement;
                if (doc.GetProperty("serviceName").GetString() == "test.svc.docs")
                {
                    info = doc.Clone();
                    break;
                }
            }
        }

        Assert.Equal("test.svc.docs", info.GetProperty("subjectPrefix").GetString());
        Assert.Equal("service layer test", info.GetProperty("description").GetString());
        Assert.Equal("https://example.invalid/repo", info.GetProperty("repositoryUrl").GetString());
        Assert.Equal("test.svc.docs._api_docs", info.GetProperty("apiDocsSubject").GetString());

        // <base>._api_docs: the documented endpoints, without the internal one.
        var docs = JsonDocument.Parse(await client.RequestAsync("test.svc.docs._api_docs", null, [], Timeout, CancellationToken.None)).RootElement;
        var endpoints = docs.GetProperty("endpoints").EnumerateArray().ToList();
        Assert.Equal(["echo", "user.:id", "fail"], endpoints.Select(e => e.GetProperty("path").GetString()));
        Assert.Equal("test.svc.docs.echo", endpoints[0].GetProperty("fullSubject").GetString());
        Assert.Equal("Returns the request body.", endpoints[0].GetProperty("description").GetString());
        Assert.Equal("test.svc.docs.user.42", endpoints[1].GetProperty("exampleSubject").GetString());
        Assert.Equal(5, docs.GetProperty("statusCodes").GetArrayLength());

        // <base>._stats: one reply per instance, counting successes and failures per endpoint.
        await client.RequestAsync("test.svc.docs.echo", null, "x"u8.ToArray(), Timeout, CancellationToken.None);
        await client.RequestAsync("test.svc.docs.echo", null, "y"u8.ToArray(), Timeout, CancellationToken.None);
        await Assert.ThrowsAsync<NatsServiceException>(() => client.RequestAsync("test.svc.docs.fail", null, [], Timeout, CancellationToken.None));

        JsonElement stats = default;
        await EventuallyAsync(
            () =>
            {
                var reply = raw.RequestAsync<byte[], byte[]>("test.svc.docs._stats", [], replyOpts: new NatsSubOpts { Timeout = Timeout }).AsTask().GetAwaiter().GetResult();
                stats = JsonDocument.Parse(reply.Data!).RootElement.Clone();
                return stats.GetProperty("endpoints").GetArrayLength() >= 3; // echo, fail, _api_docs
            },
            "stats never listed the called endpoints");
        Assert.Equal("test-instance", stats.GetProperty("instanceId").GetString());
        var echo = stats.GetProperty("endpoints").EnumerateArray().Single(e => e.GetProperty("subject").GetString() == "test.svc.docs.echo");
        Assert.Equal(2, echo.GetProperty("success").GetInt64());
        Assert.Equal(2, echo.GetProperty("latency").GetProperty("count").GetInt64());
        var fail = stats.GetProperty("endpoints").EnumerateArray().Single(e => e.GetProperty("subject").GetString() == "test.svc.docs.fail");
        Assert.Equal((1, 1), (fail.GetProperty("failures").GetInt64(), fail.GetProperty("failures4xx").GetInt64()));
    }

    [Fact]
    public async Task StopAcceptingLetsInFlightRequestsFinish()
    {
        var entered = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var (service, client, _) = await StartAsync("test.svc.drain", new EndpointRegistration
        {
            Path = "slow",
            Handler = async (_, _) =>
            {
                entered.TrySetResult();
                await release.Task;
                return "done"u8.ToArray();
            },
        });

        var inFlight = client.RequestAsync("test.svc.drain.slow", null, [], Timeout, CancellationToken.None);
        await entered.Task.WaitAsync(Timeout);
        await service.StopAcceptingAsync();
        await service.StopAcceptingAsync(); // safe to call twice

        // New requests find nobody; the one in flight is still answered.
        await Assert.ThrowsAnyAsync<Exception>(() => client.RequestAsync("test.svc.drain.slow", null, [], TimeSpan.FromMilliseconds(300), CancellationToken.None));
        release.SetResult();
        Assert.Equal("done", Encoding.UTF8.GetString(await inFlight));
    }

    [Fact]
    public async Task RequestTimeoutAndNoResponders()
    {
        var (_, client, _) = await StartAsync("test.svc.timeout", new EndpointRegistration
        {
            Path = "never",
            Handler = async (_, ct) =>
            {
                await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, ct);
                return [];
            },
        });

        await Assert.ThrowsAsync<TimeoutException>(() => client.RequestAsync("test.svc.timeout.never", null, [], TimeSpan.FromMilliseconds(200), CancellationToken.None));
        await Assert.ThrowsAsync<NatsNoRespondersException>(() => client.RequestAsync("test.svc.nobody.home", null, [], Timeout, CancellationToken.None));
    }

    [Fact]
    public void GzipAndChunkHelpersRoundTrip()
    {
        var data = Incompressible(10_000);
        Assert.Equal(data, Framing.GUnzip(Framing.GZip(data)));
        Assert.Empty(Framing.GUnzip(Framing.GZip([])));

        var chunks = Framing.Chunk(data, 3_000);
        Assert.Equal([3_000, 3_000, 3_000, 1_000], chunks.Select(c => c.Length));
        Assert.Equal(data, chunks.SelectMany(c => c).ToArray());
        Assert.Empty(Framing.Chunk([], 10));
    }
}
