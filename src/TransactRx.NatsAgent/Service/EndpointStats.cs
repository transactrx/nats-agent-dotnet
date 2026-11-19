using System.Text.Json.Serialization;
using TransactRx.NatsAgent.Internal;

namespace TransactRx.NatsAgent.Service;

/// <summary>Per-endpoint counters and latency percentiles served on <c>&lt;base&gt;._stats</c>.</summary>
internal sealed class EndpointStats(string subject)
{
    private const int BufferSize = 10_000;

    private readonly object _gate = new();
    private readonly long[] _latencies = new long[BufferSize];
    private readonly DateTimeOffset _start = DateTimeOffset.UtcNow;
    private long _success;
    private long _failures;
    private long _failures4xx;
    private long _failures5xx;
    private int _index;
    private long _count;
    private long _sum;
    private long _min = -1;
    private long _max;

    public void Add(long latencyMicros, int statusCode)
    {
        lock (_gate)
        {
            if (statusCode is >= 400 and < 500)
            {
                _failures++;
                _failures4xx++;
            }
            else if (statusCode is >= 500 and < 600)
            {
                _failures++;
                _failures5xx++;
            }
            else
            {
                _success++;
            }

            _latencies[_index] = latencyMicros;
            _index = (_index + 1) % BufferSize;
            _count++;
            _sum += latencyMicros;
            if (_min < 0 || latencyMicros < _min)
            {
                _min = latencyMicros;
            }

            if (latencyMicros > _max)
            {
                _max = latencyMicros;
            }
        }
    }

    public EndpointStatsSnapshot Snapshot()
    {
        lock (_gate)
        {
            var latency = new LatencyStats { Count = _count };
            if (_count > 0)
            {
                var samples = (int)Math.Min(_count, BufferSize);
                var sorted = _latencies.AsSpan(0, samples).ToArray();
                Array.Sort(sorted);
                latency = new LatencyStats
                {
                    Count = _count,
                    Samples = samples,
                    Min = _min / 1000.0,
                    Max = _max / 1000.0,
                    Avg = (double)_sum / _count / 1000.0,
                    P50 = Percentile(sorted, 50),
                    P65 = Percentile(sorted, 65),
                    P75 = Percentile(sorted, 75),
                    P85 = Percentile(sorted, 85),
                    P95 = Percentile(sorted, 95),
                };
            }

            return new EndpointStatsSnapshot
            {
                Subject = subject,
                Success = _success,
                Failures = _failures,
                Failures4xx = _failures4xx,
                Failures5xx = _failures5xx,
                Latency = latency,
                StartTime = _start,
                Uptime = GoDuration.Format(DateTimeOffset.UtcNow - _start),
            };
        }
    }

    // Nearest-rank, as the Go library computes it.
    private static double Percentile(long[] sorted, int p)
    {
        if (sorted.Length == 0)
        {
            return 0;
        }

        var index = Math.Min(p * sorted.Length / 100, sorted.Length - 1);
        return sorted[index] / 1000.0;
    }
}

internal sealed record LatencyStats
{
    [JsonPropertyName("count")]
    public long Count { get; init; }

    [JsonPropertyName("minMs")]
    public double Min { get; init; }

    [JsonPropertyName("maxMs")]
    public double Max { get; init; }

    [JsonPropertyName("avgMs")]
    public double Avg { get; init; }

    [JsonPropertyName("p50Ms")]
    public double P50 { get; init; }

    [JsonPropertyName("p65Ms")]
    public double P65 { get; init; }

    [JsonPropertyName("p75Ms")]
    public double P75 { get; init; }

    [JsonPropertyName("p85Ms")]
    public double P85 { get; init; }

    [JsonPropertyName("p95Ms")]
    public double P95 { get; init; }

    [JsonPropertyName("samplesInWindow")]
    public int Samples { get; init; }
}

internal sealed record EndpointStatsSnapshot
{
    [JsonPropertyName("subject")]
    public string Subject { get; init; } = "";

    [JsonPropertyName("success")]
    public long Success { get; init; }

    [JsonPropertyName("failures")]
    public long Failures { get; init; }

    [JsonPropertyName("failures4xx")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public long Failures4xx { get; init; }

    [JsonPropertyName("failures5xx")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public long Failures5xx { get; init; }

    [JsonPropertyName("latency")]
    public LatencyStats Latency { get; init; } = new();

    [JsonPropertyName("startTime")]
    public DateTimeOffset StartTime { get; init; }

    [JsonPropertyName("uptime")]
    public string Uptime { get; init; } = "";
}

// ─── nats-discover documents ──────────────────────────────────────────────

internal sealed record ServiceInfo
{
    [JsonPropertyName("serviceName")]
    public string ServiceName { get; init; } = "";

    [JsonPropertyName("subjectPrefix")]
    public string SubjectPrefix { get; init; } = "";

    [JsonPropertyName("description")]
    public string? Description { get; init; }

    [JsonPropertyName("repositoryUrl")]
    public string? RepositoryUrl { get; init; }

    [JsonPropertyName("apiDocsSubject")]
    public string? ApiDocsSubject { get; init; }
}

internal sealed record ApiDocsResponse
{
    [JsonPropertyName("serviceName")]
    public string ServiceName { get; init; } = "";

    [JsonPropertyName("subjectPrefix")]
    public string SubjectPrefix { get; init; } = "";

    [JsonPropertyName("description")]
    public string? Description { get; init; }

    [JsonPropertyName("repositoryUrl")]
    public string? RepositoryUrl { get; init; }

    [JsonPropertyName("statusCodes")]
    public IReadOnlyList<StatusCodeDoc>? StatusCodes { get; init; }

    [JsonPropertyName("endpoints")]
    public IReadOnlyList<EndpointDoc> Endpoints { get; init; } = [];
}

internal sealed record StatusCodeDoc(
    [property: JsonPropertyName("code")] int Code,
    [property: JsonPropertyName("description")] string Description);

internal sealed record EndpointDoc
{
    [JsonPropertyName("path")]
    public string Path { get; init; } = "";

    [JsonPropertyName("fullSubject")]
    public string FullSubject { get; init; } = "";

    [JsonPropertyName("exampleSubject")]
    public string? ExampleSubject { get; init; }

    [JsonPropertyName("parameters")]
    public IReadOnlyList<ParameterDoc>? Parameters { get; init; }

    [JsonPropertyName("headers")]
    public IReadOnlyList<HeaderDoc>? Headers { get; init; }

    [JsonPropertyName("response")]
    public ResponseDoc? Response { get; init; }

    [JsonPropertyName("description")]
    public string? Description { get; init; }

    /// <summary>"single" (*) or "multi" (&gt;).</summary>
    [JsonPropertyName("wildcardType")]
    public string? WildcardType { get; init; }
}

internal sealed record InstanceStatsResponse
{
    [JsonPropertyName("serviceName")]
    public string ServiceName { get; init; } = "";

    [JsonPropertyName("instanceId")]
    public string InstanceId { get; init; } = "";

    [JsonPropertyName("subjectPrefix")]
    public string SubjectPrefix { get; init; } = "";

    [JsonPropertyName("uptime")]
    public string Uptime { get; init; } = "";

    [JsonPropertyName("endpoints")]
    public IReadOnlyList<EndpointStatsSnapshot> Endpoints { get; init; } = [];
}
