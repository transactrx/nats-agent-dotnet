using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace TransactRx.NatsAgent.Service;

/// <summary>
/// One request as an endpoint handler sees it (the Go <c>nats_service.NatsMessage</c>): the
/// body is already reassembled and decompressed.
/// </summary>
public sealed class ServiceRequest
{
    public required byte[] Body { get; init; }

    public NatsHeaders? Headers { get; init; }

    /// <summary>The full subject the request arrived on.</summary>
    public required string Subject { get; init; }

    /// <summary>Correlation id: the caller's <c>_Message_Id</c> header, else generated.</summary>
    public required string MessageId { get; init; }

    /// <summary>The caller's <c>_User_Id</c> header (transport level, unauthenticated).</summary>
    public string UserId { get; init; } = "";

    /// <summary>Values of the <c>:name</c> tokens of the endpoint path.</summary>
    public IReadOnlyDictionary<string, string> Parameters { get; init; } = new Dictionary<string, string>();

    /// <summary>Logger prefixed with the message id.</summary>
    public required ILogger Logger { get; init; }

    /// <summary>Optional headers to send with a successful reply.</summary>
    public NatsHeaders? ResponseHeaders { get; set; }

    /// <summary>The first value of a request header, or "" when absent.</summary>
    public string Header(string name)
    {
        if (Headers is null || !Headers.TryGetValue(name, out var values) || values.Count == 0)
        {
            return "";
        }

        return values[0] ?? "";
    }
}

/// <summary>
/// Handles one request and returns the reply body. Throw <see cref="NatsServiceException"/>
/// to reply with the error envelope; any other exception becomes a 500.
/// </summary>
public delegate Task<byte[]> EndpointHandler(ServiceRequest request, CancellationToken cancellationToken);

/// <summary>
/// An endpoint in a service's subject space, with the documentation shown by
/// <c>nats-discover</c>. <see cref="Path"/> is one or more dot-separated tokens; a token
/// <c>:name</c> captures a parameter, <c>*</c> matches any one token and a final <c>&gt;</c>
/// matches the rest of the subject.
/// </summary>
public sealed record EndpointRegistration
{
    public required string Path { get; init; }

    public string? Description { get; init; }

    public IReadOnlyList<HeaderDoc>? Headers { get; init; }

    public IReadOnlyList<ParameterDoc>? Parameters { get; init; }

    public ResponseDoc? Response { get; init; }

    public required EndpointHandler Handler { get; init; }

    /// <summary>
    /// The queue-balanced delivery never replies (the Go "forwarded" result): the real answer
    /// comes from another subscription.
    /// </summary>
    internal bool Silent { get; init; }
}

public sealed record HeaderDoc
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("description")]
    public string? Description { get; init; }

    [JsonPropertyName("required")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Required { get; init; }

    [JsonPropertyName("example")]
    public string? Example { get; init; }
}

public sealed record ParameterDoc
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("description")]
    public string? Description { get; init; }

    [JsonPropertyName("required")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Required { get; init; }

    [JsonPropertyName("example")]
    public string? Example { get; init; }
}

public sealed record ResponseDoc
{
    [JsonPropertyName("description")]
    public string? Description { get; init; }

    [JsonPropertyName("contentType")]
    public string? ContentType { get; init; }

    [JsonPropertyName("example")]
    public string? Example { get; init; }

    [JsonPropertyName("headers")]
    public IReadOnlyList<HeaderDoc>? Headers { get; init; }
}
