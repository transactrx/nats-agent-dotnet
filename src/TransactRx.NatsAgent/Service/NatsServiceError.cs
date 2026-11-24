using System.Text.Json.Serialization;

namespace TransactRx.NatsAgent.Service;

/// <summary>
/// The error envelope every transactrx NATS service replies with (SPEC §9; the Go
/// <c>nats-service</c> <c>NatsServiceError</c>).
/// </summary>
public sealed record NatsServiceError
{
    /// <summary>HTTP-style status: 400, 403, 404, 429, 500...</summary>
    [JsonPropertyName("status")]
    public int Status { get; init; }

    [JsonPropertyName("errorMessage")]
    public string ErrorMessage { get; init; } = "";

    /// <summary>Detail code (<see cref="Wire.Codes"/>).</summary>
    [JsonPropertyName("apiStatusCode")]
    public int ApiStatusCode { get; init; }

    [JsonPropertyName("internalError")]
    public string InternalError { get; init; } = "";
}

/// <summary>
/// A protocol-level error reply (SPEC §9 envelope). Thrown by the clients when an agent or
/// tool answers with the envelope, and thrown by endpoint handlers to make the service answer
/// with it.
/// </summary>
public class NatsServiceException : Exception
{
    public NatsServiceException(NatsServiceError error, Exception? inner = null)
        : base($"service error {error.Status} (code {error.ApiStatusCode}): {error.ErrorMessage}", inner)
    {
        Error = error;
    }

    public NatsServiceException(int status, int apiStatusCode, string errorMessage, Exception? inner = null)
        : this(new NatsServiceError { Status = status, ApiStatusCode = apiStatusCode, ErrorMessage = errorMessage, InternalError = inner?.Message ?? "" }, inner)
    {
    }

    public NatsServiceError Error { get; }

    public int Status => Error.Status;

    public int ApiStatusCode => Error.ApiStatusCode;

    public string ErrorMessage => Error.ErrorMessage;

    /// <summary>400.</summary>
    public static NatsServiceException Validation(string message, int apiStatusCode, Exception? inner = null) =>
        new(400, apiStatusCode, message, inner);

    /// <summary>500.</summary>
    public static NatsServiceException Server(string message, int apiStatusCode, Exception? inner = null) =>
        new(500, apiStatusCode, message, inner);

    /// <summary>403.</summary>
    public static NatsServiceException Authorization(string message, int apiStatusCode, Exception? inner = null) =>
        new(403, apiStatusCode, message, inner);

    /// <summary>404.</summary>
    public static NatsServiceException NotFound(string subject) =>
        new(404, 404, $"subject: {subject} not found");
}
