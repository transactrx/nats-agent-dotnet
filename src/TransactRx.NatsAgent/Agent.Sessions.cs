using System.Text.Json;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent;

// The optional sessions endpoints (capability "sessions"), backed by an ISessionStore.
public sealed partial class Agent
{
    private static readonly TimeSpan SessionOpTimeout = TimeSpan.FromSeconds(30);

    private IEnumerable<EndpointRegistration> SessionRegistrations() =>
    [
        new()
        {
            Path = "sessionsList",
            Description = "List the user's sessions, favorites first then by recency. Request: {userId}.",
            Response = new ResponseDoc { Description = "SessionsListResponse", ContentType = "application/json" },
            Handler = HandleSessionsList,
        },
        new()
        {
            Path = "sessionsGet",
            Description = "Fetch one session's transcript. Request: {userId, sessionId}.",
            Response = new ResponseDoc { Description = "SessionGetResponse", ContentType = "application/json" },
            Handler = HandleSessionsGet,
        },
        new()
        {
            Path = "sessionsDelete",
            Description = "Delete a session and its transcript. Request: {userId, sessionId}.",
            Response = new ResponseDoc { Description = "SessionDeleteResponse", ContentType = "application/json" },
            Handler = HandleSessionsDelete,
        },
        new()
        {
            Path = "sessionsRename",
            Description = "Rename a session. Request: {userId, sessionId, title}.",
            Response = new ResponseDoc { Description = "SessionRenameResponse", ContentType = "application/json" },
            Handler = HandleSessionsRename,
        },
        new()
        {
            Path = "sessionsSetFavorite",
            Description = "Set or clear a session's favorite flag. Request: {userId, sessionId, favorite}.",
            Response = new ResponseDoc { Description = "SessionSetFavoriteResponse", ContentType = "application/json" },
            Handler = HandleSessionsSetFavorite,
        },
    ];

    /// <summary>
    /// Runs one sessions endpoint: parse, check required fields, apply IDT enforcement, then
    /// call the store with a 30s limit. On a verified identity the body's userId is replaced
    /// by the verified one before <paramref name="run"/> sees it.
    /// </summary>
    private async Task<byte[]> SessionEndpoint<TRequest, TResponse>(
        ServiceRequest request,
        Func<TRequest, string?> missingField,
        Func<TRequest, string> sessionId,
        Func<TRequest, string, CancellationToken, Task<TResponse>> run,
        CancellationToken ct)
        where TRequest : class, new()
    {
        TRequest req;
        try
        {
            req = WireJson.Deserialize<TRequest>(request.Body) ?? new TRequest();
        }
        catch (JsonException ex)
        {
            throw NatsServiceException.Validation("invalid request body", Codes.InvalidBody, ex);
        }

        if (missingField(req) is { } missing)
        {
            throw NatsServiceException.Validation(missing + " is required", Codes.MissingField);
        }

        var userId = UserIdOf(req);
        var id = await AuthorizeAsync(request, sessionId(req), ct).ConfigureAwait(false);
        if (id.Verified)
        {
            userId = id.UserId;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(SessionOpTimeout);
        try
        {
            return WireJson.Serialize(await run(req, userId, timeout.Token).ConfigureAwait(false));
        }
        catch (SessionNotFoundException)
        {
            throw new NatsServiceException(new NatsServiceError { Status = 404, ApiStatusCode = Codes.UnknownSession, ErrorMessage = "session not found" });
        }
        catch (NatsServiceException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw NatsServiceException.Server("session store error", Codes.Internal, ex);
        }
    }

    private static string UserIdOf(object request) => request switch
    {
        SessionsListRequest r => r.UserId ?? "",
        SessionGetRequest r => r.UserId ?? "",
        SessionDeleteRequest r => r.UserId ?? "",
        SessionRenameRequest r => r.UserId ?? "",
        SessionSetFavoriteRequest r => r.UserId ?? "",
        _ => "",
    };

    private static string? Missing(params (string Name, string? Value)[] fields)
    {
        foreach (var (name, value) in fields)
        {
            if (string.IsNullOrEmpty(value))
            {
                return name;
            }
        }

        return null;
    }

    private Task<byte[]> HandleSessionsList(ServiceRequest request, CancellationToken ct) =>
        SessionEndpoint<SessionsListRequest, SessionsListResponse>(
            request,
            r => Missing(("userId", r.UserId)),
            _ => "",
            async (_, userId, c) => new SessionsListResponse { Sessions = await _sessions!.ListAsync(userId, c).ConfigureAwait(false) ?? [] },
            ct);

    private Task<byte[]> HandleSessionsGet(ServiceRequest request, CancellationToken ct) =>
        SessionEndpoint<SessionGetRequest, SessionGetResponse>(
            request,
            r => Missing(("userId", r.UserId), ("sessionId", r.SessionId)),
            r => r.SessionId,
            async (r, userId, c) =>
            {
                var resp = await _sessions!.GetAsync(userId, r.SessionId, c).ConfigureAwait(false);
                return resp.Messages is null ? resp with { Messages = [] } : resp;
            },
            ct);

    private Task<byte[]> HandleSessionsDelete(ServiceRequest request, CancellationToken ct) =>
        SessionEndpoint<SessionDeleteRequest, SessionDeleteResponse>(
            request,
            r => Missing(("userId", r.UserId), ("sessionId", r.SessionId)),
            r => r.SessionId,
            async (r, userId, c) =>
            {
                await _sessions!.DeleteAsync(userId, r.SessionId, c).ConfigureAwait(false);
                return new SessionDeleteResponse { SessionId = r.SessionId, Deleted = true };
            },
            ct);

    private Task<byte[]> HandleSessionsRename(ServiceRequest request, CancellationToken ct) =>
        SessionEndpoint<SessionRenameRequest, SessionRenameResponse>(
            request,
            r => Missing(("userId", r.UserId), ("sessionId", r.SessionId), ("title", r.Title)),
            r => r.SessionId,
            async (r, userId, c) =>
            {
                await _sessions!.RenameAsync(userId, r.SessionId, r.Title, c).ConfigureAwait(false);
                return new SessionRenameResponse { SessionId = r.SessionId, Title = r.Title };
            },
            ct);

    private Task<byte[]> HandleSessionsSetFavorite(ServiceRequest request, CancellationToken ct) =>
        SessionEndpoint<SessionSetFavoriteRequest, SessionSetFavoriteResponse>(
            request,
            r => Missing(("userId", r.UserId), ("sessionId", r.SessionId)),
            r => r.SessionId,
            async (r, userId, c) =>
            {
                await _sessions!.SetFavoriteAsync(userId, r.SessionId, r.Favorite, c).ConfigureAwait(false);
                return new SessionSetFavoriteResponse { SessionId = r.SessionId, Favorite = r.Favorite };
            },
            ct);
}
