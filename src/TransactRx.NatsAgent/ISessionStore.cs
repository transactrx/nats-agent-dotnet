using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent;

/// <summary>
/// Backs the optional sessions endpoints. Implementations own persistence entirely; the
/// protocol only defines the shapes. Throw <see cref="SessionNotFoundException"/> when the
/// session does not exist for that user.
/// </summary>
public interface ISessionStore
{
    Task<IReadOnlyList<SessionMeta>> ListAsync(string userId, CancellationToken cancellationToken);

    Task<SessionGetResponse> GetAsync(string userId, string sessionId, CancellationToken cancellationToken);

    Task DeleteAsync(string userId, string sessionId, CancellationToken cancellationToken);

    Task RenameAsync(string userId, string sessionId, string title, CancellationToken cancellationToken);

    Task SetFavoriteAsync(string userId, string sessionId, bool favorite, CancellationToken cancellationToken);
}

/// <summary>
/// Must be thrown by <see cref="ISessionStore"/> methods when the session does not exist for
/// that user; it maps to 404/4041 on the wire.
/// </summary>
public sealed class SessionNotFoundException(string message = "session not found", Exception? inner = null)
    : Exception(message, inner);
