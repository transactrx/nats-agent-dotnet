using TransactRx.NatsAgent;
using TransactRx.NatsAgent.Wire;

namespace BaseballAgent;

/// <summary>
/// Conversations kept in memory: enough for the model to follow a conversation and for a
/// client to list and reopen it, and gone when the process stops. A deployed agent stores
/// them durably (DynamoDB, Postgres…) behind this same interface.
/// </summary>
internal sealed class MemorySessionStore : ISessionStore
{
    // Older turns than this are not sent to the model again.
    private const int HistoryMessages = 20;

    private readonly Lock _gate = new();
    private readonly Dictionary<(string User, string Session), SessionGetResponse> _sessions = [];

    /// <summary>The recent conversation, oldest first, in the shape the model takes.</summary>
    public IReadOnlyList<Message> History(string userId, string sessionId)
    {
        lock (_gate)
        {
            return _sessions.TryGetValue((userId, sessionId), out var session)
                ? [.. session.Messages.TakeLast(HistoryMessages).Select(stored => new Message { Role = stored.Role, Content = stored.Content })]
                : [];
        }
    }

    /// <summary>Records one finished exchange.</summary>
    public void Append(string userId, string sessionId, string question, string answer)
    {
        var now = DateTimeOffset.UtcNow.ToString("O");
        lock (_gate)
        {
            var session = _sessions.GetValueOrDefault((userId, sessionId))
                ?? new SessionGetResponse { SessionId = sessionId, Title = question.Length > 60 ? question[..60] + "…" : question };
            _sessions[(userId, sessionId)] = session with
            {
                Messages =
                [
                    .. session.Messages,
                    new StoredMessage { Role = "user", Content = [new ContentBlock { Text = question }], Timestamp = now },
                    new StoredMessage { Role = "assistant", Content = [new ContentBlock { Text = answer }], Timestamp = now },
                ],
            };
        }
    }

    public Task<IReadOnlyList<SessionMeta>> ListAsync(string userId, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            IReadOnlyList<SessionMeta> list = [.. _sessions
                .Where(entry => entry.Key.User == userId)
                .Select(entry => new SessionMeta
                {
                    SessionId = entry.Value.SessionId,
                    Title = entry.Value.Title,
                    Favorite = entry.Value.Favorite,
                    CreatedAt = entry.Value.Messages.FirstOrDefault()?.Timestamp,
                    LastMessageAt = entry.Value.Messages.LastOrDefault()?.Timestamp,
                })
                .OrderByDescending(meta => meta.Favorite)
                .ThenByDescending(meta => meta.LastMessageAt)];
            return Task.FromResult(list);
        }
    }

    public Task<SessionGetResponse> GetAsync(string userId, string sessionId, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return Task.FromResult(Find(userId, sessionId));
        }
    }

    public Task DeleteAsync(string userId, string sessionId, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Find(userId, sessionId);
            _sessions.Remove((userId, sessionId));
            return Task.CompletedTask;
        }
    }

    public Task RenameAsync(string userId, string sessionId, string title, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _sessions[(userId, sessionId)] = Find(userId, sessionId) with { Title = title };
            return Task.CompletedTask;
        }
    }

    public Task SetFavoriteAsync(string userId, string sessionId, bool favorite, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _sessions[(userId, sessionId)] = Find(userId, sessionId) with { Favorite = favorite };
            return Task.CompletedTask;
        }
    }

    // The runtime turns this exception into the protocol's "unknown session" error (404 / 4041).
    private SessionGetResponse Find(string userId, string sessionId) =>
        _sessions.GetValueOrDefault((userId, sessionId)) ?? throw new SessionNotFoundException();
}
