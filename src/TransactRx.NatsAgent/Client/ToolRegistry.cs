using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Client;

/// <summary>Tunes how an agent watches the tool mesh.</summary>
public sealed record ToolRegistryOptions
{
    /// <summary>Restricts the registry to these tool names. Empty = all tools.</summary>
    public IReadOnlyList<string>? Allow { get; init; }

    /// <summary>Always wins over <see cref="Allow"/>.</summary>
    public IReadOnlyList<string>? Deny { get; init; }

    /// <summary>Applied on the discover sweep (server-side matching).</summary>
    public DiscoverFilter? Filter { get; init; }

    /// <summary>
    /// How often to re-discover. Default 5m (spec recommendation). Announcements arrive
    /// immediately regardless.
    /// </summary>
    public TimeSpan SweepInterval { get; init; }

    /// <summary>
    /// Drops a tool not seen (sweep or announce) for this long. Default 3× the sweep interval.
    /// </summary>
    public TimeSpan StaleAfter { get; init; }

    /// <summary>
    /// Fires after the tool set changes (added, dropped, or card updated). Called from
    /// registry tasks; keep it fast.
    /// </summary>
    public Action? OnChange { get; init; }
}

/// <summary>
/// Maintains a live snapshot of the network tools an agent may use: an initial sweep plus
/// announce subscriptions pick up new tools the moment they deploy, and periodic sweeps age
/// dead ones out.
/// </summary>
public sealed class ToolRegistry : IAsyncDisposable
{
    private readonly ToolClient _client;
    private readonly ToolRegistryOptions _opts;
    private readonly TimeProvider _time;
    private readonly object _gate = new();
    private readonly Dictionary<string, (ToolCard Card, string Json, DateTimeOffset LastSeen)> _tools = [];
    private readonly CancellationTokenSource _stop = new();
    private INatsSub<byte[]>? _announceSub;
    private int _stopped;

    /// <summary>Creates a registry; call <see cref="StartAsync"/> to begin watching.</summary>
    public ToolRegistry(ToolClient client, ToolRegistryOptions? options = null, TimeProvider? timeProvider = null)
    {
        _client = client;
        var opts = options ?? new ToolRegistryOptions();
        if (opts.SweepInterval <= TimeSpan.Zero)
        {
            opts = opts with { SweepInterval = TimeSpan.FromMinutes(5) };
        }

        if (opts.StaleAfter <= TimeSpan.Zero)
        {
            opts = opts with { StaleAfter = opts.SweepInterval * 3 };
        }

        _opts = opts;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>The current snapshot, sorted by name.</summary>
    public IReadOnlyList<ToolCard> Tools
    {
        get
        {
            lock (_gate)
            {
                return [.. _tools.Values.Select(e => e.Card).OrderBy(c => c.Name, StringComparer.Ordinal)];
            }
        }
    }

    /// <summary>
    /// Does a synchronous initial sweep, then watches announcements and sweeps periodically
    /// until <see cref="StopAsync"/>.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _announceSub = await _client.Connection.SubscribeCoreAsync(
            Protocol.ToolAnnounceSubject, serializer: Framing.Raw, cancellationToken: cancellationToken).ConfigureAwait(false);
        _ = Background.Pump(_announceSub, _client.Logger, msg =>
        {
            if (WireJson.TryDeserialize<ToolCard>(msg.Data ?? [], out var card)
                && !string.IsNullOrEmpty(card.Name)
                && card.MatchesFilter(_opts.Filter)
                && Allowed(card.Name))
            {
                Absorb([card]);
            }

            return ValueTask.CompletedTask;
        });

        await SweepAsync(cancellationToken).ConfigureAwait(false);

        _ = Background.Run(async () =>
        {
            try
            {
                using var timer = new PeriodicTimer(_opts.SweepInterval, _time);
                while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
                {
                    using var limit = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
                    limit.CancelAfter(_opts.SweepInterval / 2);
                    await SweepAsync(limit.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // stopped
            }
        });
    }

    /// <summary>Ends watching. The last snapshot stays readable.</summary>
    public async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        _stop.Cancel();
        await Background.Close(_announceSub).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => new(StopAsync());

    /// <summary>Returns one tool's card if present.</summary>
    public bool TryGet(string name, out ToolCard card)
    {
        lock (_gate)
        {
            var found = _tools.TryGetValue(name, out var entry);
            card = entry.Card;
            return found;
        }
    }

    /// <summary>
    /// Executes a registered tool, sizing the request timeout from the tool's advertised
    /// timeoutSeconds.
    /// </summary>
    /// <exception cref="KeyNotFoundException">The tool is not in the registry.</exception>
    public Task<ToolRunResponse> RunAsync(string name, ToolRunRequest request, CancellationToken cancellationToken = default)
    {
        if (!TryGet(name, out var card))
        {
            throw new KeyNotFoundException($"network tool \"{name}\" is not in the registry");
        }

        var timeout = card.TimeoutSeconds > 0
            ? TimeSpan.FromSeconds(card.TimeoutSeconds) + ToolClient.RunTimeoutMargin
            : ToolClient.DefaultRunTimeout;
        return _client.RunAsync(name, request, timeout, cancellationToken);
    }

    private bool Allowed(string name)
    {
        if (_opts.Deny?.Contains(name) ?? false)
        {
            return false;
        }

        return _opts.Allow is not { Count: > 0 } || _opts.Allow.Contains(name);
    }

    internal async Task SweepAsync(CancellationToken ct)
    {
        IReadOnlyList<ToolCard> cards;
        try
        {
            cards = await _client.DiscoverAsync(_opts.Filter, null, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _client.Logger.LogWarning("tool registry sweep failed: {Error}", ex.Message);
            return;
        }

        Absorb([.. cards.Where(c => Allowed(c.Name))]);
        EvictStale();
    }

    private void Absorb(IReadOnlyList<ToolCard> cards)
    {
        var changed = false;
        var now = _time.GetUtcNow();
        lock (_gate)
        {
            foreach (var card in cards)
            {
                // Any card change (schema, timeout, access, description) must reach consumers;
                // tools do not always bump Version for those.
                var json = WireJson.SerializeToString(card);
                if (!_tools.TryGetValue(card.Name, out var previous) || previous.Json != json)
                {
                    changed = true;
                }

                _tools[card.Name] = (card, json, now);
            }
        }

        if (changed)
        {
            _opts.OnChange?.Invoke();
        }
    }

    private void EvictStale()
    {
        var cutoff = _time.GetUtcNow() - _opts.StaleAfter;
        var changed = false;
        lock (_gate)
        {
            foreach (var (name, entry) in _tools.ToArray())
            {
                if (entry.LastSeen < cutoff)
                {
                    _tools.Remove(name);
                    changed = true;
                    _client.Logger.LogInformation("tool registry: dropping stale tool \"{Name}\" (last seen {LastSeen:O})", name, entry.LastSeen);
                }
            }
        }

        if (changed)
        {
            _opts.OnChange?.Invoke();
        }
    }
}
