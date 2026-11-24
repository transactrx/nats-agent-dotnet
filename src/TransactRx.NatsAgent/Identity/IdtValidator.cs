using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using TransactRx.NatsAgent.Internal;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;

namespace TransactRx.NatsAgent.Identity;

/// <summary>
/// Configures inbound Internal Delegation Token checks. The default value = disabled. Fields
/// mirror the org env contract so a deployment configures every agent and tool the same way.
/// </summary>
public sealed record IdtValidation
{
    internal const string DefaultIdentityBasePath = "trx.identityservice";
    internal const string DefaultValidateSubject = "validateInternalToken";
    internal static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);
    internal static readonly TimeSpan DefaultCacheTtl = TimeSpan.FromSeconds(300);

    /// <summary><c>IDT_VALIDATION</c></summary>
    public bool Enabled { get; init; }

    /// <summary><c>IDT_OBSERVE_ONLY</c>: validate + log, never block.</summary>
    public bool ObserveOnly { get; init; }

    /// <summary><c>IDT_FAIL_OPEN</c>: allow when identity is unreachable; never for a missing token.</summary>
    public bool FailOpen { get; init; }

    /// <summary><c>NATS_IDENTITY_BASE_PATH</c> + "." + <c>NATS_IDENTITY_VALIDATE_SUBJECT</c>.</summary>
    public string? Subject { get; init; }

    /// <summary><c>IDT_VALIDATE_TIMEOUT_SECONDS</c>. Zero = the default (5s).</summary>
    public TimeSpan Timeout { get; init; }

    /// <summary><c>IDT_VALIDATE_CACHE_SECONDS</c>; zero = validate every request.</summary>
    public TimeSpan CacheTtl { get; init; }

    /// <summary>Reads the org env contract.</summary>
    public static IdtValidation FromEnvironment(ILogger? logger = null) => FromEnvironment(Environment.GetEnvironmentVariable, logger);

    internal static IdtValidation FromEnvironment(Func<string, string?> env, ILogger? logger = null)
    {
        logger ??= StdoutLogger.Instance;
        var timeout = DefaultTimeout;
        var cacheTtl = DefaultCacheTtl;

        var basePath = Trimmed("NATS_IDENTITY_BASE_PATH") is { Length: > 0 } b ? b : DefaultIdentityBasePath;
        var suffix = Trimmed("NATS_IDENTITY_VALIDATE_SUBJECT") is { Length: > 0 } s ? s : DefaultValidateSubject;

        if (Trimmed("IDT_VALIDATE_TIMEOUT_SECONDS") is { Length: > 0 } rawTimeout)
        {
            if (int.TryParse(rawTimeout, out var n) && n > 0)
            {
                timeout = TimeSpan.FromSeconds(n);
            }
            else
            {
                logger.LogWarning("IDT_METRIC event=config.warn var=IDT_VALIDATE_TIMEOUT_SECONDS value=\"{Value}\" using_default={Default}", rawTimeout, GoDuration.Format(DefaultTimeout));
            }
        }

        if (Trimmed("IDT_VALIDATE_CACHE_SECONDS") is { Length: > 0 } rawCache)
        {
            if (int.TryParse(rawCache, out var n) && n >= 0)
            {
                cacheTtl = TimeSpan.FromSeconds(n);
            }
            else
            {
                logger.LogWarning("IDT_METRIC event=config.warn var=IDT_VALIDATE_CACHE_SECONDS value=\"{Value}\" using_default={Default}", rawCache, GoDuration.Format(DefaultCacheTtl));
            }
        }

        return new IdtValidation
        {
            Enabled = Bool("IDT_VALIDATION"),
            ObserveOnly = Bool("IDT_OBSERVE_ONLY"),
            FailOpen = Bool("IDT_FAIL_OPEN"),
            Subject = basePath + "." + suffix,
            Timeout = timeout,
            CacheTtl = cacheTtl,
        };

        string Trimmed(string key) => (env(key) ?? "").Trim();
        bool Bool(string key) => string.Equals(Trimmed(key), "true", StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>What the receiver learned about the caller of a request.</summary>
public sealed record CallerIdentity
{
    public string UserId { get; init; } = "";

    public string AccountId { get; init; } = "";

    /// <summary>
    /// True only when identity confirmed the token AND granted the receiver's function. False
    /// in pass-through modes (validation off, observe-only, fail-open) — treat any body userId
    /// as caller-asserted.
    /// </summary>
    public bool Verified { get; init; }

    /// <summary>
    /// The raw header value, exposed so the receiver can forward it on onward calls via
    /// <see cref="CallContext.WithIdt"/>.
    /// </summary>
    public string Idt { get; init; } = "";

    // The token never appears in logs through an accidental ToString.
    public override string ToString() => $"CallerIdentity {{ UserId = {UserId}, AccountId = {AccountId}, Verified = {Verified} }}";
}

/// <summary>
/// Validates the Internal Delegation Token (X-TRX-IDT) that callers attach to authenticated
/// agent and tool requests (SPEC §5.1, §8.4) for one (appId, functionId) registration.
/// Agents and tool hosts share this implementation so every principal on the mesh enforces
/// identity the same way: a token is checked against identity's validateInternalToken for the
/// receiver's registration, before any work is done.
/// </summary>
public sealed class IdtValidator
{
    private const int CacheMaxEntries = 10_000;
    private const string ReasonMissingIdt = "MISSING_IDT";
    private const string ReasonValidateError = "VALIDATE_ERROR";
    private const string ReasonDeniedFn = "DENIED_FN";
    private const string ReasonInvalidIdentity = "INVALID_IDENTITY";

    /// <summary>Caps the log-only token prefix so a malformed header can't blow up log lines.</summary>
    internal const int IdPrefixLogMax = 40;

    private readonly INatsConnection? _nc;
    private readonly AgentAccess _access;
    private readonly ILogger _logger;
    private readonly TimeProvider _time;
    private readonly object _gate = new();
    private Dictionary<string, (CallerIdentity Id, DateTimeOffset Expires)> _cache = [];

    /// <param name="nc">Connection used to reach identity.</param>
    /// <param name="access">
    /// The receiver's registration. May be null only when <paramref name="config"/> is
    /// disabled; callers must refuse an enabled configuration without a complete declaration.
    /// </param>
    /// <param name="config">The validation settings; defaults are applied to unset fields.</param>
    /// <param name="logger">Destination of the <c>IDT_METRIC</c> lines. Default: stdout.</param>
    /// <param name="timeProvider">Clock for the decision cache.</param>
    public IdtValidator(INatsConnection? nc, AgentAccess? access, IdtValidation config, ILogger? logger = null, TimeProvider? timeProvider = null)
    {
        _nc = nc;
        _logger = logger ?? StdoutLogger.Instance;
        _time = timeProvider ?? TimeProvider.System;
        Config = config with
        {
            Timeout = config.Timeout <= TimeSpan.Zero ? IdtValidation.DefaultTimeout : config.Timeout,
            Subject = string.IsNullOrEmpty(config.Subject)
                ? IdtValidation.DefaultIdentityBasePath + "." + IdtValidation.DefaultValidateSubject
                : config.Subject,
        };
        _access = access ?? new AgentAccess();
        if (access is null && config.Enabled)
        {
            // Constructors already refuse this configuration; guard here too so Authorize
            // never runs against a missing declaration.
            _logger.LogWarning("IDT_METRIC event=validate.misconfig reason=missing_access");
        }

        Call = NatsCall;
    }

    /// <summary>The effective configuration (defaults applied).</summary>
    public IdtValidation Config { get; }

    /// <summary>Performs the identity round-trip; replaced in unit tests.</summary>
    internal Func<ValidateRequest, CancellationToken, Task<ValidateResponse>> Call { get; set; }

    /// <summary>
    /// Decides whether a request carrying <paramref name="token"/> (header value, may be
    /// empty) may proceed. <paramref name="scope"/> partitions the decision cache (an agent
    /// passes the session id). Returns the <see cref="CallerIdentity"/> on allow. Pass-through
    /// modes return <c>Verified=false</c>, never an error.
    /// </summary>
    /// <exception cref="NatsServiceException">Denied: the 403 / 4031 envelope, message = reason.</exception>
    public Task<CallerIdentity> AuthorizeAsync(string? token, string? scope, CancellationToken cancellationToken = default) =>
        AuthorizeCoreAsync(token, scope, useCache: true, cancellationToken);

    /// <summary>
    /// <see cref="AuthorizeAsync"/> without reading the decision cache: identity is always
    /// asked, so a token revoked since its last cached allow is refused. Used to accept a
    /// rotated token mid-run.
    /// </summary>
    public Task<CallerIdentity> AuthorizeFreshAsync(string? token, string? scope, CancellationToken cancellationToken = default) =>
        AuthorizeCoreAsync(token, scope, useCache: false, cancellationToken);

    private async Task<CallerIdentity> AuthorizeCoreAsync(string? rawToken, string? scope, bool useCache, CancellationToken ct)
    {
        var token = (rawToken ?? "").Trim();
        if (!Config.Enabled)
        {
            return new CallerIdentity { Idt = token };
        }

        var (appId, fnId) = (_access.AppId, _access.FunctionId);
        if (token.Length == 0)
        {
            // FailOpen only covers identity being unreachable, never a missing token — a
            // caller that sends no header at all must always be denied unless we're in
            // observe-only (log, don't block).
            if (Config.ObserveOnly)
            {
                _logger.LogInformation(
                    "IDT_METRIC event=validate.pass_through reason={Reason} agent={Agent} function={Function} observe={Observe} failOpen={FailOpen}",
                    ReasonMissingIdt, appId, fnId, Bool(Config.ObserveOnly), Bool(Config.FailOpen));
                return new CallerIdentity();
            }

            _logger.LogInformation("IDT_METRIC event=validate.deny reason={Reason} agent={Agent} function={Function}", ReasonMissingIdt, appId, fnId);
            throw Forbidden(ReasonMissingIdt);
        }

        var key = CacheKey(token, scope ?? "", appId, fnId);
        if (useCache && Config.CacheTtl > TimeSpan.Zero)
        {
            lock (_gate)
            {
                if (_cache.TryGetValue(key, out var entry) && _time.GetUtcNow() < entry.Expires)
                {
                    return entry.Id with { Idt = token };
                }
            }
        }

        ValidateResponse resp;
        try
        {
            resp = await Call(new ValidateRequest { Idt = token, AgentId = appId, FunctionId = fnId }, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            if (Config.ObserveOnly || Config.FailOpen)
            {
                _logger.LogInformation(
                    "IDT_METRIC event=validate.pass_through reason={Reason} agent={Agent} function={Function} idtid=\"{IdtId}\" err=\"{Error}\"",
                    ReasonValidateError, appId, fnId, IdPrefix(token), ex.Message);
                return new CallerIdentity { Idt = token };
            }

            _logger.LogInformation(
                "IDT_METRIC event=validate.deny reason={Reason} agent={Agent} function={Function} idtid=\"{IdtId}\" err=\"{Error}\"",
                ReasonValidateError, appId, fnId, IdPrefix(token), ex.Message);
            throw Forbidden(ReasonValidateError);
        }

        var reason = "";
        if (!resp.Valid)
        {
            reason = string.IsNullOrEmpty(resp.Reason) ? "INVALID_IDT" : resp.Reason;
        }
        else if (!resp.FunctionGranted)
        {
            reason = ReasonDeniedFn;
        }
        else if (string.IsNullOrWhiteSpace(resp.UserId))
        {
            // Valid and granted but identity didn't return a user id: a verified caller would
            // otherwise silently overwrite a legitimate body userId with "". Treat as a deny
            // in enforcing mode; observe-only just logs.
            reason = ReasonInvalidIdentity;
        }

        var userId = resp.UserId ?? "";
        var accountId = resp.AccountId ?? "";
        if (reason.Length > 0)
        {
            if (Config.ObserveOnly)
            {
                _logger.LogInformation(
                    "IDT_METRIC event=validate.observe decision=deny reason={Reason} agent={Agent} function={Function} idtid=\"{IdtId}\" user={User}",
                    reason, appId, fnId, IdPrefix(token), userId);

                // Fresh checks (renew) compare callers, so they get the ids identity
                // resolved; Authorize keeps its admission shape.
                return useCache
                    ? new CallerIdentity { Idt = token }
                    : new CallerIdentity { Idt = token, UserId = userId, AccountId = accountId };
            }

            _logger.LogInformation(
                "IDT_METRIC event=validate.deny reason={Reason} agent={Agent} function={Function} idtid=\"{IdtId}\" user={User} account={Account}",
                reason, appId, fnId, IdPrefix(token), userId, accountId);
            throw Forbidden(reason);
        }

        var id = new CallerIdentity { UserId = userId, AccountId = accountId, Verified = !Config.ObserveOnly, Idt = token };
        if (Config.ObserveOnly)
        {
            _logger.LogInformation(
                "IDT_METRIC event=validate.observe decision=allow agent={Agent} function={Function} idtid=\"{IdtId}\" user={User}",
                appId, fnId, IdPrefix(token), userId);

            // Never cache in observe-only: caching an allow would silence the per-request
            // observe log after the first hit, undercounting the rollout signal this mode
            // exists to produce.
            return id;
        }

        _logger.LogInformation(
            "IDT_METRIC event=validate.allow agent={Agent} function={Function} idtid=\"{IdtId}\" user={User} account={Account}",
            appId, fnId, IdPrefix(token), userId, accountId);
        if (Config.CacheTtl > TimeSpan.Zero)
        {
            lock (_gate)
            {
                var now = _time.GetUtcNow();
                if (_cache.Count >= CacheMaxEntries)
                {
                    foreach (var expired in _cache.Where(e => now > e.Value.Expires).Select(e => e.Key).ToList())
                    {
                        _cache.Remove(expired);
                    }

                    if (_cache.Count >= CacheMaxEntries)
                    {
                        // still full: drop everything rather than grow unbounded
                        _cache = [];
                    }
                }

                // The token itself is never kept in the cache.
                _cache[key] = (id with { Idt = "" }, now + Config.CacheTtl);
            }
        }

        return id;
    }

    internal int CacheCount
    {
        get
        {
            lock (_gate)
            {
                return _cache.Count;
            }
        }
    }

    private async Task<ValidateResponse> NatsCall(ValidateRequest req, CancellationToken ct)
    {
        if (_nc is null)
        {
            throw new InvalidOperationException("no NATS connection to reach identity");
        }

        NatsMsg<byte[]> reply;
        try
        {
            reply = await _nc.RequestAsync<byte[], byte[]>(
                Config.Subject!, WireJson.Serialize(req), null, Framing.Raw, Framing.Raw,
                replyOpts: new NatsSubOpts { Timeout = Config.Timeout },
                cancellationToken: ct).ConfigureAwait(false);
        }
        catch (NatsNoReplyException ex)
        {
            throw new TimeoutException("identity did not reply in time", ex);
        }

        try
        {
            return WireJson.Deserialize<ValidateResponse>(reply.Data ?? []) ?? new ValidateResponse();
        }
        catch (System.Text.Json.JsonException ex)
        {
            throw new InvalidDataException("decoding identity reply: " + ex.Message, ex);
        }
    }

    /// <summary>
    /// Returns "IDT-&lt;uuid&gt;" from "IDT-&lt;uuid&gt;.&lt;cipher&gt;" — safe for LOGS ONLY;
    /// never log the cipher tail. Not used for cache keys (see <see cref="CacheKey"/>): the
    /// prefix alone does not authenticate the token, so caching on it would let
    /// "IDT-&lt;same-uuid&gt;.&lt;forged-cipher&gt;" ride an existing allow.
    /// </summary>
    internal static string IdPrefix(string idt)
    {
        var dot = idt.IndexOf('.');
        var end = dot >= 0 ? dot : idt.Length;
        return idt[..Math.Min(end, IdPrefixLogMax)];
    }

    /// <summary>
    /// Authenticates on the full token (via its hash), never the human-readable prefix alone,
    /// so a forged cipher tail sharing a cached token's prefix cannot ride that cache entry.
    /// </summary>
    internal static string CacheKey(string idt, string scope, string appId, string fnId) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(idt))) + "|" + scope + "|" + appId + "|" + fnId;

    private static NatsServiceException Forbidden(string reason) => NatsServiceException.Authorization(reason, Codes.Forbidden);

    private static string Bool(bool b) => b ? "true" : "false";

    /// <summary>Wire shapes of identity's validateInternalToken.</summary>
    internal sealed record ValidateRequest
    {
        [JsonPropertyName("idt")]
        public string Idt { get; init; } = "";

        [JsonPropertyName("agentId")]
        public string AgentId { get; init; } = "";

        [JsonPropertyName("functionId")]
        public string FunctionId { get; init; } = "";
    }

    internal sealed record ValidateResponse
    {
        [JsonPropertyName("valid")]
        public bool Valid { get; init; }

        [JsonPropertyName("functionGranted")]
        public bool FunctionGranted { get; init; }

        [JsonPropertyName("userId")]
        public string? UserId { get; init; }

        [JsonPropertyName("accountId")]
        public string? AccountId { get; init; }

        [JsonPropertyName("reason")]
        public string? Reason { get; init; }
    }
}
