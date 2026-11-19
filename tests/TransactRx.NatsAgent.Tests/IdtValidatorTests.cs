using Microsoft.Extensions.Logging;
using TransactRx.NatsAgent.Identity;
using TransactRx.NatsAgent.Service;
using TransactRx.NatsAgent.Wire;
using ValidateRequest = TransactRx.NatsAgent.Identity.IdtValidator.ValidateRequest;
using ValidateResponse = TransactRx.NatsAgent.Identity.IdtValidator.ValidateResponse;

namespace TransactRx.NatsAgent.Tests;

/// <summary>
/// Port of pkg/idt/idt_test.go. The identity round-trip is replaced through the internal
/// <c>Call</c> seam (Go: <c>v.call</c>); a deny is a thrown <see cref="NatsServiceException"/>
/// (Go: the returned <c>*NatsServiceError</c>).
/// </summary>
public sealed class IdtValidatorTests
{
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly RecordingLogger _log = new();

    // The Go helper passes a nil reply where identity must not be consulted (a call would
    // panic). Here such a reply counts the call and throws; the tests assert the count is 0.
    private int _unexpectedCalls;

    private IdtValidator FakeValidator(IdtValidation cfg, Func<ValidateRequest, ValidateResponse>? reply)
    {
        var v = new IdtValidator(null, new AgentAccess { AppId = "appX", FunctionId = "fnX" }, cfg, _log);
        v.Call = (req, _) =>
        {
            if (reply is null)
            {
                Interlocked.Increment(ref _unexpectedCalls);
                throw new InvalidOperationException("identity must not be consulted");
            }

            try
            {
                return Task.FromResult(reply(req));
            }
            catch (Exception ex)
            {
                return Task.FromException<ValidateResponse>(ex);
            }
        };
        return v;
    }

    private static ValidateResponse Granted(string userId = "u1", string accountId = "a1") =>
        new() { Valid = true, FunctionGranted = true, UserId = userId, AccountId = accountId };

    private static async Task<NatsServiceException> Denied(Task<CallerIdentity> authorize) =>
        await Assert.ThrowsAsync<NatsServiceException>(() => authorize);

    [Fact]
    public async Task AuthorizeDisabledPassesThrough()
    {
        var v = FakeValidator(new IdtValidation { Enabled = false }, null);
        var id = await v.AuthorizeAsync("IDT-1.c", "s1");
        // disabled validator must pass through
        Assert.False(id.Verified);
        Assert.Equal("IDT-1.c", id.Idt);
        Assert.Equal(0, _unexpectedCalls);
    }

    [Fact]
    public async Task AuthorizeMissingIDTIs403()
    {
        var v = FakeValidator(new IdtValidation { Enabled = true }, null);
        var e = await Denied(v.AuthorizeAsync("", "s1"));
        Assert.Equal(403, e.Status);
        Assert.Equal(Codes.Forbidden, e.ApiStatusCode);
        Assert.Equal("MISSING_IDT", e.ErrorMessage);
        Assert.Equal(0, _unexpectedCalls);
    }

    [Fact]
    public async Task AuthorizeGrantedAndCached()
    {
        var calls = 0;
        var badRequests = new List<ValidateRequest>();
        var v = FakeValidator(new IdtValidation { Enabled = true, CacheTtl = Minute }, req =>
        {
            calls++;
            if (req.AgentId != "appX" || req.FunctionId != "fnX" || (req.Idt != "IDT-1.c" && req.Idt != "IDT-2.c"))
            {
                badRequests.Add(req); // bad request to identity
            }

            return Granted();
        });
        for (var i = 0; i < 3; i++)
        {
            var id = await v.AuthorizeAsync("IDT-1.c", "s1");
            Assert.True(id.Verified);
            Assert.Equal("u1", id.UserId);
            Assert.Equal("a1", id.AccountId);
        }

        // allow must be cached per (idtid,session)
        Assert.Equal(1, calls);

        // Different session or rotated token → new identity call.
        await v.AuthorizeAsync("IDT-1.c", "s2");
        await v.AuthorizeAsync("IDT-2.c", "s1");
        Assert.Equal(3, calls); // cache key must include sessionId and idtid
        Assert.Empty(badRequests);
    }

    [Fact]
    public async Task AuthorizeCacheKeyedOnFullTokenNotPrefix()
    {
        var calls = 0;
        var v = FakeValidator(new IdtValidation { Enabled = true, CacheTtl = Minute }, req =>
        {
            calls++;
            return req.Idt == "IDT-1.good"
                ? Granted()
                : new ValidateResponse { Valid = false, Reason = "DENIED_FORGED" };
        });
        var id = await v.AuthorizeAsync("IDT-1.good", "s1");
        Assert.True(id.Verified);
        Assert.Equal(1, calls);

        // Same uuid prefix, forged cipher tail, same session: must NOT ride the cached allow —
        // the cache key must authenticate on the full token.
        var e = await Denied(v.AuthorizeAsync("IDT-1.forged", "s1"));
        Assert.Equal(403, e.Status);
        Assert.Equal("DENIED_FORGED", e.ErrorMessage);
        Assert.Equal(2, calls); // forged token must trigger a fresh identity call
    }

    [Fact]
    public async Task AuthorizeCacheExpires()
    {
        var calls = 0;
        var v = FakeValidator(new IdtValidation { Enabled = true, CacheTtl = TimeSpan.FromMilliseconds(10) }, _ =>
        {
            calls++;
            return Granted();
        });
        await v.AuthorizeAsync("IDT-1.c", "s1");
        await Task.Delay(TimeSpan.FromMilliseconds(20));
        await v.AuthorizeAsync("IDT-1.c", "s1");
        Assert.Equal(2, calls); // expired cache entry must trigger a fresh identity call
    }

    [Fact]
    public async Task AuthorizeFailOpenDoesNotCoverMissingToken()
    {
        var v = FakeValidator(new IdtValidation { Enabled = true, FailOpen = true }, null);
        // FailOpen must not bypass a missing token
        var e = await Denied(v.AuthorizeAsync("", "s1"));
        Assert.Equal(403, e.Status);
        Assert.Equal("MISSING_IDT", e.ErrorMessage);
        Assert.Equal(0, _unexpectedCalls);
    }

    [Fact]
    public async Task AuthorizeDeniedNotCached()
    {
        var calls = 0;
        var v = FakeValidator(new IdtValidation { Enabled = true, CacheTtl = Minute }, _ =>
        {
            calls++;
            return new ValidateResponse { Valid = true, FunctionGranted = false, Reason = "DENIED_FN" };
        });
        for (var i = 0; i < 2; i++)
        {
            var e = await Denied(v.AuthorizeAsync("IDT-1.c", "s1"));
            Assert.Equal(403, e.Status);
            Assert.Equal("DENIED_FN", e.ErrorMessage);
        }

        Assert.Equal(2, calls); // deny must not be cached
    }

    [Fact]
    public async Task AuthorizeInvalidTokenReason()
    {
        var v = FakeValidator(new IdtValidation { Enabled = true }, _ => new ValidateResponse { Valid = false, Reason = "TOKEN_REVOKED" });
        var e = await Denied(v.AuthorizeAsync("IDT-1.c", "s1"));
        Assert.Equal("TOKEN_REVOKED", e.ErrorMessage);
    }

    [Fact]
    public async Task AuthorizeIdentityErrorFailClosedThenOpen()
    {
        static ValidateResponse Boom(ValidateRequest req) => throw new TestException();

        // fail-closed
        var e = await Denied(FakeValidator(new IdtValidation { Enabled = true }, Boom).AuthorizeAsync("IDT-1.c", "s1"));
        Assert.Equal(403, e.Status);
        Assert.Equal("VALIDATE_ERROR", e.ErrorMessage);

        // fail-open: pass-through, unverified
        var id = await FakeValidator(new IdtValidation { Enabled = true, FailOpen = true }, Boom).AuthorizeAsync("IDT-1.c", "s1");
        Assert.False(id.Verified);
    }

    [Fact]
    public async Task AuthorizeGrantedButEmptyUserIDIsDenied()
    {
        var calls = 0;
        var v = FakeValidator(new IdtValidation { Enabled = true, CacheTtl = Minute }, _ =>
        {
            calls++;
            return Granted(userId: "  ");
        });

        // A deny is an exception here, so there is no identity that could be "verified" (Go
        // also checks the zero Identity returned next to the error).
        var e = await Denied(v.AuthorizeAsync("IDT-1.c", "s1"));
        Assert.Equal(403, e.Status);
        Assert.Equal("INVALID_IDENTITY", e.ErrorMessage);

        // Must not be cached: a second call re-consults identity.
        e = await Denied(v.AuthorizeAsync("IDT-1.c", "s1"));
        Assert.Equal("INVALID_IDENTITY", e.ErrorMessage);
        Assert.Equal(2, calls); // empty-userId deny must not be cached
        Assert.Equal(0, v.CacheCount);
    }

    [Fact]
    public async Task AuthorizeObserveOnlyEmptyUserIDPassesThroughUnverified()
    {
        var v = FakeValidator(new IdtValidation { Enabled = true, ObserveOnly = true }, _ => Granted(userId: ""));
        // observe-only must pass through unverified even with empty userId
        var id = await v.AuthorizeAsync("IDT-1.c", "s1");
        Assert.False(id.Verified);
    }

    [Fact]
    public async Task AuthorizeObserveOnlyNeverBlocks()
    {
        var v = FakeValidator(
            new IdtValidation { Enabled = true, ObserveOnly = true },
            _ => new ValidateResponse { Valid = true, FunctionGranted = false, Reason = "DENIED_FN" });
        // observe-only must pass through unverified
        var id = await v.AuthorizeAsync("IDT-1.c", "s1");
        Assert.False(id.Verified);
    }

    [Fact]
    public async Task AuthorizeObserveOnlyMissingHeaderPassesThroughUnverified()
    {
        var calls = 0;
        var v = FakeValidator(new IdtValidation { Enabled = true, ObserveOnly = true }, _ =>
        {
            calls++;
            return new ValidateResponse();
        });
        // observe-only missing header must pass through unverified
        var id = await v.AuthorizeAsync("", "s1");
        Assert.False(id.Verified);
        Assert.Equal("", id.Idt);
        Assert.Equal(0, calls); // observe-only missing header must not consult identity
    }

    [Fact]
    public async Task AuthorizeObserveOnlyNeverCachesAllows()
    {
        var calls = 0;
        var v = FakeValidator(new IdtValidation { Enabled = true, ObserveOnly = true, CacheTtl = Minute }, _ =>
        {
            calls++;
            return Granted();
        });
        for (var i = 0; i < 3; i++)
        {
            // observe-only allow must stay unverified pass-through
            var id = await v.AuthorizeAsync("IDT-1.c", "s1");
            Assert.False(id.Verified);
        }

        // observe-only must never cache an allow (undercounts rollout signal)
        Assert.Equal(3, calls);
    }

    [Fact]
    public void ValidationFromEnvDefaults()
    {
        var env = new Dictionary<string, string>
        {
            ["IDT_VALIDATION"] = "",
            ["NATS_IDENTITY_BASE_PATH"] = "",
            ["NATS_IDENTITY_VALIDATE_SUBJECT"] = "",
            ["IDT_VALIDATE_TIMEOUT_SECONDS"] = "",
            ["IDT_VALIDATE_CACHE_SECONDS"] = "",
        };
        var c = IdtValidation.FromEnvironment(env.GetValueOrDefault, _log);
        Assert.False(c.Enabled);
        Assert.Equal("trx.identityservice.validateInternalToken", c.Subject);
        Assert.Equal(TimeSpan.FromSeconds(5), c.Timeout);
        Assert.Equal(TimeSpan.FromSeconds(300), c.CacheTtl);

        env["IDT_VALIDATION"] = "true";
        env["IDT_VALIDATE_CACHE_SECONDS"] = "0";
        c = IdtValidation.FromEnvironment(env.GetValueOrDefault, _log);
        Assert.True(c.Enabled);
        Assert.Equal(TimeSpan.Zero, c.CacheTtl);
        Assert.Empty(_log.Lines);
    }

    [Fact]
    public void ValidationFromEnvWarnsOnUnparsableCache()
    {
        var env = new Dictionary<string, string>
        {
            ["IDT_VALIDATION"] = "",
            ["NATS_IDENTITY_BASE_PATH"] = "",
            ["NATS_IDENTITY_VALIDATE_SUBJECT"] = "",
            ["IDT_VALIDATE_TIMEOUT_SECONDS"] = "",
            ["IDT_VALIDATE_CACHE_SECONDS"] = "0s", // unparsable, not "0"
        };
        var c = IdtValidation.FromEnvironment(env.GetValueOrDefault, _log);

        // unparsable cache env must keep the default (behaviour unchanged; only a log line is added)
        Assert.Equal(IdtValidation.DefaultCacheTtl, c.CacheTtl);

        // The log line itself (the Go test does not capture it; the text is Go's).
        var line = Assert.Single(_log.Lines);
        Assert.Equal("IDT_METRIC event=config.warn var=IDT_VALIDATE_CACHE_SECONDS value=\"0s\" using_default=5m0s", line);
    }

    [Fact]
    public void IdPrefixCapsBothBranches()
    {
        var longToken = "IDT-" + new string('x', IdtValidator.IdPrefixLogMax + 50);
        Assert.Equal(IdtValidator.IdPrefixLogMax, IdtValidator.IdPrefix(longToken).Length); // dotless branch
        Assert.Equal(IdtValidator.IdPrefixLogMax, IdtValidator.IdPrefix(longToken + ".cipher").Length); // dotted branch
    }

    [Fact]
    public async Task AuthorizeFreshSkipsCacheRead()
    {
        var calls = 0;
        var valid = true;
        var v = FakeValidator(new IdtValidation { Enabled = true, CacheTtl = Minute }, _ =>
        {
            calls++;
            return valid ? Granted() : new ValidateResponse { Valid = false, Reason = "TOKEN_REVOKED" };
        });
        await v.AuthorizeAsync("IDT-1.c", "s1"); // first allow
        valid = false;

        // Authorize must still use the cache
        await v.AuthorizeAsync("IDT-1.c", "s1");
        Assert.Equal(1, calls);

        // AuthorizeFresh must ask identity again
        var e = await Denied(v.AuthorizeFreshAsync("IDT-1.c", "s1"));
        Assert.Equal("TOKEN_REVOKED", e.ErrorMessage);
        Assert.Equal(2, calls);
    }

    // Observe-only deny: Authorize keeps its admission shape (no ids), while AuthorizeFresh
    // (renew) returns the ids identity resolved so the caller can be compared. Never verified
    // either way.
    [Fact]
    public async Task AuthorizeFreshObserveDenyCarriesIDs()
    {
        var v = FakeValidator(
            new IdtValidation { Enabled = true, ObserveOnly = true },
            _ => new ValidateResponse { Valid = true, FunctionGranted = false, UserId = "u1", AccountId = "a1", Reason = "DENIED_FN" });

        var id = await v.AuthorizeAsync("IDT-1.c", "s1");
        Assert.False(id.Verified);
        Assert.Equal("", id.UserId);
        Assert.Equal("", id.AccountId);
        Assert.Equal("IDT-1.c", id.Idt);

        id = await v.AuthorizeFreshAsync("IDT-1.c", "s1");
        Assert.False(id.Verified);
        Assert.Equal("u1", id.UserId);
        Assert.Equal("a1", id.AccountId);
        Assert.Equal("IDT-1.c", id.Idt);
    }

    // Go: errTest ("boom").
    private sealed class TestException() : Exception("boom");

    /// <summary>Keeps every formatted log line (Go: a captured <c>log.Logger</c>).</summary>
    private sealed class RecordingLogger : ILogger
    {
        private readonly List<string> _lines = [];

        public IReadOnlyList<string> Lines
        {
            get
            {
                lock (_lines)
                {
                    return [.. _lines];
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Add(formatter(state, exception));
            }
        }
    }
}
