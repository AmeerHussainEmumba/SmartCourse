# internal/platform/ratelimit

GCRA rate limiting (ADR-0020) via `go-redis/redis_rate` — one atomic Redis call per check, replacing the naive `INCR`+`EXPIRE` fixed-window approach that allows roughly double the intended rate for traffic straddling a window boundary. That specific flaw is why this got a dedicated ADR rather than being three lines of ad-hoc Redis code.

This is the concrete case RFC §10.3 names as the one kind of shared state that must never live behind a `sync.Mutex`/`RWMutex`: rate-limit state is shared across every API replica, not one process, so it lives in Redis.

## What's here

- **`Limiter.Allow(ctx, key, limit)`** — the low-level check, exposed directly (not only through `Middleware`) so service-layer code can apply a limit keyed on something not knowable until the request body is parsed — per-account login attempts, where the account is the email in the body, not something route-level middleware can see. **Fails open** on a Redis error (RFC §13.4): a Redis outage degrades to "no rate limiting," never to "nobody can authenticate."
- **`Limiter.Middleware(keyPrefix, limit)`** — per-client-IP limiting for a whole route group.
- **`AccountKey(scope, identity)`** — the shared key-format convention (`ratelimit:{scope}:acct:{identity}`, RFC §5.7) so callers building a per-account key stay consistent with `Middleware`'s own IP-keyed format without duplicating the prefix logic.

## Testing

No dedicated test file for this package's own logic (`Limiter`/`Middleware`/the fail-open behavior) in isolation — an honest gap, not a silent one. What IS tested: `internal/identity`'s integration suite proves the end-to-end *effect* (`TestLogin_PerAccountRateLimit_BlocksAfterThreshold`, `docs/guides/testing.md`), which is real coverage of the outcome a caller depends on, just not of this package's internals independent of a specific route wiring it in a specific way.
