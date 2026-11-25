# libs/cache

The Redis connection. Redis holds cache, hot counters, rate-limiter state, and (eventually) Asynq's queues — never a source of truth (RFC §5.1). Every key written anywhere in this codebase must be reconstructible from PostgreSQL.

**Used by** `libs/infra` (`Connect` opens Redis for every binary that has dependencies: identity, course, analytics and notification binaries) and directly by `audit-api` (token-version checks). `publishing-worker` and `audit-worker` do not use Redis.

`Open` connects and pings with a bounded timeout, the same fail-fast-at-boot shape as `libs/db.Open` — a misconfigured `REDIS_ADDR` fails loudly at startup rather than surfacing as a mysterious failure on first use.

## Who uses it, and for what

- `libs/auth` — refresh-token rotation state, `RequireFreshPrivilege`'s token-version cache (ADR-0015 addendum).
- `libs/ratelimit` — GCRA rate-limit counters (ADR-0020), the one piece of shared state across replicas that must never live behind a Go `sync.Mutex` (RFC §10.3).
- `services/course/course` — the version-scoped course-detail cache `GetPublishedDetail` reads through and the publish workflow's `WarmCache` activity writes (ADR-0025).

## Testing

No dedicated test file — `Open` is exercised indirectly by every test that calls `libs/testenv.Setup`, which connects through it to a real, disposable Redis container.

## Tracing

`EnableTracing(client)` adds a go-redis hook: one CLIENT child span per command (or per pipeline) run under a request span, named after the command, never the key or arguments. `redis.Nil` is not an error. Hand-written instead of `redisotel` to avoid another module; `libs/infra` calls it only when tracing is on.
