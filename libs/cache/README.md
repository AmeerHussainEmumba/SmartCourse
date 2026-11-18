# libs/cache

The Redis connection. Redis holds cache, hot counters, rate-limiter state, and (eventually) Asynq's queues — never a source of truth (RFC §5.1). Every key written anywhere in this codebase must be reconstructible from PostgreSQL.

`Open` connects and pings with a bounded timeout, the same fail-fast-at-boot shape as `libs/db.Open` — a misconfigured `REDIS_ADDR` fails loudly at startup rather than surfacing as a mysterious failure on first use.

## Who uses it, and for what

- `libs/auth` — refresh-token rotation state, `RequireFreshPrivilege`'s token-version cache (ADR-0015 addendum).
- `libs/ratelimit` — GCRA rate-limit counters (ADR-0020), the one piece of shared state across replicas that must never live behind a Go `sync.Mutex` (RFC §10.3).
- `services/course/course` — the version-scoped course-detail cache `GetPublishedDetail` reads through and the publish workflow's `WarmCache` activity writes (ADR-0025).

## Testing

No dedicated test file — `Open` is exercised indirectly by every test that calls `libs/testenv.Setup`, which connects through it to a real, disposable Redis container.
