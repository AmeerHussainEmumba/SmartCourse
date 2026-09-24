# internal/platform — shared kernel

Everything every domain module is allowed to depend on (RFC §4.3). Nothing in here knows about courses, enrollments, or students — if a change here needs to know what a "course" is, it belongs in a domain module instead.

## What's here

| Package | Owns |
|---|---|
| `config` | Fail-fast env config. No secret has a default — a missing `JWT_SIGNING_KEY` stops the process at boot, not at first use. |
| `logging` | Structured `slog` JSON logging, request-scoped via context. |
| `db` | The PostgreSQL connection (GORM + pgx), pool sizing, readiness ping. |
| `cache` | The Redis connection. |
| `errors` | The `*AppError` type every handler returns — stable `code`, safe `message`, internal detail kept out of the response. |
| `httpx` | Request ID middleware, error-response writing, DTO binding, keyset-pagination cursor helpers. |
| `auth` | argon2id hashing, JWT issue/verify, opaque single-use tokens (refresh/reset/verify), and the three-layer middleware (`RequireAuth`, `RequireRole`, `RequireFreshPrivilege`). |
| `ratelimit` | GCRA rate limiting (ADR-0020) via `go-redis/redis_rate` — per-IP middleware and a per-identity `Allow` call for checks middleware can't express (e.g. per-account login limits). |
| `outbox` | The only path from a state change to a published event (ADR-0006). |
| `relay` | The bounded worker pool that drains `outbox` and dispatches to registered handlers (ADR-0021) — goroutines, a channel, `sync.WaitGroup`-drained shutdown, `sync.Mutex`-guarded claim tracking, `sync/atomic` counters, all against real work. See its own README. |
| `router` | Assembles the Gin engine: middleware chain, health endpoints. |

## Why this exists as a separate layer

Every domain module (`internal/identity`, `internal/course`, ...) follows the same `handler.go` / `service.go` / `repository.go` / `dto.go` / `domain.go` shape. That uniformity only holds if the cross-cutting stuff — how an error becomes JSON, how a password gets hashed, how a token is validated — is written once here and reused, not reinvented per module. `.golangci.yml`'s `depguard` rules allow every module to import `internal/platform/**` while blocking modules from importing each other.

## Auth: three layers, not one

`internal/platform/auth` implements three distinct checks, and a module wiring a route needs to pick the right combination rather than reaching for whichever one is habit:

1. **`RequireAuth`** — is there a valid, unexpired, correctly-signed token. Attaches the caller's identity (user ID, role, token version) to the request's `context.Context`, not to `gin.Context` — so service-layer code can read it via `auth.FromContext(ctx)` without any dependency on HTTP.
2. **`RequireRole("instructor")`** — coarse, route-level. Cannot check *which* course; that's not known yet at middleware time.
3. **`RequireFreshPrivilege`** — narrow. Only on admin routes and anything that mutates another user's role/access. Checks the token's embedded version against Redis so a just-demoted user can't keep using admin routes for the rest of their token's 15-minute life. See ADR-0015's addendum for the full reasoning — this is not a general per-request check, and adding it to a route that doesn't need it reintroduces the Redis round-trip the whole design avoids.

A fourth check — **ownership** ("this student owns *this* enrollment") — is not here. It lives in each module's `service.go`, because only the service has loaded the resource to compare against. See `internal/enrollment/README.md` for a worked example.

## Testing

Unit tests for `auth` (password hashing round-trips, JWT signature/expiry/alg-confusion rejection, token generation) live alongside the code (`*_test.go`). No database or Redis needed — see `docs/guides/testing.md`.
