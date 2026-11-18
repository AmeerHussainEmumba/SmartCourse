# libs — shared kernel

Everything every domain module is allowed to depend on (RFC §4.3). Nothing in here knows about courses, enrollments, or students — if a change here needs to know what a "course" is, it belongs in a domain module instead.

This used to be one package tree, `internal/platform/`. Each directory here is now its **own Go module** (`github.com/AmeerHussainEmumba/SmartCourse/libs/<name>`, its own `go.mod`), so that different people can own different libraries and a library only declares the third-party dependencies it actually uses. See `docs/plans/module-split.md`.

## How the modules fit together

- Dependencies between libraries form a DAG with no cycles: `httpx → errors`, `router → httpx`, `auth → config, errors, httpx`, `ratelimit → errors, httpx`, `cache → config`, `db → config`, `infra → cache, config, db, router`, `temporal → config`. `errors`, `config`, `logging`, `outbox` and `relay` have no sibling dependencies. `testenv` (test-only) depends on `config` and `db`.
- A library's `go.mod` lists a `replace` for **every** sibling library it depends on, including indirect ones, because Go does not inherit `replace` directives from dependencies. When you add an import of another library, add its `require` and `replace` lines (and those of anything it depends on) to your module's `go.mod`, then run `go mod tidy` with `GOWORK=off`.
- `go.work` at the repo root lists every module for local development. CI and the Dockerfiles build with `GOWORK=off`, so a missing `replace` fails there even if it works on your machine.
- `go test ./...` does not cross module boundaries. Use `make test` (or `scripts/for-each-module.sh go test ./...`), which runs the command in every module.

## What's here

| Package | Owns |
|---|---|
| `config` | Fail-fast env config. No secret has a default — a missing `JWT_SIGNING_KEY` stops the process at boot, not at first use. |
| `logging` | Structured `slog` JSON logging, request-scoped via context. |
| `db` | The PostgreSQL connection (GORM + pgx), pool sizing, readiness ping. |
| `cache` | The Redis connection. |
| `errors` | The `*AppError` type every handler returns — stable `code`, safe `message`, internal detail kept out of the response. |
| `httpx` | Request ID middleware, error-response writing, DTO binding, keyset-pagination cursor helpers. |
| `auth` | argon2id hashing, JWT issue/verify, opaque single-use tokens (refresh/reset/verify), and the three-layer middleware (`RequireAuth`, `RequireRole`, `RequireFreshPrivilege`). See its own README. |
| `ratelimit` | GCRA rate limiting (ADR-0020) via `go-redis/redis_rate` — per-IP middleware and a per-identity `Allow` call for checks middleware can't express (e.g. per-account login limits). |
| `outbox` | The only path from a state change to a published event (ADR-0006). See its own README. |
| `relay` | The bounded worker pool that drains `outbox` and dispatches to registered handlers (ADR-0021) — goroutines, a channel, `sync.WaitGroup`-drained shutdown, `sync.Mutex`-guarded claim tracking, `sync/atomic` counters, all against real work. See its own README. |
| `router` | Assembles the Gin engine: middleware chain, health endpoints. |
| `infra` | Opens the shared Postgres and Redis clients (`Connect`) and builds the `/health/ready` checks. Imports no domain module. |
| `temporal` | Builds the lazy Temporal client. Separate from `infra` so identity-api and analytics-api never link the Temporal SDK. |

## Why this exists as a separate layer

Every domain module (`services/identity/identity`, `services/course/course`, ...) follows the same `handler.go` / `service.go` / `repository.go` / `dto.go` / `domain.go` shape. That uniformity only holds if the cross-cutting stuff — how an error becomes JSON, how a password gets hashed, how a token is validated — is written once here and reused, not reinvented per module. `.golangci.yml`'s `depguard` rules allow every module to import `libs/**` while blocking modules from importing each other.

## Auth: three layers, not one

`libs/auth` implements three distinct checks, and a module wiring a route needs to pick the right combination rather than reaching for whichever one is habit:

1. **`RequireAuth`** — is there a valid, unexpired, correctly-signed token. Attaches the caller's identity (user ID, role, token version) to the request's `context.Context`, not to `gin.Context` — so service-layer code can read it via `auth.FromContext(ctx)` without any dependency on HTTP.
2. **`RequireRole("instructor")`** — coarse, route-level. Cannot check *which* course; that's not known yet at middleware time.
3. **`RequireFreshPrivilege`** — narrow. Only on admin routes and anything that mutates another user's role/access. Checks the token's embedded version against Redis so a just-demoted user can't keep using admin routes for the rest of their token's 15-minute life. See ADR-0015's addendum for the full reasoning — this is not a general per-request check, and adding it to a route that doesn't need it reintroduces the Redis round-trip the whole design avoids.

A fourth check — **ownership** ("this student owns *this* enrollment") — is not here. It lives in each module's `service.go`, because only the service has loaded the resource to compare against. See `services/course/enrollment/README.md` for a worked example.

## Testing

Unit tests for `auth` (password hashing round-trips, JWT signature/expiry/alg-confusion rejection, token generation) live alongside the code (`*_test.go`). No database or Redis needed — see `docs/guides/testing.md`.
