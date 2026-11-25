# libs — shared kernel

Everything every service is allowed to depend on (RFC §4.3). Services import only `libs/*`; no service imports another service's code (ADR-0033, `docs/plans/microservices-split.md`). Nothing in here knows about courses, enrollments, or students — if a change here needs to know what a "course" is, it belongs in a domain module instead.

This used to be one package tree, `internal/platform/`. Each directory here is now its **own Go module** (`github.com/AmeerHussainEmumba/SmartCourse/libs/<name>`, its own `go.mod`), so that different people can own different libraries and a library only declares the third-party dependencies it actually uses. See `docs/plans/module-split.md`.

## How the modules fit together

- Dependencies between libraries form a DAG with no cycles. Non-test imports today (verify with `grep` or `scripts/check-module-graph.sh`; `go.mod` files also list test-only edges such as `testenv`): `httpx → errors`, `router → httpx`, `auth → config, errors, httpx`, `ratelimit → errors, httpx`, `idempotency → errors, httpx`, `cache → config`, `db → config`, `infra → auth, cache, config, db, outbox, router, telemetry` (and its `infra/observe` sub-package), `runtime → config, infra/observe, logging`, `temporal → config, telemetry`, `relay → logging`, `asynqx → logging, telemetry`, `eventbus → config, events, kafka, logging, relay, telemetry`, `jobs → asynqx, eventbus, idempotency, outbox`, `testenv → config, db`. `errors`, `config`, `logging`, `outbox`, `events`, `kafka`, `mongostore`, `telemetry` and `contracts/publishing` import no sibling. `testenv` is imported only by tests.
- A library's `go.mod` lists a `replace` for **every** sibling library it depends on, including indirect ones, because Go does not inherit `replace` directives from dependencies. When you add an import of another library, add its `require` and `replace` lines (and those of anything it depends on) to your module's `go.mod`, then run `go mod tidy` with `GOWORK=off`.
- `go.work` at the repo root lists every module for local development. CI and the Dockerfiles build with `GOWORK=off`, so a missing `replace` fails there even if it works on your machine.
- `go test ./...` does not cross module boundaries. Use `make test` (or `scripts/for-each-module.sh go test ./...`), which runs the command in every module.

## What's here

| Package | Owns | Used by (service binaries) |
|---|---|---|
| `config` | Fail-fast env config. No secret has a default — a missing `JWT_SIGNING_KEY` stops the process at boot, not at first use. `LoadWith(Requirements)` lets a binary declare which groups it needs. | all eleven |
| `logging` | Structured `slog` JSON logging, request-scoped via context. | all but `identity-admin` (through `runtime`) |
| `runtime` | Process bootstrap for every long-running binary: `runtime.Main`, `WithDependencies`, `WithConfig`, `WithOutbox`, signals, the internal listener (`/metrics`, `/health/live`, `/health/ready`), graceful shutdown. See its README. | all ten long-running binaries (not `identity-admin`) |
| `db` | The PostgreSQL connection (GORM + pgx), pool sizing, readiness ping. | via `infra` |
| `cache` | The Redis connection. | via `infra`; `audit-api` |
| `errors` | The `*AppError` type every handler returns — stable `code`, safe `message`, internal detail kept out of the response. | the four APIs |
| `httpx` | Request ID middleware, error-response writing, DTO binding, keyset-pagination cursor helpers. | the four APIs |
| `idempotency` | `Idempotency-Key` middleware for non-idempotent POST routes, against the service's own `idempotency_keys` table (DR-023). | `course-api`; pruned by `course-worker` |
| `auth` | argon2id hashing, JWT issue/verify, opaque single-use tokens (refresh/reset/verify), and the three-layer middleware (`RequireAuth`, `RequireRole`, `RequireFreshPrivilege`). See its own README. | the four APIs |
| `ratelimit` | GCRA rate limiting (ADR-0020) via `go-redis/redis_rate` — per-IP middleware and a per-identity `Allow` call. | `identity-api`, `course-api` |
| `outbox` | The only path from a state change to a published event (ADR-0006), into the service's own `outbox` table. See its own README. | `identity-api`, `course-api` (writers); `identity-worker`, `course-worker` (prune) |
| `relay` | The bounded worker pool that drains a service's outbox and publishes each row through a `Publisher` (ADR-0021, ADR-0029, ADR-0033), plus the `Handler`/`Deliver` types consumers use. One publish-only relay per producing service. See its own README. | `identity-worker`, `course-worker` (run it); the other workers and `audit-api` use its types |
| `events` | Event contracts: `.proto` files, generated Go code, the envelope, the Schema Registry codec and `register-schemas`. | via `eventbus`; `audit-worker`, `audit-api` |
| `kafka` | The pure-Go Kafka client layer (franz-go, ADR-0030): idempotent producer, consumer-group runner, retry budget and `OnExhausted` hook, a broker `Ping`/`HealthCheck`, and `kafka/topics` (event topics and one `retry.<consumer>` topic each, idempotent `Ensure`). Used through `eventbus`. | `identity-worker`, `course-worker`, `analytics-worker`, `notification-worker`, `audit-worker`, `audit-api` |
| `eventbus` | The Kafka side of the event transport shared by every service (ADR-0029): publisher, transactional and stream consumer runners, codec, topic routing, `processed_events` pruning, replay-by-republish. It was `services/worker/transport`. | the same six binaries as `kafka` |
| `jobs` | Scheduled-job plumbing on Asynq: `Job`, `Register`, and the shared prunes (`PruneOutbox`, `PruneIdempotencyKeys`, `PruneProcessedEvents`). | `identity-worker`, `course-worker`, `analytics-worker`, `notification-worker` |
| `asynqx` | Asynq wrapper: idempotent enqueue, bounded per-queue pools, a cron scheduler, an inspector, and a per-service queue namespace so services sharing one Redis never consume each other's tasks. | the same four workers |
| `mongostore` | MongoDB archive, audit log and failed-event parking (ADR-0031). See its own README. | `audit-worker`, `audit-api` (store); `course-worker`, `notification-worker`, `analytics-worker` (dead-letter sink); `analytics-api` (metric) |
| `telemetry` | OpenTelemetry tracing, W3C trace-context for the envelope, Gin middleware and Prometheus metrics (ADR-0032). | every binary, wired by `runtime`/`infra` |
| `router` | Assembles the Gin engine: middleware chain, health endpoints. | the four APIs |
| `infra` | Opens the Postgres and Redis clients (`Connect`), builds readiness checks and the observability set-up (`infra/observe` has no database code). Imports no domain module. | every binary with a database (all but `publishing-worker` and the audit pair) |
| `temporal` | Builds the lazy Temporal client. Separate from `infra` so binaries that never use Temporal never link the SDK. | `course-api`, `course-worker`, `publishing-worker` |
| `contracts/publishing` | The course-publish workflow contract: workflow name, the two task queues, activity names, DTOs. Plain Go, no Temporal import (ADR-0035). | `course-api`, `course-worker`, `publishing-worker` (through `services/course` and `services/publishing`) |
| `testenv` | Real Postgres (with the per-service roles), Redis, Kafka and MongoDB containers for integration tests: `Setup`, `Env.ServiceDB`, `ServiceDSN`, `Qualify`, `Env.Owner`. Test-only, no binary imports it. See its README. | tests only (services, libs, `test/e2e`) |

## Why this exists as a separate layer

Every domain module (`services/identity/identity`, `services/course/course`, ...) follows the same `handler.go` / `service.go` / `repository.go` / `dto.go` / `domain.go` shape. That uniformity only holds if the cross-cutting stuff — how an error becomes JSON, how a password gets hashed, how a token is validated — is written once here and reused, not reinvented per module. `.golangci.yml`'s `depguard` rules allow every module to import `libs/**` while blocking modules from importing each other, and `scripts/check-module-graph.sh` fails if any service requires another service's module.

## Auth: three layers, not one

`libs/auth` implements three distinct checks, and a module wiring a route needs to pick the right combination rather than reaching for whichever one is habit:

1. **`RequireAuth`** — is there a valid, unexpired, correctly-signed token. Attaches the caller's identity (user ID, role, token version) to the request's `context.Context`, not to `gin.Context` — so service-layer code can read it via `auth.FromContext(ctx)` without any dependency on HTTP.
2. **`RequireRole("instructor")`** — coarse, route-level. Cannot check *which* course; that's not known yet at middleware time.
3. **`RequireFreshPrivilege`** — narrow. Only on admin routes and anything that mutates another user's role/access. Checks the token's embedded version against Redis so a just-demoted user can't keep using admin routes for the rest of their token's 15-minute life. See ADR-0015's addendum for the full reasoning — this is not a general per-request check, and adding it to a route that doesn't need it reintroduces the Redis round-trip the whole design avoids.

A fourth check — **ownership** ("this student owns *this* enrollment") — is not here. It lives in each module's `service.go`, because only the service has loaded the resource to compare against. See `services/course/enrollment/README.md` for a worked example.

## Testing

Unit tests for `auth` (password hashing round-trips, JWT signature/expiry/alg-confusion rejection, token generation) live alongside the code (`*_test.go`). No database or Redis needed — see `docs/guides/testing.md`.
