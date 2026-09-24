# Getting started

You're joining a Go backend for a course-delivery platform. This doc gets you from a fresh laptop to a running system, tells you what to read before you touch code, and explains enough of the architecture that the rest of the docs make sense. It assumes nothing about what you've already read.

If something here is wrong or goes stale, fix it in the same change that made it wrong — this file rotting is exactly the failure mode it exists to prevent.

---

## 1. What you need to know before starting

You do **not** need prior Go experience to be productive here, but you do need to spend about a day getting oriented before making changes with confidence. Two different kinds of prerequisite knowledge:

### If you're new to Go

Go is different from Java/C#/Python/TypeScript in a few ways that matter immediately:

- **Errors are values, not exceptions.** Every function that can fail returns `(result, error)`, and the caller checks `err != nil` explicitly, right there, every time. There's no `try/catch` propagating silently past code that didn't expect it.
- **Interfaces are satisfied implicitly, and declared by the consumer, not the implementer.** A type doesn't say "I implement X" — if it has the right methods, it satisfies the interface automatically. This is the idiom behind how modules avoid depending on each other (§3 below).
- **`context.Context` is threaded through almost every function as the first parameter.** It carries cancellation, deadlines, and a few request-scoped values (who's calling, trace/request IDs) — never a general-purpose bag of stuff.
- **Goroutines are cheap to start; leaking them is not.** Every goroutine needs a clear exit condition.
- **No inheritance** — composition (embedding) instead.

Recommended: [A Tour of Go](https://go.dev/tour/), then Effective Go's sections on errors and concurrency. About a day, and you'll be able to read this codebase comfortably.

### Concepts worth understanding regardless of Go experience

These five ideas explain *why* the code is shaped the way it is. Misunderstanding any of them produces changes that look correct in review and are not:

1. **Why the transactional outbox exists.** Committing a database change and then separately publishing an event to a message queue is a *dual write* — if the process dies between the two steps, the database says one thing happened and nobody else ever hears about it. The fix: write the event to a database table (`outbox`) in the *same transaction* as the state change. See `docs/adr/smartcourse-adr.md` ADR-0006.
2. **Why consumers of events must be idempotent.** The outbox pattern above guarantees an event is never *lost*, but it can be delivered more than once (a relay might publish successfully and then crash before marking the row done). Every downstream consumer has to handle "I've already seen this" without double-counting.
3. **Which data is strongly consistent, and which is allowed to lag.** Anything a user can assert as fact about their own account ("I'm enrolled," "I completed that lesson") is transactionally guaranteed and correct immediately. Anything derived (a dashboard count, a search index entry) is allowed to be a few seconds stale, and is always rebuildable from the source of truth. Conflating the two is a real design mistake, not a style issue.
4. **Why `sync.Mutex` doesn't protect what you might think it protects.** This service runs as multiple stateless replicas. A `sync.Mutex` only guards state *within one process* — it does nothing for state shared *across* replicas. Anything that needs to be correct across replicas (a hot counter, a rate limit) lives in Redis or Postgres, not behind a Go mutex. This is the most common category of subtle bug available in a horizontally-scaled Go service, and it compiles and passes the race detector while being wrong.
5. **Why a database row lock (`SELECT ... FOR UPDATE`) is used for enrollment limits instead of an application-level check.** Two students racing for the last seat in a course can both read "49 of 50 seats taken," both pass the check, and both enroll — silently violating the limit, with no error anywhere, and no functional test that would ever catch it. The fix is a lock that makes the two requests serialize on the database row. This is proven, not just asserted, by `internal/enrollment/service_integration_test.go`'s concurrency test — read it once you're set up; it's the best single file for understanding why this codebase tests the way it does.

---

## 2. Setting up on your laptop

### Prerequisites

- **Go 1.25+** — check with `go version`.
- **Docker** (with Compose v2, i.e. `docker compose`, not the old standalone `docker-compose`) — check with `docker compose version`.
- That's it to get the core system running. Kafka, Temporal, MongoDB, and the observability stack come later and aren't needed yet — see §5.

### Steps

```bash
git clone <repo-url> SmartCourse && cd SmartCourse

# 1. Configuration — copy the template and fill in real values.
#    Nothing here has a working default; a missing value fails startup
#    loudly rather than silently using something insecure.
cp env.example .env
#    At minimum, generate a real signing key:
#    openssl rand -base64 48
#    and put it in JWT_SIGNING_KEY in .env.

# 2. Start the minimum infrastructure: Postgres, Redis, MinIO.
make dev-core

# 3. Apply the schema.
make migrate-up

# 4. Run the API.
make run-api
```

Check it's alive:

```bash
curl localhost:8080/health/live     # {"status":"live"}
curl localhost:8080/health/ready    # {"status":"ready"} once Postgres/Redis are reachable
```

Try the real thing:

```bash
curl -X POST localhost:8080/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"you@example.com","password":"correct-horse-battery","full_name":"Your Name"}'
```

You should get back an access token, a refresh token, and your user record. From here, `api/openapi.yaml` documents every endpoint, and each module's README (`internal/<module>/README.md`) explains the design behind it.

### If a port is already taken

`docker-compose.yml` intentionally maps Postgres to host port **5433** (not 5432) and Redis to **6380** (not 6379), specifically to avoid colliding with other projects' containers that might already be running on your machine. If you still hit a port conflict, check `lsof -nP -iTCP:<port> -sTCP:LISTEN` for what's already bound, and either stop it or remap the port in `docker-compose.yml` and `env.example` together (they have to stay in sync).

### Stopping / resetting

```bash
make dev-down                 # stop containers, keep data
docker compose down -v        # stop and wipe volumes — full reset, including your local Postgres data
```

---

## 3. Running tests

Two tiers, and the difference matters:

```bash
make test              # unit tests — fast, no Docker needed
make test-race         # unit tests with the race detector (always on in CI)
make test-integration  # integration + concurrency tests — needs Docker, spins up real Postgres/Redis containers per test
```

**Read `docs/guides/testing.md` before writing a new test.** It indexes every existing test — what it checks and why — and explains the two tiers in depth. The short version: this project doesn't mock the database. Integration tests run against real, disposable Postgres containers (`testcontainers-go`) because constraint violations, transaction behavior, and lock contention are precisely what needs verifying, and a mock only proves your code called the method you told it to call.

If you want to see what a real concurrency bug looks like when it's *not* caught: read `internal/enrollment/service_integration_test.go`'s `TestEnroll_SeatLimit_NeverExceededUnderConcurrency`. It's referenced constantly in this codebase's docs for a reason.

---

## 4. The architecture, briefly

**PostgreSQL is the single source of truth.** Redis and (eventually) MongoDB hold only derived, rebuildable data — never something a user's correctness depends on.

**One Go binary, two run modes.** `--mode=api` serves HTTP. `--mode=worker` will run the outbox relay, Kafka consumers, and background jobs once those exist (see §5 — they don't yet). Because it's one build, the API and the workers can never drift apart on a shared code change.

**Eight domain modules, one shared kernel.** Each module (`internal/identity`, `internal/course`, `internal/enrollment`, `internal/progress`, and four not yet built) owns one business capability and follows the same internal shape: `handler.go` (HTTP), `service.go` (business logic and transactions), `repository.go` (the only file touching the database directly), `dto.go` (API request/response types), `domain.go` (entities). None of them import each other's internals directly — enforced by a lint rule, not just convention. `internal/README.md` has the full module map.

**State changes now, side effects later.** A request that changes something (enrolling, publishing a course) commits to Postgres and returns immediately. Anything that doesn't need to block the caller — updating a search index, sending a notification, recalculating analytics — happens asynchronously via the outbox pattern (§1.1 above). This is *why* enrollment feels instant even though "enrolling" conceptually triggers several downstream effects.

**Full diagrams and the complete reasoning:** `docs/rfc/smartcourse-rfc.md`. Skim §3 (consistency model) and §4 (architecture overview) — those two sections are what everything else is derived from.

---

## 5. What's actually built right now

Don't assume the RFC describes the current state of the code — it describes the target design, and marks what's implemented. As of now:

**Built, tested, running:** the full database schema; the shared kernel (auth, config, error handling, rate limiting, the outbox writer); the `identity`, `course`, `enrollment`, and `progress` modules, each with a working HTTP API and integration tests against real infrastructure; **the outbox relay** (`internal/platform/relay`) — a real bounded worker pool that drains events and dispatches them to handlers, proven by its own concurrency tests, not just written; and `internal/analytics`, the relay's first handler, which is why `GET /analytics/platform` returns real numbers today instead of zeros. Course publishing's validation step also runs genuinely concurrently (`errgroup`), not just sequentially.

This matters for where you look for examples: if you want to see this codebase's concurrency patterns in production code (not just in a test file), read `internal/platform/relay/` and `internal/course/service.go`'s `runPublishChecks`, not just the RFC's description of them.

**Not built yet, on purpose, not a gap:** the Temporal-based publishing workflow (durable, crash-recoverable orchestration — currently synchronous with real concurrent validation but no crash recovery mid-publish; see `internal/course/service.go`'s `Publish` method, which explicitly marks where Temporal replaces it), Kafka and Schema Registry (the relay is a deliberate in-process bridge ahead of them — see ADR-0021), `search`/`notification`/`audit` modules (same relay-handler pattern as `analytics`, just not built yet), asset upload, and the observability stack (Prometheus/Grafana/Jaeger).

Full detail: `docs/rfc/smartcourse-rfc.md` §21.0.

---

## 6. Where to go next

Reading order, roughly in order of how much context each one assumes:

1. This document.
2. `docs/rfc/smartcourse-rfc.md` §3 and §4 — the consistency model and architecture overview everything else derives from.
3. `migrations/` in order — the schema *is* the domain model; the constraints encode the actual business rules (a partial unique index doing real work is more informative than a paragraph describing the rule it enforces).
4. `internal/enrollment/` end to end — the clearest illustration of a transaction, an outbox write, and the sync/async boundary in one small module.
5. `internal/platform/README.md` — the conventions every module builds on.
6. `docs/adr/smartcourse-adr.md` — *why* things are the way they are, including decisions that were reconsidered. Skim the index; read the ones relevant to whatever you're about to touch.
7. `docs/guides/testing.md` — before you write your first test.

## 7. Working agreements

- Every non-trivial decision gets an ADR (`docs/adr/`). "I thought about three options and picked one" is worth thirty seconds to record — it's the difference between a future contributor knowing something was reasoned versus wondering if it was an accident.
- Every schema change is a migration with a tested down path (`migrations/README.md`).
- Every module you touch should have a concurrency test if it touches shared state, and an integration test if it touches the database. If you're not sure which tier a new test belongs in, `docs/guides/testing.md` has the answer.
- If your change adds something the original project brief didn't ask for, or reinterprets something it left ambiguous, log it in `docs/rfc/differences-from-requirements.md` — that's the mechanism that keeps "we made a reasonable judgment call" distinguishable from "we quietly went off-spec."
- If `.golangci.yml`'s `depguard` check fails on your change, the dependency you added is wrong — restructure around it (usually: declare a narrow interface in the consuming module) rather than loosening the rule.
