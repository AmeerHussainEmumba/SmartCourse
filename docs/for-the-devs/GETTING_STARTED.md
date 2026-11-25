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
5. **Why a database row lock (`SELECT ... FOR UPDATE`) is used for enrollment limits instead of an application-level check.** Two students racing for the last seat in a course can both read "49 of 50 seats taken," both pass the check, and both enroll — silently violating the limit, with no error anywhere, and no functional test that would ever catch it. The fix is a lock that makes the two requests serialize on the database row. This is proven, not just asserted, by `services/course/enrollment/service_integration_test.go`'s concurrency test — read it once you're set up; it's the best single file for understanding why this codebase tests the way it does.

---

## 2. Setting up on your laptop

### Prerequisites

- **Go 1.26+** (the modules declare `go 1.26.5`; CI and the images use 1.26.11) — check with `go version`.
- **Docker** (with Compose v2, i.e. `docker compose`, not the old standalone `docker-compose`) — check with `docker compose version`.
- That's it to get the system running, including course publishing (M2: Temporal is part of `make dev-workflow`, the profile tier these steps use, not `dev-core`) and email (Mailpit, a local mail sink that every profile starts). Kafka, MongoDB and the observability stack are optional tiers (`make dev-events`, `make dev-observability`): the services run fine without them, in the default `EVENT_TRANSPORT=inprocess` mode. See "Optional tiers" in §2 and what is wired in §5.

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

# 2. Start infrastructure — Postgres, Redis, MinIO, and (since make seed
#    publishes real courses through a real Temporal workflow, M2) Temporal
#    itself. `dev-workflow` is a superset of `dev-core`, not a different
#    stack — everything `dev-core` starts is still here too.
make dev-workflow

# 3. Apply the schema.
make migrate-up

# 4. Populate realistic data — instructors, courses at every lifecycle
#    state, students with partial progress. Goes through the real
#    service layer (not raw SQL), so it also proves the core modules
#    (including the publish workflow) work together. Safe to re-run —
#    it's a no-op the second time. See tools/seed/README.md.
#    `make seed` runs on your host, not in a container, so it must reach
#    everything through the ports compose publishes on 127.0.0.1. The
#    Makefile guarantees that for Temporal: it always runs the seed with
#    TEMPORAL_HOST_PORT=localhost:7233, whatever .env says (a .env with the
#    compose-internal `temporal:7233` used to fail at the publish step), and
#    it rewrites POSTGRES_DSN / REDIS_ADDR / MINIO_ENDPOINT to localhost:5433 /
#    localhost:6380 / localhost:9002 only if they still name the
#    compose-internal postgres:5432 / redis:6379 / minio:9000 (it prints a
#    line when it does). Use `make seed SEED_TEMPORAL_HOST_PORT=host:port` if
#    your Temporal is elsewhere. The seed is a no-op once the admin user
#    exists, so a half-finished run needs the data reset before retrying.
make seed

# 5. Nothing left to do — `make dev-workflow` (step 2) already started the
#    application containers too (identity-api, course-api, analytics-api,
#    worker — they're in the `core` profile, which `workflow` includes —
#    plus Temporal and its UI). See "Running natively instead" below if
#    you want faster iteration than a Docker rebuild on every change.
```

Check it's alive (each HTTP service has its own `/health/ready`):

```bash
curl localhost:8090/health/ready    # identity-api
curl localhost:8091/health/ready    # course-api
curl localhost:8092/health/ready    # analytics-api
curl localhost:8093/health/ready    # worker admin surface
```

Try the real thing — log in as one of the accounts `make seed` just created (every seeded account shares the password printed at the end of that command). Auth lives in `identity-api`, port **8090**:

```bash
curl -X POST localhost:8090/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"student1@smartcourse.dev","password":"SeedPass123!"}'
```

Or register your own account the same way that login would otherwise have nothing to authenticate:

```bash
curl -X POST localhost:8090/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"you@example.com","password":"correct-horse-battery","full_name":"Your Name"}'
```

Register answers `202` with no tokens (the same answer whether or not the email exists, so it cannot be used to discover registered emails). Log in with the same credentials to get tokens. Either way, login gives you an access token, a refresh token, and a user record. Courses live in `course-api`, port **8091**:

```bash
curl localhost:8091/api/v1/courses
```

From here, `api/openapi.yaml` documents every endpoint (across all three services — it's one spec, not three, since the route paths and request/response shapes didn't change, only which process answers them), and each module's README (for example `services/course/course/README.md`) explains the design behind it.

### DB roles

Postgres has one database and four service schemas (`identity`, `course`, `analytics`, `notification`), and each schema has its own login role: `svc_identity`, `svc_course`, `svc_analytics`, `svc_notification`. A role can use only its own schema's tables and sequences, plus the `citext` / `pg_trgm` functions in `public`; every other service schema is closed to it (`permission denied`), and it cannot create tables. `smartcourse` (from `POSTGRES_DSN`) is the owner: it runs the migrations and `make seed`.

- **Passwords** come from `SVC_<SERVICE>_PASSWORD` in `.env` (dev values are in `env.example`; nothing is in a committed SQL file). `scripts/db-roles.sh` creates the roles and grants. It runs by itself the first time the compose Postgres volume is created (a script mounted into the container) and at the end of every `make migrate-up`; `make db-roles` runs it alone. It is safe to repeat. **If your Postgres volume already existed**, `make migrate-up` is what creates the roles.
- **The services do not use their roles yet.** `env.example` lists the per-service DSNs (`IDENTITY_POSTGRES_DSN` and so on) and search paths; the binaries still connect as the owner until they are switched over.
- **Migration order** is `platform identity course analytics notification cutover`. The `cutover` set moves the old shared `platform` tables' rows into each service's own `outbox`, `processed_events` and `idempotency_keys` and then drops the `platform` schema.
- **In tests**, `testenv.Setup(t)` returns `env.DB` as the owner (its search path is `public` only, so write `course.outbox`, not `outbox`), and `env.ServiceDB(t, "course")` connects as that service's role with search path `course,public`. Use `ServiceDB` for anything a service would do. See `libs/testenv/README.md`.
- **Name collisions:** `outbox`, `processed_events` and `idempotency_keys` now exist in more than one schema. A connection whose search path lists several service schemas silently uses the first one. Never build such a search path; qualify the name instead.

### What is listening where

Everything is published on `127.0.0.1` only.

| Port | What | Started by |
|---|---|---|
| 8090 / 8091 / 8092 | identity-api / course-api / analytics-api | `dev-core` and up |
| 8093 | worker admin surface: `/health/live`, `/health/ready`, failed-event listing and replay (admin token) | `dev-core` and up |
| 8089 | Swagger UI over `api/openapi.yaml` | `dev-core` and up |
| 8025 (web) / 1025 (SMTP) | Mailpit, the local mail sink: read the mail the system sends | `dev-core` and up |
| 5433 / 6380 | Postgres / Redis | `dev-core` and up |
| 9002 (S3) / 9003 (console) | MinIO | `dev-core` and up |
| 7233 / 8088 | Temporal / Temporal UI | `dev-workflow` and up |
| 29092 / 8081 / 27017 | Kafka (from the host) / Schema Registry / MongoDB | `dev-events` and up |
| 9090 / 3001 / 16686 | Prometheus / Grafana / Jaeger UI (OTLP on 4317 and 4318) | `dev-observability` and up |

### Optional tiers: Kafka, MongoDB, observability

The default stack runs with `EVENT_TRANSPORT=inprocess`: the relay runs the handlers itself and nothing needs Kafka or MongoDB. To try the Kafka path:

```bash
make dev-events                                   # adds Kafka, Schema Registry, MongoDB
(cd libs/events && go run ./cmd/register-schemas -registry http://localhost:8081)   # idempotent
# then give the worker, in .env: EVENT_TRANSPORT=kafka, KAFKA_BROKERS=kafka:9092,
# SCHEMA_REGISTRY_URL=http://schema-registry:8081, and MONGO_URI=mongodb://mongodb:27017
```

In kafka mode the worker refuses to start, with a clear error, if the registry is unreachable or a schema subject is missing. The audit archiver only runs in kafka mode and needs `MONGO_URI`. With `MONGO_URI` unset the worker still starts: archive and replay are simply unavailable. Switching a database that has already published events in-process to kafka mode does not replay them (see §5). The metrics and tracing wiring for `make dev-observability` is still being finished (§5).

### Running natively instead

`make dev-workflow` containerizes everything, including the app, which means a code change needs a rebuild to see. For faster iteration, stop the containerized app services (`docker compose stop identity-api course-api analytics-api worker` — leave Postgres/Redis/MinIO/Temporal running) and run natively instead, each in its own terminal:

```bash
make run-identity-api     # :8090
make run-course-api       # :8091
make run-analytics-api    # :8092
make run-worker           # no HTTP port
```

### Browsing and running the API without writing curl by hand

Two ways to explore `api/openapi.yaml` interactively, both reading the same file — neither is a separate thing to keep in sync:

- **In the browser:** `make dev-core` (step 2 above) also starts a Swagger UI container reading `api/openapi.yaml` live — **http://localhost:8089**. Every endpoint, with an "Execute" button that fires a real request against your local API. The spec lists all three services' base URLs in the server dropdown at the top (`localhost:8090` for auth/users, `:8091` for courses/enrollments/progress, `:8092` for analytics) — pick the one matching whatever endpoint you're trying, since each is a separate process. Click "Authorize" and paste an access token from the register/login call above to call anything that needs one.
- **In Postman:** File → Import → select `api/openapi.yaml` from this repo. Postman builds a full collection from it, with all three base URLs available as environment/collection variables. Set your access token as another variable, then reference it as a Bearer token on requests that need auth.

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

**Plain `go test ./...` at the repo root tests nothing.** The repo is a set of separate Go modules (§4), and Go does not cross module boundaries. These `make` targets run the command in every module for you (`scripts/for-each-module.sh`). To test one module while you work: `cd services/course && go test -race -tags=integration ./...`.

`make verify-deliverables` runs live checks against a running stack (it creates test users and courses in whatever database the stack uses, so run it against a disposable compose project, for example `docker compose -p sc-verify --profile core --profile workflow up -d --build`, then migrate and seed that project, and remove it with `down -v`). Rows for features that are not built yet print PENDING and do not fail the run; only PASS rows count as proven (`docs/plans/week-3-acceptance-matrix.md`). Integration tests start several containers each: on a busy machine a test can fail with `context deadline exceeded` while Docker starts a container. Rerun that single test before suspecting your code.

**Read `docs/guides/testing.md` before writing a new test.** It indexes every existing test — what it checks and why — and explains the two tiers in depth. The short version: this project doesn't mock the database. Integration tests run against real, disposable Postgres containers (`testcontainers-go`) because constraint violations, transaction behavior, and lock contention are precisely what needs verifying, and a mock only proves your code called the method you told it to call.

If you want to see what a real concurrency bug looks like when it's *not* caught: read `services/course/enrollment/service_integration_test.go`'s `TestEnroll_SeatLimit_NeverExceededUnderConcurrency`. It's referenced constantly in this codebase's docs for a reason.

---

## 4. The architecture, briefly

**PostgreSQL is the single source of truth.** Redis and (eventually) MongoDB hold only derived, rebuildable data — never something a user's correctness depends on.

**Four services, one repository and one database, each with its own schema (ADR-0022, ADR-0023, ADR-0024).** `services/identity/cmd/identity-api` serves auth/users from its own Postgres schema (`identity`). `services/course/cmd/course-api` serves courses, enrollments, progress and certificates together, sharing one schema (`course`) — kept in one process and one schema because enrollment locks the `courses` row in the same transaction it writes an enrollment row in (ADR-0012's seat-limit guarantee). `services/analytics/cmd/analytics-api` serves the read-only analytics dashboard from its own schema (`analytics`), fed entirely by events — it never queries `course` or `identity` directly. `services/worker/cmd/worker` runs everything asynchronous: the outbox relay (polling the shared `platform` schema every producer writes to), the Temporal worker for course publishing, the Asynq pools and scheduled jobs, the notification dispatcher (its own `notification` schema), and, when `EVENT_TRANSPORT=kafka`, one Kafka consumer group per handler. It also serves a small admin HTTP surface on port 8093 (health, and listing and replaying failed events). The four are built and deployed independently, and each binary wires only its own modules in its own `cmd/*/wire.go`. No foreign key or query crosses a schema boundary — a service that only needed another's data for display, not ownership, keeps its own small event-fed copy instead (e.g. `course.instructor_mirror`, so the course catalogue can show an instructor's name without reaching into `identity`). (This used to be one binary with a `--mode` flag, then two services, then four sharing one flat schema, then four each with their own — see ADR-0022 through ADR-0024 if you're curious why and when each change happened; it's a good example of a design decision getting revisited based on real feedback, which is worth seeing happen at least once.)

**Domain modules, one shared kernel.** The HTTP modules are `services/identity/identity`, `services/course/course`, `services/course/enrollment`, `services/course/progress`, `services/course/certificate` and `services/analytics/analytics`. The worker hosts the asynchronous ones: `services/worker/notification` (dispatcher and mail provider), `services/worker/archive` (the audit archiver), `services/worker/deadletter` (parking and replaying failed events), `services/worker/admin` (the admin HTTP surface), `services/worker/jobs` (scheduled jobs) and `services/worker/transport` (the Kafka side of the event transport). `search` is not a module of its own: the index write and `GET /courses/search` live in `services/course` (ADR-0025). Each domain module owns one business capability and follows the same internal shape: `handler.go` (HTTP), `service.go` (business logic and transactions), `repository.go` (the only file touching the database directly), `dto.go` (API request/response types), `domain.go` (entities). None of them import each other's internals directly. Between services that is enforced by Go itself (each service is its own Go module, see below); inside the `course` service, where course, enrollment, progress and certificate share one module, by a lint rule rather than just convention. `services/README.md` has the module map.

**One repository, 25 Go modules (ADR-0026).** Each service is its own Go module under `services/` (`identity`, `course`, `analytics`, `worker`), the shared kernel is split into one module per package under `libs/` (`auth`, `db`, `config`, `errors`, `idempotency`, ... 19 including `testenv`), and `tools/seed` and `test/e2e` are modules too. Three things follow that will surprise you if you come from a single-module project:

- **A service can only import what its `go.mod` lists.** `identity` cannot import `course` — not because a linter says so, but because it doesn't depend on it. `scripts/check-module-graph.sh` fails CI if that changes. The `worker` is the one service allowed to depend on others (it runs `course` and `analytics` event handlers and the publish workflow).
- **Each binary links only what it uses.** `identity-api` has no Temporal SDK in it. `scripts/check-link-graph.sh` enforces that. This is what the Week 2 review asked for.
- **`go.work` is for your laptop; CI and Docker use `GOWORK=off`.** If you add an import of a library to a module, add its `require` **and its `replace`** (plus the `replace` of anything that library depends on) to your module's `go.mod`, then `GOWORK=off go mod tidy`. Go does not inherit `replace` directives, so forgetting one works on your machine and fails in CI. `libs/README.md` has the dependency order, and `docs/guides/versioning.md` explains when modules get tagged versions (not yet).

**State changes now, side effects later.** A request that changes something (enrolling, publishing a course) commits to Postgres and returns immediately. Anything that doesn't need to block the caller — sending a notification, issuing a certificate, recalculating analytics, archiving the event — happens asynchronously via the outbox pattern (§1.1 above). The relay delivers outbox rows either in-process (`EVENT_TRANSPORT=inprocess`, the default: it runs the handlers itself) or through Kafka (`EVENT_TRANSPORT=kafka`: it produces to a topic after the broker acks, and a consumer group per handler runs them, ADR-0029). The handlers are the same under both. This is *why* enrollment feels instant even though "enrolling" conceptually triggers several downstream effects.

**Full diagrams and the complete reasoning:** `docs/rfc/smartcourse-rfc.md`. Skim §3 (consistency model) and §4 (architecture overview) — those two sections are what everything else is derived from.

---

## 5. What's actually built right now

Don't assume the RFC describes the current state of the code — it describes the target design, and marks what's implemented. As of now:

**Built, tested, running** (see `docs/plans/week-3.md` §10 for the dated log):
- **The core:** the full schema; the shared kernel (auth, config, errors, rate limiting, the outbox writer); `identity`, `course`, `enrollment` and `progress` with HTTP APIs and integration tests against real infrastructure; the outbox relay (`libs/relay`), a bounded worker pool proven by concurrency tests.
- **Course publishing** is a real Temporal workflow (M2, ADR-0025): `POST /courses/{id}/publish` starts `PublishCourseWorkflow`, `GET /courses/{id}/publish-status` polls it. `GET /courses/search` and a Redis course-detail cache are real too.
- **Assets:** `POST /assets/upload-url` issues pre-signed MinIO URLs; publishing checks every lesson asset against object storage.
- **`Idempotency-Key`** on the course POST routes (`libs/idempotency`, DR-023).
- **Certificates:** one per completed enrollment, owner-only list and get, public verification by code (`services/course/certificate`, DR-025).
- **Analytics:** all 9 FR-5 metrics have a source, including Most Popular Courses and Workflow Issues (DR-026); per-course stats are owner-gated for instructors (DR-024); a reconciler recomputes everything from the mirrors every 60 s.
- **Worker:** Asynq pools (one server per queue), scheduled jobs (stale-publish reconciliation, analytics and enrollment-counter reconciliation, idempotency-key and outbox pruning), and the notification dispatcher with an SMTP provider pointed at Mailpit (web UI on http://localhost:8025). No real mail provider is integrated, by decision.
- **Kafka transport:** `EVENT_TRANSPORT=kafka` makes the relay produce to Kafka after the broker acks and runs a consumer group per handler (`libs/kafka`, `services/worker/transport`, ADR-0029). The default stays `inprocess`.
- **MongoDB:** the audit archiver (Kafka mode only), the dead-letter store, and replay of failed events through the worker's admin surface on `127.0.0.1:8093` (admin role only). `analytics-api` serves the Failed Events counts from the same store, and answers 503 on that one route when MongoDB is unavailable.

**Where to look for patterns:** concurrency in production code is in `libs/relay/`, `libs/kafka/`, `libs/asynqx/` and `services/course/workflows/workflow.go`, not only in the RFC. To run the publish workflow locally use `make dev-workflow` (not `dev-core`).

**In progress or not wired yet:**
- **Observability.** The Prometheus, Grafana and Jaeger containers (`make dev-observability`) and `libs/telemetry` exist, and instrumenting the services with metrics and traces is being wired now; until it lands, treat the dashboards as empty. Alert thresholds are placeholders.
- **Workflow Issues** (the other half of the ninth analytics metric): counts publishes that ended in `publish_failed`, from `course.publish_failed` events since that event existed (DR-026); failures before it are not counted.
- **Search-index and cache reconciliation** (ADR-0025's gap).
- **Unverified:** a Confluent-format consumer reading our Kafka bytes, multi-broker or TLS Kafka, the full stack running in Kafka mode from built images, and the CI jobs for the Kafka tier.
- **Switching an existing database to Kafka mode:** rows already published in-process never reach Kafka, and a new consumer group starts at the beginning of each topic.

Full detail: `docs/rfc/smartcourse-rfc.md` §21.0.

---

## 6. Where to go next

Reading order, roughly in order of how much context each one assumes:

1. This document.
2. `docs/rfc/smartcourse-rfc.md` §3 and §4 — the consistency model and architecture overview everything else derives from.
3. `migrations/` in order — the schema *is* the domain model; the constraints encode the actual business rules (a partial unique index doing real work is more informative than a paragraph describing the rule it enforces).
4. `services/course/enrollment/` end to end — the clearest illustration of a transaction, an outbox write, and the sync/async boundary in one small module.
5. `libs/README.md` — the conventions every module builds on.
6. `docs/adr/smartcourse-adr.md` — *why* things are the way they are, including decisions that were reconsidered. Skim the index; read the ones relevant to whatever you're about to touch.
7. `docs/guides/testing.md` — before you write your first test.
8. `docs/plans/week-3.md` (the dated progress log in §10 is the current state of the work) and `docs/rfc/differences-from-requirements.md` — every place the design goes beyond or reinterprets the original brief, including the Week 3 decisions (DR-022 to DR-025).

## 7. Working agreements

- Every non-trivial decision gets an ADR (`docs/adr/`). "I thought about three options and picked one" is worth thirty seconds to record — it's the difference between a future contributor knowing something was reasoned versus wondering if it was an accident.
- Every schema change is a migration with a tested down path (`migrations/README.md`).
- Every module you touch should have a concurrency test if it touches shared state, and an integration test if it touches the database. If you're not sure which tier a new test belongs in, `docs/guides/testing.md` has the answer.
- If your change adds something the original project brief didn't ask for, or reinterprets something it left ambiguous, log it in `docs/rfc/differences-from-requirements.md` — that's the mechanism that keeps "we made a reasonable judgment call" distinguishable from "we quietly went off-spec."
- If `.golangci.yml`'s `depguard` check fails on your change, the dependency you added is wrong — restructure around it (usually: declare a narrow interface in the consuming module) rather than loosening the rule.
