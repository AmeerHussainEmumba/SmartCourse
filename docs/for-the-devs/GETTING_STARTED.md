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
- That's it to get the system running. `make dev-core` starts the infrastructure (Postgres, Redis, MinIO, Mailpit, Kafka, Schema Registry, MongoDB, Swagger UI) and nine of the ten long-running binaries (the eleventh binary, `identity-admin`, is a one-shot CLI that `make seed` and `make stack-up` run on demand). Course publishing needs `make dev-workflow` (the tier these steps use): it adds Temporal, its UI and `publishing-worker`. The observability stack is the optional tier `make dev-observability`. Kafka is part of `core` (there is no in-process mode). See "Profiles" in §2 and `docs/guides/local-stack.md`. The object store is the community fork `pgsty/minio` (the official `minio/minio` images can no longer be pulled on a fresh machine); see README "Object store image" and ADR-0019.

### Steps

```bash
git clone <repo-url> SmartCourse && cd SmartCourse

# 1. Configuration — copy the template and fill in real values.
#    Nothing here has a working default; a missing value fails startup
#    loudly rather than silently using something insecure.
cp env.example .env
#    The compose services take their configuration from docker-compose.yml
#    (dev defaults), so .env matters for native `make run-*` and for the Makefile.
#    Before running anything natively, generate a real signing key:
#    openssl rand -base64 48
#    and put it in JWT_SIGNING_KEY in .env.

# 2. Start the stack — infrastructure plus the services, and (since make
#    seed publishes real courses through a real Temporal workflow, M2)
#    Temporal and publishing-worker. `dev-workflow` is a superset of
#    `dev-core`, not a different stack — everything `dev-core` starts is
#    still here too. The compose postgres creates the per-service roles on a
#    fresh volume; the workers retry a few times until the tables exist.
#    (Run it again with UP_FLAGS=--build after a code change.)
make dev-workflow

# 3. Apply the schema (then the per-service roles, idempotent).
#    One-shot alternative for a fresh stack: `make stack-up` does steps 2
#    and 3, registers the event schemas and, when IDENTITY_ADMIN_EMAIL and
#    IDENTITY_ADMIN_PASSWORD are set, creates the first admin.
make migrate-up

# 4. Populate realistic data: instructors, courses at every lifecycle
#    state (draft, published, archived), students with partial progress.
#    `make seed` first creates the first admin with the identity-admin CLI
#    (`docker compose run --rm -T identity-api identity-admin`, from
#    SEED_ADMIN_EMAIL / SEED_ADMIN_PASSWORD; the defaults are the
#    env.example values admin@smartcourse.local / change-me-admin-password),
#    then runs tools/seed on your host as a plain HTTP client of the
#    published ports (8090 identity-api, 8091 course-api, 8092 analytics-api).
#    It registers users, promotes instructors, creates and publishes courses,
#    enrolls students and marks progress through the real endpoints, so it
#    also proves the public contracts and the publish workflow. It waits for
#    the services to be ready and fails with a clear message if one is not.
#    Safe to re-run (accounts are found by logging in, courses by slug).
#    A first run can take a few minutes when the register rate limit (5 a
#    minute per IP) applies; it waits and retries. See tools/seed/README.md.
make seed

# 5. Nothing left to do — `make dev-workflow` (step 2) already started the
#    application containers too (the nine binaries of the `core` profile,
#    which `workflow` includes, plus publishing-worker, Temporal and its
#    UI). See "Running natively instead" below if you want faster
#    iteration than a Docker rebuild on every change.
```

Check it's alive (each HTTP service has its own `/health/ready`):

```bash
curl localhost:8090/health/ready    # identity-api
curl localhost:8091/health/ready    # course-api
curl localhost:8092/health/ready    # analytics-api
curl localhost:8093/health/ready    # audit-api
curl localhost:9101/health/ready    # identity-api's internal listener; every binary has one (9101-9104 APIs, 9111-9116 workers)
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

From here, `api/openapi.yaml` documents every endpoint (across identity-api, course-api, analytics-api and audit-api — one spec over four processes, each path tagged with the server that answers it; it covers the public HTTP API only, not the internal listeners or the Kafka events), and each module's README (for example `services/course/course/README.md`) explains the design behind it.

### DB roles

Postgres has one database and four service schemas (`identity`, `course`, `analytics`, `notification`), and each schema has its own login role: `svc_identity`, `svc_course`, `svc_analytics`, `svc_notification`. A role can use only its own schema's tables and sequences, plus the `citext` / `pg_trgm` functions in `public`; every other service schema is closed to it (`permission denied`), and it cannot create tables. `smartcourse` (from `POSTGRES_DSN` in `.env`) is the owner: it runs the migrations and `scripts/db-roles.sh`. `make seed` does not use the database at all: it creates the first admin with `identity-admin` (as `svc_identity`, inside the identity-api container) and then calls the HTTP APIs. `publishing-worker` has no database and `audit-api` / `audit-worker` use MongoDB only, so there is no `svc_publishing` or `svc_audit`.

- **Passwords** come from `SVC_<SERVICE>_PASSWORD` in `.env` (dev values are in `env.example`; nothing is in a committed SQL file). `scripts/db-roles.sh` creates the roles and grants. It runs by itself the first time the compose Postgres volume is created (a script mounted into the container) and at the end of every `make migrate-up`; `make db-roles` runs it alone. It is safe to repeat. **If your Postgres volume already existed**, `make migrate-up` is what creates the roles.
- **Every service connects with its own role.** In compose each container gets its role's DSN and `POSTGRES_SEARCH_PATH=<schema>,public` from `docker-compose.yml`. For a native `make run-*`, `env.example` lists the per-service DSNs (`IDENTITY_POSTGRES_DSN` and so on); set `POSTGRES_DSN` and `POSTGRES_SEARCH_PATH` for the binary you run (the `POSTGRES_DSN` in `env.example` is the owner's, for migrations only).
- **Migration order** is `platform identity course analytics notification cutover`. The `cutover` set is a one-off for databases created before the split: it moves the old shared `platform` tables' rows into each service's own `outbox`, `processed_events` and `idempotency_keys` and then drops the `platform` schema (on a fresh database it moves nothing; the `platform` set itself keeps only the `citext` / `pg_trgm` extensions in `public`).
- **In tests**, `testenv.Setup(t)` returns `env.DB` as the owner (its search path is `public` only, so write `course.outbox`, not `outbox`), and `env.ServiceDB(t, "course")` connects as that service's role with search path `course,public`. Use `ServiceDB` for anything a service would do. See `libs/testenv/README.md`.
- **Name collisions:** `outbox`, `processed_events` and `idempotency_keys` now exist in more than one schema. A connection whose search path lists several service schemas silently uses the first one. Never build such a search path; qualify the name instead.

### What is listening where

Everything is published on `127.0.0.1` only.

| Port | What | Started by |
|---|---|---|
| 8090 / 8091 / 8092 / 8093 | identity-api / course-api / analytics-api / audit-api (container port 8080) | `dev-core` and up |
| 9101 / 9102 / 9103 / 9104 | internal listeners of the four APIs (`/metrics`, `/health/*`) | `dev-core` and up |
| 9111 / 9112 / 9113 / 9114 / 9116 | internal listeners of identity-worker / course-worker / analytics-worker / notification-worker / audit-worker | `dev-core` and up |
| 9115 | internal listener of publishing-worker | `dev-workflow` and up |
| 8089 | Swagger UI over `api/openapi.yaml` | `dev-core` and up |
| 8025 (web) / 1025 (SMTP) | Mailpit, the local mail sink: read the mail the system sends | `dev-core` and up |
| 5433 / 6380 | Postgres / Redis | `dev-core` and up |
| 9002 (S3) / 9003 (console) | MinIO | `dev-core` and up |
| 29092 / 8081 / 27017 | Kafka (from the host) / Schema Registry / MongoDB | `dev-core` and up |
| 7233 / 8088 | Temporal / Temporal UI | `dev-workflow` and up |
| 9090 / 3001 / 16686 | Prometheus / Grafana / Jaeger UI (OTLP on 4317 and 4318) | `dev-observability` and up |

`/metrics` is only on the internal listeners, never on an API's public port.

### Profiles

| Command | Adds |
|---|---|
| `make dev-core` | Postgres, Redis, MinIO (+init), Mailpit, Kafka (+topic init), Schema Registry (+schema bootstrap), MongoDB, Swagger UI and nine binaries: everything except `publishing-worker` (so course publishing does not work on `core`) |
| `make dev-workflow` | `core` + Temporal, Temporal UI, `publishing-worker` |
| `make dev-observability` | `workflow` + Prometheus, Grafana, Jaeger |
| `make dev-full` | everything |
| `make dev-events` | deprecated alias of `dev-core` (Kafka, Schema Registry and MongoDB are in `core` now) |
| `make dev-down` | stops every profile's containers (data volumes are kept) |

### Kafka, the Schema Registry and MongoDB

Kafka is the only event transport (there is no `EVENT_TRANSPORT` and no in-process mode), so every worker except `publishing-worker` needs it, and every worker except the audit ones needs the registry (`audit-api` and `audit-worker` need only the brokers and MongoDB). All three are in `core`: compose creates the topics (and one `retry.<consumer>` topic per consumer) with `kafka-init`, and registers the event schemas with the one-shot `schema-init` service before any worker starts. On the host, `make register-schemas` does the same (idempotent). Each binary asks `libs/config` only for the groups it needs; the table below says which. The compose services get their in-network values (`kafka:9092`, `http://schema-registry:8081`, `mongodb://mongodb:27017`) from `docker-compose.yml`; a native `make run-*` uses `localhost:29092`, `http://localhost:8081` and `mongodb://localhost:27017` from `.env`.

The identity, course, analytics and notification workers refuse to start, with a clear error, if `KAFKA_BROKERS` or `SCHEMA_REGISTRY_URL` is missing. If the registry is unreachable or a schema subject is missing, the worker stays up instead: it logs the missing subject by name, retries, reports not ready on its internal listener and starts no consumer until the problem is fixed. `MONGO_URI` is optional for those four: without it dead-letter parking is off and the worker still starts. It is required by `audit-api` and `audit-worker`, which own the MongoDB store. The admin surface (failed-event listing and replay) is `audit-api`, not a worker listener. `make dev-observability` adds Prometheus (which scrapes the internal listeners of all ten long-running binaries), Grafana and Jaeger (`observability/README.md`).

### Configuration per binary

A binary fails at startup, naming the variable, when a group it needs is missing, and starts without the variables of the groups it does not need. Every binary also reads the optional shared settings (`APP_ENV`, `LOG_LEVEL`, `METRICS_HTTP_ADDR`, `OTEL_*`). Each service's binaries use that service's Postgres role and `POSTGRES_SEARCH_PATH=<schema>,public`. `env.example` lists the variables per binary; the rules are in `libs/config/README.md`.

| Binary | Required groups | Optional groups it reads |
|---|---|---|
| identity-api | Postgres, Redis, JWT, Argon (defaults) | |
| identity-worker | Postgres, Redis, Kafka | Asynq/jobs |
| identity-admin (CLI) | Postgres, Redis, Argon (defaults); no JWT | |
| course-api | Postgres, Redis, MinIO, JWT | Temporal |
| course-worker | Postgres, Redis, MinIO, Kafka | MongoDB, Temporal, Asynq/jobs |
| analytics-api | Postgres, Redis, JWT | MongoDB |
| analytics-worker | Postgres, Redis, Kafka | MongoDB, Asynq/jobs |
| notification-worker | Postgres, Redis, Kafka | MongoDB, SMTP, Asynq/jobs |
| publishing-worker | none | Temporal |
| audit-api | Redis, JWT, Kafka brokers, MongoDB (no Postgres, no registry) | |
| audit-worker | Kafka brokers, MongoDB (no Postgres, no registry) | |

Required variables per group: Postgres `POSTGRES_DSN` and `POSTGRES_SEARCH_PATH`; Redis `REDIS_ADDR`; MinIO `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY_ID`, `MINIO_SECRET_ACCESS_KEY`; JWT `JWT_SIGNING_KEY`; Kafka `KAFKA_BROKERS` and `SCHEMA_REGISTRY_URL`. `PROCESSED_EVENTS_RETENTION` (default `336h`) is read by course-worker, analytics-worker and notification-worker.

### Running natively instead

`make dev-workflow` containerizes everything, including the app, which means a code change needs a rebuild to see (`make dev-workflow UP_FLAGS=--build`). For faster iteration, stop the containerized binary you are changing (for example `docker compose stop course-api course-worker`; leave the infrastructure running) and run it natively, each in its own terminal, with the `.env` values for that binary (its own role DSN and `POSTGRES_SEARCH_PATH`, see `env.example` section 2) and a free `METRICS_HTTP_ADDR`:

```bash
make run-identity-api     # HTTP_PORT default 8080: set HTTP_PORT=8090 to match compose
make run-course-api       # HTTP_PORT=8091
make run-course-worker    # no public port; internal listener METRICS_HTTP_ADDR
```

Any of the eleven binaries works the same way (`make run-<binary>`, `make build-<binary>`).

### Browsing and running the API without writing curl by hand

Two ways to explore `api/openapi.yaml` interactively, both reading the same file — neither is a separate thing to keep in sync:

- **In the browser:** `make dev-core` (and every profile that includes it, such as the `dev-workflow` of step 2) also starts a Swagger UI container reading `api/openapi.yaml` live — **http://localhost:8089**. Every endpoint, with an "Execute" button that fires a real request against your local API. The spec lists the four services' base URLs (`localhost:8090` identity-api: auth/users/roles, `:8091` course-api: courses/enrollments/progress/certificates, `:8092` analytics-api, `:8093` audit-api: failed events and replay), and each path is tagged with the server that answers it, so "Execute" goes to the right process without picking one by hand. Click "Authorize" and paste an access token from the register/login call above to call anything that needs one.
- **In Postman:** File → Import → select `api/openapi.yaml` from this repo. Postman builds a full collection from it, with the four base URLs available as environment/collection variables. Set your access token as another variable, then reference it as a Bearer token on requests that need auth.

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

**PostgreSQL is the single source of truth.** Redis and MongoDB hold only derived, rebuildable data — never something a user's correctness depends on.

**Six services, eleven binaries, one repository and one database (ADR-0022, ADR-0023, ADR-0024, ADR-0033, ADR-0034, ADR-0035).** Each service is its own Go module under `services/` and imports nothing but `libs/*`; they talk only through Kafka events. The table is the map:

| Service | Binaries | Owns | Notes |
|---|---|---|---|
| `services/identity` | `identity-api` (8090), `identity-worker`, `identity-admin` (CLI) | Postgres schema `identity`, its outbox | Auth, users, roles. The worker relays the identity outbox and runs the token-version re-seeder. `identity-admin` creates the first admin (the HTTP API cannot). |
| `services/course` | `course-api` (8091), `course-worker` | schema `course`: courses, enrollments, progress, certificates, outbox, `processed_events`, `idempotency_keys` | One module and one schema because enrollment locks the `courses` row in the transaction that writes the enrollment (ADR-0012). The worker relays the outbox, runs the consumers (instructor mirror, enrollment archival on `course.course_archived`, certificate issuer on `progress.course_completed`), the scheduled jobs and the Temporal **activities**. |
| `services/analytics` | `analytics-api` (8092), `analytics-worker` | schema `analytics`, `processed_events` | Read-only dashboard fed entirely by events; the worker holds the consumers and the reconciler. It never queries `course` or `identity`. |
| `services/notification` | `notification-worker` | schema `notification`, `processed_events` | No API. Consumers write delivery records, an Asynq pool sends mail (SMTP to Mailpit locally). |
| `services/publishing` | `publishing-worker` | nothing (no database) | The Temporal workflow `PublishCourseWorkflow` (orchestration only). It calls the activities in `course-worker` by name over a separate task queue (`libs/contracts/publishing`, ADR-0035). |
| `services/audit` | `audit-api` (8093), `audit-worker` | MongoDB: event archive, `audit_log`, `failed_events` | The worker archives every event; the API lists, shows and replays failed events (admin role only). Replay is a republish to `retry.<consumer>`, not a re-invocation. |

No foreign key or query crosses a schema boundary, and the Postgres roles enforce it. A service that needs another's data for display, not ownership, keeps its own small event-fed copy (for example `course.instructor_mirror`, so the course catalogue can show an instructor's name without reaching into `identity`). Each binary wires only its own modules in its own `cmd/<binary>/wire.go` on top of `libs/runtime` (signals, config, telemetry, graceful shutdown, and the internal listener). There is no `services/worker` any more: it was one shared worker that imported `course` and `analytics`, and ADR-0033 split it up. (The history, one binary with a `--mode` flag, then two, then four, then this, is ADR-0022 to ADR-0024, ADR-0026 and ADR-0033, and is a good example of a design decision getting revisited on real feedback.)

**Domain modules, one shared kernel.** The HTTP domain packages are `services/identity/identity`, `services/course/course`, `services/course/enrollment`, `services/course/progress`, `services/course/certificate` and `services/analytics/analytics`; the asynchronous side lives in each service's own packages (`services/notification/notification`, `services/audit/{archive,deadletter,replay,admin,store}`, `services/publishing/publish`, `services/course/activities`) and in `libs/eventbus` (Kafka publisher, consumer runner, codec), `libs/relay` (the publish-only outbox relay) and `libs/jobs` (scheduled jobs). `search` is not a service of its own: the index write and `GET /courses/search` live in `services/course` (ADR-0025). Each domain package owns one business capability and follows the same internal shape: `handler.go` (HTTP), `service.go` (business logic and transactions), `repository.go` (the only file touching the database directly), `dto.go` (API request/response types), `domain.go` (entities). Between services the Go module boundary keeps them apart (below); inside the `course` service, where course, enrollment, progress and certificate share one module, a lint rule does (`.golangci.yml`'s `depguard`). The table above is the service map and `libs/README.md` is the library map.

**One repository, 33 Go modules (ADR-0026, ADR-0033).** Six service modules under `services/` (`identity`, `course`, `analytics`, `notification`, `publishing`, `audit`), 24 library modules under `libs/` (one per package: `auth`, `db`, `config`, `errors`, `eventbus`, `events`, `kafka`, `relay`, `runtime`, `testenv`, `contracts/publishing`, ...; `libs/README.md` lists them), and three more: `tools/seed`, `tools/guardcheck` and `test/e2e`. Three things follow that will surprise you if you come from a single-module project:

- **A service can only import what its `go.mod` lists, and it lists libraries only.** `identity` cannot import `course` (and `publishing` cannot import `course` either, which is why the workflow contract is a library). `scripts/check-module-graph.sh` fails CI if a service requires another service; `tools/seed` and `test/e2e` require no service module either.
- **Each binary links only what it uses.** `identity-api` has no Temporal SDK in it. `scripts/check-link-graph.sh` enforces that, per binary.
- **`go.work` is for your laptop; CI and Docker use `GOWORK=off`.** If you add an import of a library to a module, add its `require` **and its `replace`** (plus the `replace` of anything that library depends on) to your module's `go.mod`, then `GOWORK=off go mod tidy`. Go does not inherit `replace` directives, so forgetting one works on your machine and fails in CI. `libs/README.md` has the dependency order, and `docs/guides/versioning.md` explains when modules get tagged versions (not yet).

**State changes now, side effects later.** A request that changes something (enrolling, publishing a course) commits to Postgres and returns immediately. Anything that doesn't need to block the caller — sending a notification, issuing a certificate, recalculating analytics, archiving the event — happens asynchronously via the outbox pattern (§1.1 above). Each service that produces events has its own outbox table and its own relay in its worker, which only publishes: it produces to a Kafka topic and marks the row published after the broker acks. Consumers run in the owning service's worker, one consumer group per consumer, and write a `processed_events` marker in the same transaction as their effect, so a redelivery does nothing (ADR-0033). Kafka is the only transport; there is no in-process mode, and cross-service order is not guaranteed, so a consumer must tolerate a dependency arriving late. This is *why* enrollment feels instant even though "enrolling" conceptually triggers several downstream effects.

**Full diagrams and the complete reasoning:** `docs/rfc/smartcourse-rfc.md`. Skim §3 (consistency model) and §4 (architecture overview) — those two sections are what everything else is derived from.

---

## 5. What's actually built right now

Don't assume the RFC describes the current state of the code — it describes the target design, and marks what's implemented. As of now:

**Built, tested, running** (see `docs/plans/week-3.md` §10 for the dated log):
- **The core:** the full schema; the shared kernel (auth, config, errors, rate limiting, the outbox writer); `identity`, `course`, `enrollment` and `progress` with HTTP APIs and integration tests against real infrastructure; the per-service outbox relay (`libs/relay`, a bounded worker pool proven by concurrency tests).
- **Course publishing** is a real Temporal workflow (M2, ADR-0025, ADR-0035): `POST /courses/{id}/publish` starts `PublishCourseWorkflow` (run by `publishing-worker`, whose activities run in `course-worker`), `GET /courses/{id}/publish-status` polls it. `GET /courses/search` and a Redis course-detail cache are real too.
- **Assets:** `POST /assets/upload-url` issues pre-signed MinIO URLs; publishing checks every lesson asset against object storage.
- **`Idempotency-Key`** on the course POST routes (`libs/idempotency`, DR-023).
- **Certificates:** one per completed enrollment, owner-only list and get, public verification by code (`services/course/certificate`, DR-025).
- **Analytics:** all 9 FR-5 metrics have a source, including Most Popular Courses and Workflow Issues (DR-026); per-course stats are owner-gated for instructors (DR-024); a reconciler recomputes everything from the mirrors every 60 s.
- **Workers:** each service's worker runs its own relay and consumers (`libs/eventbus`). Asynq pools and scheduled jobs live in `course-worker` (stale-publish reconciliation, enrollment-counter and its own outbox, `processed_events` and idempotency-key pruning), `analytics-worker` (reconciler) and `notification-worker` (the dispatcher with an SMTP provider pointed at Mailpit, web UI on http://localhost:8025; no real mail provider is integrated, by decision).
- **Kafka transport:** Kafka is the only transport: each service's relay produces to Kafka after the broker acks, and a consumer group per consumer reads it (`libs/kafka`, `libs/eventbus`, ADR-0033).
- **MongoDB and the audit service:** `audit-worker` archives every event; the workers park events that exhaust their retries in `failed_events`; `audit-api` lists them and replays them by republishing to `retry.<consumer>` (admin role only). `analytics-api` reads the Failed Events counts from the same collection and answers 503 on that one route when MongoDB is unavailable.
- **Observability:** every binary serves `/metrics` and `/health/*` on its internal listener; the `observability` profile provisions Prometheus (scraping all ten long-running binaries), one Grafana dashboard and Jaeger, and `libs/telemetry` carries trace context through the event envelope (ADR-0032). Alert thresholds are placeholders.
- **End-to-end tests:** `make test-e2e` (`test/e2e`, build tag `e2e`) builds the binaries and runs them as processes against testcontainers for Postgres, Redis, Kafka, the Schema Registry, MongoDB, Mailpit and Temporal; it needs Docker and takes several minutes. Not every scenario is ported to the new harness yet: see `test/README.md`.

**Where to look for patterns:** concurrency in production code is in `libs/relay/`, `libs/eventbus/`, `libs/kafka/`, `libs/asynqx/` and `services/publishing/publish/workflow.go`, not only in the RFC. To run the publish workflow locally use `make dev-workflow` (not `dev-core`).

**In progress or not wired yet:**
- **Workflow Issues** (the other half of the ninth analytics metric): counts publishes that ended in `publish_failed`, from `course.publish_failed` events since that event existed (DR-026); failures before it are not counted.
- **Search-index and cache reconciliation** (ADR-0025's gap).
- **Unverified:** a Confluent-format consumer reading our Kafka bytes, multi-broker or TLS Kafka, MongoDB with authentication or TLS, and the Grafana dashboard and alert rules under real load (they are provisioned and placeholder-tuned).
- **E2E scenarios:** E02 and E08 to E14 are still pending on the new container harness (`test/README.md`, `scripts/check-pending-e2e.sh`).

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
