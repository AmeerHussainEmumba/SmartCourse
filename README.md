# SmartCourse

A course-delivery platform backend for EduCorp: course authoring and publishing, enrollment, progress tracking, and analytics — built to stay consistent and observable under concurrent load, not just functionally correct in a single-user test.

This README is the fast path to running the system and finding your way around. For *why* it's built this way, see the docs linked at the bottom — they're not optional reading, they're where every non-obvious decision is actually explained.

## Quick start

```bash
cp env.example .env          # for native `make run-*` and the Makefile; docker compose does not need it
make dev-workflow            # the whole stack: infrastructure + the ten long-running binaries + Temporal (see Profiles below)
make migrate-up              # apply the schema (six migration sets) and create the per-service Postgres roles
make seed                    # optional — creates the first admin (identity-admin), then a realistic dataset over the HTTP APIs; safe to re-run
```

On a fresh machine, `make stack-up` does the first-run sequence in one go: starts the infrastructure, runs `migrate-up`, registers the event schemas, starts every service and, when `IDENTITY_ADMIN_EMAIL` and `IDENTITY_ADMIN_PASSWORD` are set, creates the first admin (`identity-admin`).

Check it's alive (each binary also has an internal listener with `/health/ready` and `/metrics`, on `127.0.0.1` only):

```bash
curl localhost:8090/health/ready    # identity-api   (internal listener :9101)
curl localhost:8091/health/ready    # course-api     (:9102)
curl localhost:8092/health/ready    # analytics-api  (:9103)
curl localhost:8093/health/ready    # audit-api      (:9104)
curl localhost:9111/health/ready    # identity-worker; course-worker 9112, analytics-worker 9113, notification-worker 9114, publishing-worker 9115, audit-worker 9116
```

Browse and run every endpoint without writing curl by hand: **http://localhost:8089** (a Swagger UI reading `api/openapi.yaml` live, also started by `make dev-core`) — or `File → Import` that same file into Postman. See `docs/for-the-devs/GETTING_STARTED.md`'s "Browsing and running the API" section for both.

### Services and binaries

Six services, eleven binaries (ten long-running processes and the `identity-admin` CLI). Each service is its own Go module and imports only `libs/*`.

| Service | Binary | Host port (public / internal) | What it does |
|---|---|---|---|
| identity | `identity-api` | 8090 / 9101 | register, login, tokens, profile, roles (`/auth/*`, `/me`, `/admin/users/*`) |
| identity | `identity-worker` | internal 9111 | relays the identity outbox to Kafka; token-version re-seeder |
| identity | `identity-admin` | none (CLI) | creates the first admin (`make seed`, `make stack-up`) |
| course | `course-api` | 8091 / 9102 | courses, modules, lessons, assets, publish, enrollments, progress, certificates |
| course | `course-worker` | internal 9112 | course relay, consumers (instructor mirror, enrollment archival, certificate issuer), scheduled jobs, Temporal activities |
| analytics | `analytics-api` | 8092 / 9103 | read-only analytics (`/analytics/*`) |
| analytics | `analytics-worker` | internal 9113 | event consumers and the reconciler |
| notification | `notification-worker` | internal 9114 | consumers, Asynq pools, SMTP delivery (no API) |
| publishing | `publishing-worker` | internal 9115 | the Temporal `PublishCourseWorkflow` (no database; `workflow` profile) |
| audit | `audit-api` | 8093 / 9104 | failed-event list, inspect and replay (admin only) |
| audit | `audit-worker` | internal 9116 | archives every event into MongoDB |

### Profiles

| Command | Starts |
|---|---|
| `make dev-core` | Postgres, Redis, MinIO, Mailpit, Kafka, Schema Registry (+ schema bootstrap), MongoDB, Swagger UI and nine binaries: identity-api/-worker, course-api/-worker, analytics-api/-worker, notification-worker, audit-api/-worker |
| `make dev-workflow` | `core` + Temporal, Temporal UI and `publishing-worker`. **Course publishing needs this profile**: on plain `core` there is no `publishing-worker`, so `POST /courses/{id}/publish` starts a workflow that nothing executes |
| `make dev-observability` | `workflow` + Prometheus, Grafana, Jaeger |
| `make dev-full` | everything |

`make dev-events` still works as an alias of `dev-core` (Kafka is in `core` now); `make dev-down` stops everything (volumes are kept). `make build` / `make build-<binary>` and `make run-<binary>` build and run the binaries natively. See [`docs/guides/local-stack.md`](docs/guides/local-stack.md) for ports, per-service Postgres roles, memory and known issues, and [`docs/adr/smartcourse-adr.md` ADR-0018](docs/adr/smartcourse-adr.md#adr-0018--docker-compose-profiles-for-tiered-local-development) for why the environment is tiered.

| Host port | What |
|---|---|
| 8090 / 8091 / 8092 / 8093 | identity-api / course-api / analytics-api / audit-api |
| 9101-9104 / 9111-9116 | internal listeners (`/metrics`, `/health/*`) of the APIs / of the workers (identity, course, analytics, notification, publishing, audit) |
| 8089 / 8025 / 8088 | Swagger UI / Mailpit web / Temporal UI |
| 5433 / 6380 / 9002 / 9003 | Postgres / Redis / MinIO S3 / MinIO console |
| 29092 / 8081 / 27017 | Kafka (host listener) / Schema Registry / MongoDB |
| 7233 / 9090 / 3001 / 16686 | Temporal / Prometheus / Grafana / Jaeger UI |

### Object store image

The S3-compatible object store is `pgsty/minio` (a community fork of MinIO with prebuilt images, AGPLv3), pinned by tag and digest in `docker-compose.yml`, with `pgsty/mc` creating the bucket. The official `minio/minio` and `minio/mc` images can no longer be pulled on a fresh machine, which is why. See the amendment in [ADR-0019](docs/adr/smartcourse-adr.md#adr-0019--minio-for-learning-asset-object-storage) for the risk and how to revert.

## Architecture, in one paragraph

PostgreSQL is the system of record (everything a user can assert as fact about their own account — enrolled, completed, certified — is strongly consistent, enforced by database constraints, not application code that can race). Redis and MongoDB hold only derived, rebuildable data. State changes are recorded and their side effects (search indexing, analytics, notifications, caching) happen asynchronously via a transactional outbox, so a crash mid-request never silently loses work or desyncs a dashboard from reality. The system is split into services that share no code but the libraries in `libs/*` (the plan is `docs/plans/microservices-split.md`): `identity`, `course`, `analytics`, `notification`, `publishing` and `audit`, built as eleven binaries (an `-api` and a `-worker` per service where it has both; `publishing-worker` runs the Temporal publish workflow, `notification-worker` has no API, and `identity-admin` is a CLI). Each service owns its Postgres schema, its own outbox (where it produces events) and its own idempotency tables, and connects with its own Postgres role (`svc_identity`, `svc_course`, `svc_analytics`, `svc_notification`; `publishing` has no database and `audit` uses MongoDB only), so a query across a service boundary fails with `permission denied` instead of silently working ([ADR-0024](docs/adr/smartcourse-adr.md#adr-0024--schema-per-service-within-one-shared-database), [ADR-0033](docs/adr/smartcourse-adr.md#adr-0033--independent-services-per-service-outbox-and-idempotency-tables-kafka-only-integration-per-service-roles); the shared `platform` schema no longer exists). Services talk through Kafka events (Protobuf, Schema Registry); where one needs another's data it keeps an event-fed copy rather than reaching across. Each service's worker relays its own outbox to Kafka; consumers are idempotent through a `processed_events` marker written in the same transaction as the effect. The audit service archives every event in MongoDB and replays failed ones by republishing to a per-consumer retry topic ([ADR-0034](docs/adr/smartcourse-adr.md#adr-0034--the-audit-service-archive-dead-letter-store-and-replay-as-republish)); the publish workflow lives in `publishing`, its activities in `course-worker` ([ADR-0035](docs/adr/smartcourse-adr.md#adr-0035--the-publishing-service-workflows-and-activities-on-separate-task-queues)). Every binary serves `/metrics` and `/health/*` on an internal listener that is never on a public port. The RFC has not been fully revised for this topology yet (the ADRs and `docs/plans/microservices-split.md` are current; where the RFC disagrees, they win).

## Repository layout

```
services/              One Go module per service, each with its own go.mod; no service imports another (docs/plans/microservices-split.md)
  identity/            identity-api, identity-worker, identity-admin (CLI: first admin)
  course/              course-api, course-worker — course, enrollment, progress and certificate in one module, because the seat limit needs one transaction (ADR-0012)
  analytics/           analytics-api, analytics-worker
  notification/        notification-worker
  publishing/          publishing-worker (the Temporal publish workflow)
  audit/               audit-api, audit-worker (MongoDB archive, failed-event replay)
libs/                  Shared kernel and contracts, one Go module per package (24): auth cache config contracts/publishing db errors eventbus events httpx idempotency infra jobs kafka logging mongostore outbox ratelimit relay router runtime telemetry temporal asynqx testenv (see libs/README.md)
tools/seed/            `make seed` — populates a running stack by calling the HTTP APIs (after identity-admin creates the first admin); tools/guardcheck is the compose/source policy checker used by the guard scripts
migrations/          Versioned SQL schema (golang-migrate) — see migrations/README.md
docs/
  for-the-devs/      Start here — onboarding, setup, architecture in brief
  baseline/          The frozen commissioning brief — never edited
  prd/ rfc/ adr/     Living design documents
  plans/             Working plans for multi-step changes, with progress logs
  guides/            How-to docs, including testing.md
api/                 openapi.yaml — the public HTTP API contract of the four API services (one spec, per-path servers)
proto/               Pointer only; the Protobuf event contracts live in libs/events (see proto/README.md)
observability/       Prometheus config and rules, Grafana provisioning and dashboard (`make dev-observability`; placeholder alert thresholds; scrapes all ten binaries)
scripts/             Guards and helpers: check-link-graph.sh, check-module-graph.sh, check-module-hygiene.sh, check-dockerfile-modules.sh, check-dev-defaults.sh, check-http-hardening.sh, test-guards.sh, verify-deliverables.sh, for-each-module.sh
test/e2e/            Container-tier end-to-end scenarios: builds the binaries and runs them against testcontainers (its own Go module, requires no service module; `make test-e2e`; see test/README.md)
go.work              Lists every Go module for local development; CI and Docker build with GOWORK=off
```

Every top-level directory has its own `README.md` explaining what it's for — this map is the summary, not the whole story.

Every domain module under `services/` follows the same shape (`handler.go`, `service.go`, `repository.go`, `dto.go`, `domain.go` — see [`docs/rfc/smartcourse-rfc.md` §4.3](docs/rfc/smartcourse-rfc.md)), and none of them import each other's `repository`/`domain` types directly — cross-module reads go through a consumer-declared interface. Between services that is enforced by Go modules (`scripts/check-module-graph.sh`); inside `services/course` by `.golangci.yml`'s `depguard` rules, not just code review.

## API overview

REST over JSON, versioned under `/api/v1`, on four ports (8090 identity, 8091 course, 8092 analytics, 8093 audit). Full contract of the public API: `api/openapi.yaml` (one spec, each path tagged with its service). Summary by module:

| Module | Surface |
|---|---|
| `identity` | register, login, refresh, logout, profile, password reset, email verification |
| `course` | course/module/lesson CRUD, catalogue (keyset-paginated), publish, asset upload URLs (`POST /assets/upload-url`) |
| `enrollment` | enroll, withdraw, my-courses, roster (`POST /enrollments` honours `Idempotency-Key`, as do the other course POST routes; DR-023) |
| `progress` | mark lesson complete, progress, certificates (`GET /certificates`, `GET /certificates/{id}`, public `GET /certificates/verify/{code}`); issued by a `progress.course_completed` consumer in `services/course/certificate`, run by `course-worker` |
| `analytics` | platform metrics and per-course stats (`/analytics/*`) — populated asynchronously from Kafka events by `analytics-worker`, not written directly by any handler above |
| `audit` | admin failed-event list, inspect and replay (`/admin/failed-events`), served by `audit-api` |

Errors are always `{ "code", "message", "details" }` with a stable, documented `code` — never a raw stack trace or database error (see `libs/errors`).

## Tech stack

Go 1.26+ (toolchain 1.26.11 in CI and the images), Gin, GORM (bounded to simple CRUD — see [DR-004](docs/rfc/differences-from-requirements.md#dr-004--gorm-is-bounded-to-simple-crud-raw-sql-elsewhere)) + raw SQL for complex queries, PostgreSQL, Redis, MongoDB, Apache Kafka + Confluent Schema Registry, Temporal, Asynq, MinIO (S3-compatible — [ADR-0019](docs/adr/smartcourse-adr.md#adr-0019--minio-for-learning-asset-object-storage)), OpenTelemetry + Prometheus + Grafana + Jaeger, Docker Compose.

**Running in the compose stack** (checked end to end on 2026-11-25, see `docs/guides/local-stack.md`): Go, Gin, GORM, PostgreSQL (one database, a schema and a role per service), Redis (cache, rate limiting, Asynq), Kafka with Schema Registry and Protobuf events, MongoDB (audit archive and failed events), Temporal (course publishing), Mailpit (a local SMTP sink, not a mail provider) and, under the `observability` profile, Prometheus, Grafana and Jaeger. What is built versus designed is tracked in `docs/plans/week-3.md` (§10 progress log) and `docs/rfc/smartcourse-rfc.md` §21.0, which are not all revised for the split yet.

## Testing

Unit, integration (real Postgres via testcontainers), and concurrency tests, each documented — what they check and how to run them — in [`docs/guides/testing.md`](docs/guides/testing.md).

```bash
make test            # unit tests
make test-race       # everything, with the race detector (always on in CI)
make test-integration # integration + concurrency suite against real Postgres
make test-e2e        # container-tier end-to-end scenarios (Docker, several minutes; E2E_RUN=TestE2E_E01 for one)
```

## Where to start reading

**New to this codebase?** Start at [`docs/for-the-devs/GETTING_STARTED.md`](docs/for-the-devs/GETTING_STARTED.md) — laptop setup, how to run tests, the architecture in brief, and what to know before your first change. It's a better starting point than this README, which is a reference, not a walkthrough.

## Document map

| Document | What it's for |
|---|---|
| [`docs/for-the-devs/GETTING_STARTED.md`](docs/for-the-devs/GETTING_STARTED.md) | Onboarding: setup, tests, architecture in brief, prerequisite knowledge. Start here. |
| [`docs/baseline/SmartCourse.md`](docs/baseline/SmartCourse.md) | The original commissioning brief. Frozen; never edited. |
| [`docs/baseline/Execution Guidelines.md`](docs/baseline/Execution%20Guidelines.md) | The original execution/assignment guidelines. Frozen; never edited. |
| [`docs/prd/smartcourse-prd.md`](docs/prd/smartcourse-prd.md) | Product requirements: use cases, FR/NFR, traceability. |
| [`docs/rfc/smartcourse-rfc.md`](docs/rfc/smartcourse-rfc.md) | The architecture, in full, and why. |
| [`docs/rfc/differences-from-requirements.md`](docs/rfc/differences-from-requirements.md) | Every place the design adds to or reinterprets the frozen brief, and why. |
| [`docs/adr/smartcourse-adr.md`](docs/adr/smartcourse-adr.md) | Every individual architecture decision as a point-in-time record. |
| [`docs/guides/testing.md`](docs/guides/testing.md) | Test categories: what each checks, how to run it. |
| [`docs/plans/week-3.md`](docs/plans/week-3.md) | The Week 3 plan, decisions, and a dated progress log of what is done and what is not. |
| [`docs/plans/week-3-acceptance-matrix.md`](docs/plans/week-3-acceptance-matrix.md) | Every deliverable and how it will be proven; `make verify-deliverables` runs the live checks. |
| [`docs/plans/module-split-findings.md`](docs/plans/module-split-findings.md) | The 38 defects found by adversarial testing and how each was fixed. |
