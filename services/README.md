# services/

One Go module per service. Each service has its own `go.mod`, its own domain packages, and one to three `cmd/<binary>/` entry points (ADR-0022, ADR-0023; the module split is in `docs/plans/module-split.md`, the independence split in `docs/plans/microservices-split.md` and ADR-0033 to ADR-0035). Shared code lives in [`libs/`](../libs/README.md), also one module per package. **A service imports only `libs/*`, never another service.**

There are six services and eleven binaries (ten long-running processes and one CLI):

| Module | Binaries (`cmd/`) | Owns | What it runs |
|---|---|---|---|
| [`services/identity`](identity/README.md) | `identity-api`, `identity-worker`, `identity-admin` (CLI) | Postgres schema `identity` (incl. `identity.outbox`) | HTTP for registration, login, tokens and roles; the identity outbox relay, the token-version re-seeder and outbox pruning; the CLI that creates the first admin. |
| [`services/course`](course/README.md) | `course-api`, `course-worker` | schema `course` (incl. `course.outbox`, `course.processed_events`, `course.idempotency_keys`) | HTTP for courses, enrollment, progress and certificates, kept in **one** module and process because enrollment locks the `courses` row in the same transaction it writes an enrollment row in (ADR-0012). The worker runs the course relay, the Kafka consumers (instructor mirror, enrollment archival, certificate issuer), the scheduled jobs and the Temporal **activities**. |
| [`services/analytics`](analytics/README.md) | `analytics-api`, `analytics-worker` | schema `analytics` (incl. `analytics.processed_events`) | Read-only HTTP; the worker consumes events into mirrors and aggregates and runs the reconciler. Emits no events. |
| [`services/notification`](notification/README.md) | `notification-worker` | schema `notification` (incl. `notification.processed_events`) | No HTTP API. Consumes events, writes delivery rows, sweeps and sends mail through Asynq and SMTP (Mailpit locally). Emits no events. |
| [`services/publishing`](publishing/README.md) | `publishing-worker` | nothing (no database) | The Temporal worker that runs `PublishCourseWorkflow` by orchestration only; the activities run in `course-worker` (ADR-0035). |
| [`services/audit`](audit/README.md) | `audit-api`, `audit-worker` | MongoDB (`events`, `audit_log`, `failed_events`); no Postgres | The archiver consumes every topic; the API lists failed events and replays them by republishing to `retry.<consumer>` (ADR-0034). |

[`tools/seed`](../tools/seed/README.md) is a separate module and a plain HTTP client of the public APIs (`make seed`). [`test/e2e`](../test/README.md) is a separate module that builds and runs the binaries as processes and talks to them over HTTP, Kafka, Postgres, MongoDB and Redis; it requires no service module either.

Every long-running binary is `libs/runtime.Main`: configuration (`config.LoadWith`, only the variable groups that binary needs), telemetry, signals and graceful shutdown, and an **internal** listener (`METRICS_HTTP_ADDR`, default `:9100`) with `/metrics`, `/health/live` and `/health/ready`. `/metrics` is never on a public API port. Each binary's README lists the variables it requires and the ones it does not read.

Kafka is the only integration transport. Each service that writes events has its own outbox and a publish-only relay in its worker; each service that consumes events runs its own consumers (one Kafka consumer group per consumer name, plus `retry.<consumer>`) with the handler and a `processed_events` marker in one transaction. Each service connects to Postgres as its own role (`svc_identity`, `svc_course`, `svc_analytics`, `svc_notification`) with `POSTGRES_SEARCH_PATH=<schema>,public`; the shared `platform` schema no longer exists (`migrations/README.md`). Each worker that uses Asynq prefixes its queues with the service name (`identity:low`, `notification:critical`, ...) so workers sharing one Redis database never take each other's tasks.

## Domain packages

Each domain package follows the same internal shape: `handler.go` (HTTP), `service.go` (domain logic, transactions, authorisation), `repository.go` (the only file importing GORM/raw SQL for that package), `dto.go` (request/response types, never the domain types), `domain.go` (entities). Read [`libs/README.md`](../libs/README.md) first; it explains the conventions every module below builds on.

| Package | Owns | Notes |
|---|---|---|
| [`identity/identity`](identity/identity/README.md) | Users, credentials, roles, refresh/reset/verification tokens | Produces `identity.*` events. |
| [`course/course`](course/course/README.md) | Courses, versions, modules, lessons, publish lifecycle, catalogue and search | Consumes identity events into `instructor_mirror`. `course.Service.Publish` starts the workflow by name. |
| [`course/enrollment`](course/enrollment/README.md) | Enrollments, seat limits, prerequisites | Consumes `course.course_archived` (withdraws active enrollments). |
| [`course/progress`](course/progress/README.md) | Lesson progress, course completion | Produces `progress.*` events. |
| [`course/certificate`](course/certificate/README.md) | Certificates issued once per completed enrollment; owner-only list and get; public verification by code | Consumes `progress.course_completed` (DR-025). |
| [`course/activities`](course/activities/README.md) | The Temporal activities of the publish workflow | Thin wrappers over `course.Service`; registered by `course-worker` under the names in `libs/contracts/publishing`. |
| [`analytics/analytics`](analytics/analytics/README.md) | Mirrors, aggregates and the nine FR-5 metrics | The Failed Events metric reads the audit store's MongoDB collection read-only through `libs/mongostore` (ADR-0034's documented exception). |
| [`notification/notification`](notification/notification/README.md) | Recipient mirror, delivery records, mail provider | No real mail provider, by decision. |
| `publishing/publish` | `PublishCourseWorkflow` and its registration | Orchestration only (see its service README). |
| `audit/{archive,deadletter,replay,admin,store}` | Archiver, park sink, replay, admin HTTP, lazy MongoDB | See the audit service README. |

`search` is not a standalone service: `course_search_index`'s write (`course.Service.IndexSearch`, a publish activity) and read (`GET /courses/search`) live inside `services/course/course` (ADR-0025; `docs/rfc/smartcourse-rfc.md` section 21.0 lists what is deferred). A standalone search service would consume `course.*` events with its own schema and replace `IndexSearch`, not duplicate it (ADR-0025's reversal trigger).

## Boundaries: what keeps services apart

Four layers, strongest first:

1. **Between services: Go modules.** A service's `go.mod` may list only `libs/*`. `scripts/check-module-graph.sh` (`make check-module-graph`, run in CI) fails if any service, `tools/seed` or `test/e2e` requires a service module, or a library requires a service or tool.
2. **What each binary links: `scripts/check-link-graph.sh`** (`make check-link-graph`). One rule per binary: forbidden package prefixes (for example `course-api` must not link Kafka, Asynq, MongoDB or `libs/jobs`; `publishing-worker` must not link a database driver, Kafka, Asynq or MongoDB) and a package budget.
3. **The database: per-service Postgres roles.** A role has rights on its own schema only, so a query that crosses a schema fails with `permission denied`. `libs/testenv` proves it for every role pair (`TestRolesCannotTouchOtherServiceSchemas`).
4. **Inside `services/course`: `.golangci.yml` `depguard`.** `course`, `enrollment`, `progress`, `certificate` and `activities` share one module because ADR-0012 needs them in one process, so Go cannot separate them. No package there imports another's repository, domain, or GORM models (RFC §4.4).

Where one package needs data another owns *within the same transaction and the same schema* (enrollment locking the `courses` row, both in the `course` schema, ADR-0024), it issues that SQL directly rather than calling the other package's Go API; see `course/enrollment/README.md`. Where a service needs another service's data, it keeps a small event-fed mirror table (`instructor_mirror`, analytics' `*_mirror`, notification's `recipients`), never a cross-schema query and never a call into the other service's code.

## The publish workflow is split on purpose

`PublishCourseWorkflow` lives in `services/publishing` and has no database. Its activities live in `services/course/activities` and run in `course-worker`. The workflow calls every activity by name on the `course-activities` queue; `course-api` starts the workflow by name on `publish-workflows`. The names, queues and DTOs are the wire-format contract in `libs/contracts/publishing` (ADR-0035). Neither side imports the other.

## Adding a new service or binary

1. Create `services/<name>/` with a `go.mod` (module path `github.com/AmeerHussainEmumba/SmartCourse/services/<name>`), `cmd/<binary>/main.go` and `wire.go` (build on `libs/runtime`), and the domain package(s).
2. For every library you import, add a `require` **and a `replace`**, including the libraries *those* depend on (Go does not inherit `replace` directives). Run `go mod tidy` with `GOWORK=off`.
3. Add the module to `go.work`.
4. Add a rule per binary to `scripts/check-link-graph.sh`. `scripts/check-module-graph.sh` needs no change unless you break the rule that a service requires only libs.
5. Add a `Dockerfile.<binary>` that copies only the modules it needs (`scripts/check-dockerfile-modules.sh`), a compose service, a scrape job in `observability/prometheus/prometheus.yml`, and the binary to the E2E harness (`test/e2e/harness/topology.go`).
6. If it writes events it needs its own outbox table, if it consumes events its own `processed_events` (a migration set under `migrations/<name>/`), a `svc_<name>` role in `scripts/db-roles.sh`, and a consumer name in `libs/kafka/topics` (which also creates its retry topic).
7. Add it to CODEOWNERS, and give the module a README and a CHANGELOG.
