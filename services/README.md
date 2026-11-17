# services/

One Go module per deployable service. Each service has its own `go.mod`, its own domain packages, and its own `cmd/<binary>/` entry point (ADR-0022, ADR-0023; the module split is in `docs/plans/module-split.md`). Shared code lives in [`libs/`](../libs/README.md) — also one module per package.

| Module | Packages inside | Binary | What it runs |
|---|---|---|---|
| `services/identity` | `identity` | `cmd/identity-api` | HTTP: registration, login, tokens, roles. Fully self-contained. |
| `services/course` | `course`, `enrollment`, `progress`, `workflows` | `cmd/course-api` | HTTP for courses, enrollment and progress, kept in **one** module and process because enrollment locks the `courses` row in the same transaction it writes an enrollment row in (ADR-0012). `workflows` holds the publish workflow the worker runs. |
| `services/analytics` | `analytics` | `cmd/analytics-api` | HTTP, read-only. Learns about the rest of the system through the relay/outbox. |
| `services/worker` | `audit`, `notification`, `search` (README-only plans) | `cmd/worker` | No HTTP. Runs the outbox relay and the Temporal worker for course publishing (ADR-0021, ADR-0025). |

[`tools/seed`](../tools/seed/README.md) is a separate module that drives the real service layer to populate a local database (`make seed`).

## Domain modules

Each domain package follows the same internal shape — `handler.go` (HTTP), `service.go` (domain logic, transactions, authorisation), `repository.go` (the only file importing GORM/raw SQL for that package), `dto.go` (request/response types — never the domain types), `domain.go` (entities). Read [`libs/README.md`](../libs/README.md) first; it explains the conventions every module below builds on.

| Module | Owns | Status |
|---|---|---|
| [`identity`](identity/identity/README.md) | Users, credentials, roles, refresh/reset/verification tokens | **Built** — see its README |
| [`course`](course/course/README.md) | Courses, versions, modules, lessons, publish lifecycle (now a Temporal workflow, M2) | **Built** |
| [`enrollment`](course/enrollment/README.md) | Enrollments, seat limits, prerequisites | **Built** — also a relay handler (`course.course_archived` → withdraw active enrollments), alongside `analytics` |
| [`progress`](course/progress/README.md) | Lesson progress, course completion | **Built** |
| [`analytics`](analytics/analytics/README.md) | Aggregates, platform/course-level counters | **Built (partial)** — pulled forward from M3 as the outbox relay's first handler (ADR-0021); 7 of the 9 FR-5 metrics live |
| `search` | A standalone search module (own schema, own relay handler, ranking strategy) | Not started as its own module — but `course_search_index`'s write (`course.Service.IndexSearch`) and read (`GET /courses/search`) are both real and live today, built inside `services/course/course` as part of the M2 publish workflow (ADR-0025). See `services/course/course/README.md`. |
| `notification` | Notification preferences and delivery records | Not started — M3 (Asynq-backed delivery) |
| `audit` | Event archive, audit log (MongoDB) | Not started — M3 |

`notification`, `search` and `audit` are README-only directories under `services/worker/` today, not abandoned work — see `docs/rfc/smartcourse-rfc.md` §21.0 for exactly what's deferred and why. They live under the worker because each is a relay consumer: register a new handler with the relay (`libs/relay`), same pattern as `analytics`, no new infrastructure required (`audit` still waits on MongoDB, which nothing in the codebase touches yet). If a standalone `search` module lands later with its own schema (the same path `analytics` took relative to its original inline position), `course`'s `IndexSearch` write is superseded by that module's own relay-driven handler, not duplicated alongside it — ADR-0025's reversal trigger.

## Boundaries: what keeps services apart

There are three layers, strongest first:

1. **Between services — Go modules.** `identity`, `course` and `analytics` can't import each other: a service's `go.mod` doesn't list the others, and `scripts/check-module-graph.sh` (`make check-module-graph`, run in CI) fails if one is added. Only `services/worker` (it runs `analytics` and `course` relay handlers and the publish workflow) and `tools/seed` may require other service modules. Libraries can't require any service.
2. **What each binary links — `scripts/check-link-graph.sh`** (`make check-link-graph`). `identity-api` and `analytics-api` must not link the Temporal SDK or another service; this is what the Week 2 review asked for.
3. **Inside `services/course` — `.golangci.yml` `depguard`.** `course`, `enrollment`, `progress` and `workflows` share one module because ADR-0012 needs them in one process, so Go can't separate them. No package there imports another's repository, domain, or GORM models (RFC §4.4); `course` never imports `workflows`.

Where one package genuinely needs data another owns *within the same database transaction and the same Postgres schema* (enrollment locking the `courses` row it doesn't "own," for instance — both live in the `course` schema, ADR-0024), it issues that SQL directly rather than calling into the other package's Go API — see `services/course/enrollment/README.md` for why that's the correct call, not a shortcut. Where a package needs another service's data across a schema boundary (`course` needing an instructor's display name from `identity`), it keeps its own small mirror table instead, kept current purely by consuming that service's relay events — never a cross-schema query (`services/course/course/handlers.go`, `services/analytics/analytics/handlers.go`).

## `workflows/`

Holds `PublishCourseWorkflow` and its `Activities` (RFC §7, ADR-0009/0025) — course publishing's Temporal workflow, built in M2. Not a domain module and not shaped like one (no `handler.go`/`dto.go`/`domain.go`): it's an orchestration layer over `services/course/course`'s own exported, Temporal-agnostic `Service` methods. It imports `course`; `course` never imports it back — see `course.PublishWorkflowName`'s doc comment for why that's the only direction the dependency can run. Only `course-api` starts the workflow (by name) and only `worker` runs it.

## The worker couples to two services — on purpose, for now

`services/worker` imports `services/analytics` and `services/course` for their relay handlers (`analytics.Handlers()`, `course.Handlers()`, `enrollment.Handlers()`) and the publish workflow. The relay claims an outbox row, runs every registered handler, and only then marks it published (ADR-0021), so splitting delivery across separate per-service relays would lose events unless each consumer tracked its own offset. That is the Kafka consumer-group model planned for M3. Until then the worker is the one place allowed to depend on more than one service, and whoever owns the worker owns that coupling (ADR-0026).

## Adding a new service or binary

1. Create `services/<name>/` with a `go.mod` (module path `github.com/AmeerHussainEmumba/SmartCourse/services/<name>`), `cmd/<binary>/main.go` and `wire.go`, and the domain package(s).
2. For every library you import, add a `require` **and a `replace`**, including the libraries *those* depend on — Go does not inherit `replace` directives. Run `go mod tidy` with `GOWORK=off`.
3. Add the module to `go.work`.
4. Add a rule to `scripts/check-link-graph.sh` and, if it may depend on another service, to `scripts/check-module-graph.sh`.
5. Add a `Dockerfile.<binary>` that copies only the modules it needs, and a compose service.
6. Add it to CODEOWNERS.
