# internal/

Everything that isn't a thin `cmd/` entry point lives here — Go's `internal/` convention means nothing outside this module can import these packages, which is what makes "SmartCourse's internals" a meaningful boundary at the language level, not just a folder convention.

Two kinds of things live here: the **shared kernel** (`platform/`) and eight **domain modules**, one per business capability (RFC §4.3). Every domain module follows the same internal shape — `handler.go` (HTTP), `service.go` (domain logic, transactions, authorisation), `repository.go` (the only file importing GORM/raw SQL for that module), `dto.go` (request/response types — never the domain types), `domain.go` (entities). Read `internal/platform/README.md` first; it explains the conventions every module below builds on.

## Domain modules

| Module | Owns | Status |
|---|---|---|
| [`identity`](identity/README.md) | Users, credentials, roles, refresh/reset/verification tokens | **Built** — see its README |
| [`course`](course/README.md) | Courses, versions, modules, lessons, publish lifecycle (now a Temporal workflow, M2) | **Built** |
| [`enrollment`](enrollment/README.md) | Enrollments, seat limits, prerequisites | **Built** — also the relay's second handler (`course.course_archived` → withdraw active enrollments), alongside `analytics` |
| [`progress`](progress/README.md) | Lesson progress, course completion | **Built** |
| [`analytics`](analytics/README.md) | Aggregates, platform/course-level counters | **Built (partial)** — pulled forward from M3 as the outbox relay's first handler (ADR-0021); 7 of 10 FR-5 metrics live |
| `search` | A standalone search module (own schema, own relay handler, ranking strategy) | Not started as its own module — but `course_search_index`'s write (`course.Service.IndexSearch`) and read (`GET /courses/search`) are both real and live today, built inside `internal/course` as part of the M2 publish workflow (ADR-0025) rather than waiting for this module. See `internal/course/README.md`. |
| `notification` | Notification preferences and delivery records | Not started — M3 (Asynq-backed delivery) |
| `audit` | Event archive, audit log (MongoDB) | Not started — M3 |

`notification` and `audit` are empty directories today, not abandoned work — see `docs/rfc/smartcourse-rfc.md` §21.0 for exactly what's deferred and why. `notification` is a straightforward addition when it lands: register a new handler with the relay (`internal/platform/relay`), same pattern as `analytics`, no new infrastructure required. `audit` still waits on MongoDB, which nothing in the codebase touches yet. If a standalone `search` module lands later with its own schema (the same path `internal/analytics` took relative to its original inline position), `internal/course`'s `IndexSearch` write is superseded by that module's own relay-driven handler, not duplicated alongside it — ADR-0025's reversal trigger.

## Why cross-module imports are restricted

No module imports another module's `repository`, `domain`, or GORM models (RFC §4.4) — enforced by `.golangci.yml`'s `depguard` rules, not just convention. Where one module genuinely needs data another module owns *within the same database transaction and the same Postgres schema* (enrollment locking the `courses` row it doesn't "own," for instance — both live in the `course` schema, ADR-0024), it issues that SQL directly rather than calling into the other module's Go API — see `internal/enrollment/README.md` for why that's the correct call, not a shortcut. Where a module needs another's data across a schema boundary (`course` needing an instructor's display name from `identity`), it keeps its own small mirror table instead, kept current purely by consuming that module's relay events — never a cross-schema query (`internal/course/handlers.go`, `internal/analytics/handlers.go`). `internal/platform/bootstrap` is the one place in the whole codebase allowed to import every module — it's the composition root, imported in turn by `cmd/identity-api`, `cmd/course-api`, `cmd/analytics-api`, and `cmd/worker` (ADR-0022, ADR-0023, ADR-0024).

## `platform/`

The shared kernel every module depends on: config, logging, database/cache clients, the error-response contract, auth (JWT, argon2id, the three-layer authorization model), and the outbox writer. See `internal/platform/README.md`.

## `workflows/`

Holds `PublishCourseWorkflow` and its `Activities` (RFC §7, ADR-0009/0025) — course publishing's Temporal workflow, built in M2. Not one of the eight domain modules above and not shaped like one (no `handler.go`/`dto.go`/`domain.go`): it's an orchestration layer over `internal/course`'s own exported, Temporal-agnostic `Service` methods, the same way `internal/platform/bootstrap` is an orchestration layer over every domain module's wiring. It imports `internal/course`; `course` never imports it back — see `course.PublishWorkflowName`'s doc comment for why that's the only direction the dependency can run.
