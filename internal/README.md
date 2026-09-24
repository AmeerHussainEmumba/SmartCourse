# internal/

Everything that isn't a thin `cmd/` entry point lives here — Go's `internal/` convention means nothing outside this module can import these packages, which is what makes "SmartCourse's internals" a meaningful boundary at the language level, not just a folder convention.

Two kinds of things live here: the **shared kernel** (`platform/`) and eight **domain modules**, one per business capability (RFC §4.3). Every domain module follows the same internal shape — `handler.go` (HTTP), `service.go` (domain logic, transactions, authorisation), `repository.go` (the only file importing GORM/raw SQL for that module), `dto.go` (request/response types — never the domain types), `domain.go` (entities). Read `internal/platform/README.md` first; it explains the conventions every module below builds on.

## Domain modules

| Module | Owns | Status |
|---|---|---|
| [`identity`](identity/README.md) | Users, credentials, roles, refresh/reset/verification tokens | **Built** — see its README |
| [`course`](course/README.md) | Courses, versions, modules, lessons, publish lifecycle | **Built** |
| [`enrollment`](enrollment/README.md) | Enrollments, seat limits, prerequisites | **Built** |
| [`progress`](progress/README.md) | Lesson progress, course completion | **Built** |
| [`analytics`](analytics/README.md) | Aggregates, platform/course-level counters | **Built (partial)** — pulled forward from M3 as the outbox relay's first handler (ADR-0021); 5 of 10 FR-5 metrics live |
| `search` | Search index (`course_search_index`) | Not started — the natural next relay handler, same pattern as `analytics` |
| `notification` | Notification preferences and delivery records | Not started — M3 (Asynq-backed delivery) |
| `audit` | Event archive, audit log (MongoDB) | Not started — M3 |

The three unstarted modules are empty directories today, not abandoned work — see `docs/rfc/smartcourse-rfc.md` §21.0 for exactly what's deferred and why. `search` and `notification` are now straightforward additions: register a new handler with the relay (`internal/platform/relay`) the same way `analytics` did, no new infrastructure required. `audit` still waits on MongoDB, which nothing in the codebase touches yet.

## Why cross-module imports are restricted

No module imports another module's `repository`, `domain`, or GORM models (RFC §4.4) — enforced by `.golangci.yml`'s `depguard` rules, not just convention. Where one module genuinely needs data another module owns *within the same database transaction* (enrollment locking the `courses` row it doesn't "own," for instance), it issues that SQL directly rather than calling into the other module's Go API — see `internal/enrollment/README.md` for why that's the correct call, not a shortcut. `cmd/smartcourse/modules.go` is the one place in the whole codebase allowed to import every module — it's the composition root.

## `platform/`

The shared kernel every module depends on: config, logging, database/cache clients, the error-response contract, auth (JWT, argon2id, the three-layer authorization model), and the outbox writer. See `internal/platform/README.md`.

## `workflows/`

Will hold Temporal workflow and activity definitions for course publishing (RFC §7). Empty today — M2 scope. `course.Service.Publish` currently does this work synchronously, with the seam to swap in `StartWorkflow` explicitly marked in its doc comment.
