# Assessment: where the project stands after the module split

**Date:** 2026-11-17 · **Branch:** `refactor/module-split-docs` · **Companion to:** `docs/plans/module-split.md`, ADR-0026, DR-020

## Purpose

Phase 7 of the module-split plan: re-check the result against the frozen baseline (`docs/baseline/SmartCourse.md`, `docs/baseline/Execution Guidelines.md`) and the Week 2 review, confirm the split did not stray from either, and give a rating that does not flatter. Nothing here is a claim that a control or requirement is "done" on anyone's behalf: each row says what was checked and how.

## Method, and its limits

- **Verified against the code in this session:** concurrency primitives in production code (grep over non-test files), the unbuilt items (Kafka, MongoDB, Asynq, OpenTelemetry, Prometheus, asset upload, `Idempotency-Key` handling), certificate issuance, analytics routes and stored metrics, what `VerifyAssets` actually checks, enrollment prerequisites, and the link counts per binary.
- **Taken from the repo's own documents without independent re-verification:** the detail of each M2 workflow activity, the relay bug history, and the coverage claims in `docs/guides/testing.md`. The test results below come from running the suites, not from those claims.
- **Not verifiable from here:** GitHub Actions behaviour. The CI workflow was edited (per-module loops, install-only lint action, module-graph step) and validated as YAML and by running every step locally, but has not run on a GitHub runner.
- **Ratings are one engineer's reading of the documents.** The baseline is short and says little about structure; where it is silent, the row says so.

## A. Baseline requirements (`SmartCourse.md`)

Status words: **Met** (built and exercised by tests or a live run), **Partial** (some of the ask is built), **Not built** (planned, with the milestone that owns it), **n/a** (baseline silent).

| # | Requirement | Status | Evidence / gap |
|---|---|---|---|
| 1.1 | Create/update courses, modules, lessons | Met | `services/course/course` (14 routes), integration and HTTP tests |
| 1.2 | Learning assets | Partial | MinIO container and bucket exist (ADR-0019); the upload-URL endpoint is not wired (no `upload-url` in production code) |
| 1.3 | Registration with roles (student/instructor/admin) | Met | `services/identity/identity` (11 routes), seeded and logged in live |
| 1.4 | Enrollment rules: duplicates, limits, prerequisites, history | Met | partial unique index, row lock (ADR-0012), `prerequisitesSatisfied`, withdrawn rows kept |
| 1.5 | Consistency across the system on each update/enrollment | Met | one transaction plus outbox (ADR-0006); full integration suite passes after the split |
| 2.1 | Publish: metadata validated | Met | `ValidateMetadata` activity |
| 2.2 | Publish: **assets verified** | **Partial** | `VerifyAssets` checks that no lesson's content exceeds a size limit. It does not verify uploaded files, because the upload endpoint does not exist. The README/RFC call it a "narrower real check in the same graph position"; that is accurate, but it is not what the brief's wording asks for |
| 2.3 | Publish: search index updated | Met | real `course_search_index` upsert (ADR-0025) |
| 2.4 | Publish: cache refreshed | Met | `WarmCache`, Redis |
| 2.5 | Publish: analytics initialised | Met | `course.course_published` consumed by analytics |
| 2.6 | Course marked Ready; partial failures do not corrupt | Met | `state: ready` observed live through course-api after the split; blue-green promotion is the last step (ADR-0010) |
| 3.1 | Enrollment recorded, progress initialised, analytics updated | Met | |
| 3.2 | Enrollment triggers notifications | Not built | M3 |
| 3.3 | Idempotency, backpressure, recovery, concurrency | Met | unique index; bounded relay channel; retry and give-up threshold; seat-limit concurrency test |
| 4.1 | Async processing: publishing, analytics, cache, search | Met | Temporal workflow and the in-process relay (ADR-0021/0025) |
| 4.2 | Async processing: notification delivery | Not built | M3 (Asynq) |
| 4.3 | Traceable, recoverable, no duplicate processing | Partial | recoverable and de-duplicated (`processed_events`); not traceable beyond logs until OpenTelemetry (M4) |
| 4.4 | Kafka event backbone | Not built | M3. The relay is a deliberate in-process stand-in (ADR-0021) |
| 5 | Analytics metrics (the baseline names nine) | **Partial: 7 of 9** | Total students, total instructors, total courses published, average courses per student (`platform_snapshot`); completion rate, average completion time (`course_stats`); new enrollments over time (`daily_enrollments`). **Missing: Most Popular Courses, Failed Events.** The PRD and README say "ten metrics"; the baseline lists nine plus an empty tenth bullet, so "7 of 10" overstates the denominator |
| 6.1 | Clear separation of responsibilities | Met | strengthened by this work: one Go module per service, enforced by the module graph and two CI guards |
| 6.2 | Monitoring and logging | Partial | structured JSON logging yes; Prometheus/Grafana/Jaeger/OpenTelemetry exist only as compose profiles and code comments (M4) |
| 6.3 | Diagnose failures in publishing, enrollment, background tasks | Partial | `publish-status`, logs, Temporal UI; no metrics or traces |
| 7.1 | Concurrent publishing with goroutines, WaitGroups, channels, context | Met | `errgroup` in `runPublishChecks`; Temporal activity graph; context throughout |
| 7.2 | Bounded worker pool, queues, graceful shutdown, retries, backpressure | Met | `libs/relay`, with tests for bounded concurrency, idempotency, poison events, shutdown drain |
| 7.3 | `sync.Mutex`, `sync/atomic` | Met | in `libs/relay` (2 and 1 production files) |
| 7.3 | **`sync.RWMutex`** | **Not built** | zero production uses. Logged as DR-016: forcing one in would be the anti-pattern ADR-0021 warns about. The baseline lists it as something the implementation "must demonstrate" |
| 7.4 | Channel pipelines, fan-out/fan-in | Partial | channel-based job dispatch; no genuine fan-out/fan-in pipeline beyond it |
| 7.5 | No race conditions, deadlocks, goroutine leaks | Met so far | `-race` is on in CI and passes; leak testing is part of Phase 5 (results in section D) |
| T | Go 1.25+, Gin, GORM, PostgreSQL, Redis, Temporal, Docker/Compose | Met | Go 1.26.5; GORM bounded to simple CRUD (DR-004) |
| T | MongoDB, Kafka, Schema Registry, Asynq or RabbitMQ | Not built | M3 |
| T | Prometheus, Grafana, Jaeger, OpenTelemetry | Not built | M4 |
| Out | PRD with use cases, requirements, milestones, traceability | Met | `docs/prd/smartcourse-prd.md` |

Count of the 32 rows above: 19 Met, 7 Partial, 6 Not built. (Every row counts once; the two 7.3 rows and the three tech-stack rows count separately. "Met so far" in 7.5 is counted as Met.) The Not built rows are all milestones the project has openly scheduled (M3 or M4) except `sync.RWMutex`, which was a deliberate decision logged as DR-016.

## B. Execution Guidelines

| Ask | Status | Note |
|---|---|---|
| Organised structure; clear module/service separation; meaningful commits | Met | this is the area the module split improves most |
| README: overview, architecture diagram, setup, API overview, tech stack | Met | one Mermaid diagram in `README.md`; the module-split layout section was rewritten |
| Technical documentation: service/module breakdown, data and event flow, design decisions, assumptions and trade-offs | Met | RFC, 26 ADRs, DR log (DR-020 added) |
| Week 1: service structure, schema, basic APIs, local setup | Met | marked complete by the reviewer |
| Week 2: enrollment, Temporal publishing, state transitions | Met with caveats | reviewer: "fine with minor fixes on elevate"; those fixes have not been seen. `VerifyAssets` (A, 2.2) is the one gap against the brief's wording |
| Week 3: Kafka, background workers (Asynq), analytics, observability | Not started | `feat/m3-events-phase1` holds documentation commits only |

## C. The Week 2 review comments

| Comment | What was done | Status |
|---|---|---|
| Every binary imports `bootstrap`, so each image builds everyone's code | `bootstrap` deleted; per-service `wire.go`; measured: `identity-api` 589 to 388 packages, Temporal 61 to 0; `analytics-api` 589 to 386, 61 to 0; `course-api` 581; `worker` 586. `scripts/check-link-graph.sh` fails if that regresses (verified to fail against the pre-change code) | Done |
| Move handlers, services, repositories into per-service folders, each with its own `go.mod` | `services/{identity,course,analytics,worker}`, `tools/seed`, each its own module; root `go.mod` removed | Done |
| Keep shared code in `libs/common` | Split further: **one module per library package** (13 plus `libs/testenv`) instead of a single `libs/common`. Chosen for future per-owner control | **Done differently from the literal ask; not confirmed with the supervisor** (DR-020) |
| Wire each service in its own `main.go`, not the shared bootstrap | each `cmd/<binary>` has its own `main.go` and `wire.go` | Done |
| "A break in `internal/course` breaks `identity-api`'s compile" | `identity` does not depend on `course`; `check-module-graph.sh` fails CI if it ever does | Done |
| "The Temporal SDK and every other library come along" | `identity-api` and `analytics-api` link no Temporal code. `course-api` and `worker` do, because they use it | Done |
| Week 2 "minor fixes on elevate" | not seen, so not addressed | **Open** |

What this work did **not** do, and the supervisor may expect: per-service event consumption (the worker still imports the `course` and `analytics` handler packages; deferred to M3 Kafka, ADR-0026), per-service migrations folders, tagged module versions (mechanics built, nothing tagged).

## D. Did the split stray from the baseline or the earlier ADRs?

| Invariant | Evidence it held |
|---|---|
| Services independently deployable (ADR-0022/0023) | four images built from only the modules each needs, with `GOWORK=off`; stack run from them, all three APIs ready |
| Seat-limit transaction (ADR-0012) | `course`, `enrollment`, `progress` kept in one module; the concurrency test passes after the split |
| Schema per service (ADR-0024) | unchanged; migrations untouched |
| Outbox and relay semantics (ADR-0006/0021) | unchanged; relay tests pass |
| Publishing as a workflow (ADR-0025) | live: 202 from course-api, workflow ran in the worker, state `ready` |
| No behaviour change | the full integration suite passed at every phase |

Test results from the adversarial phase are recorded in section E once it completes.

## E. Rating

*(Filled in after the adversarial test phase; see below.)*
