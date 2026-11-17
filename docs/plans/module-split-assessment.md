# Assessment: where the project stands after the module split

**Date:** 2026-11-17 · **Branch:** `refactor/module-split-tests` · **Companion to:** `docs/plans/module-split.md`, ADR-0026, DR-020

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
| 2.6 | Course marked Ready; partial failures do not corrupt | Met | `state: ready` observed live after the split; blue-green promotion is the last step (ADR-0010), and injected activity faults left the live version intact. The adversarial phase found search exposing a course whose publish failed (F-08); fixed, with regression tests, so search now joins on the published version |
| 3.1 | Enrollment recorded, progress initialised, analytics updated | Met | |
| 3.2 | Enrollment triggers notifications | Not built | M3 |
| 3.3 | Idempotency, backpressure, recovery, concurrency without inconsistency | Met | The enroll path held under 150 contenders for 25 seats. The adversarial phase found the **withdraw** path did not (was Partial until fixed): a concurrent double withdraw decrements `enrollment_count` twice and emits two events, leaking seats (F-01, found independently by both testers). Concurrent final lessons could leave an enrollment `active` forever (F-06), and the relay released its claim lock before handlers ran, so two replicas could run one event concurrently (F-04). All three are fixed (ADR-0027 for the relay) and covered by regression tests |
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
| 7.5 | No race conditions, deadlocks, goroutine leaks | Met | The Go race detector passes and no goroutine or connection leaks were found (1,600 cancelled requests; `goleak` on the relay). The adversarial phase also found application-level races the race detector cannot see (F-01, F-06, F-14 reset tokens redeemed concurrently, F-22, F-23); all fixed with regression tests |
| T | Go 1.25+, Gin, GORM, PostgreSQL, Redis, Temporal, Docker/Compose | Met | Go 1.26.5; GORM bounded to simple CRUD (DR-004) |
| T | MongoDB, Kafka, Schema Registry, Asynq or RabbitMQ | Not built | M3 |
| T | Prometheus, Grafana, Jaeger, OpenTelemetry | Not built | M4 |
| Out | PRD with use cases, requirements, milestones, traceability | Met | `docs/prd/smartcourse-prd.md` |

Count of the 32 rows above: **19 Met, 7 Partial, 6 Not built.** (Every row counts once; the two 7.3 rows and the three tech-stack rows count separately.) Three rows (2.6, 3.3, 7.5) were **downgraded to Partial when the adversarial testers broke them, and restored to Met once the defects were fixed and regression-tested**. The Met status therefore rests on one adversarial pass, not on a long track record. The Not built rows are all milestones the project has openly scheduled (M3 or M4) except `sync.RWMutex`, which was a deliberate decision logged as DR-016.

## B. Execution Guidelines

| Ask | Status | Note |
|---|---|---|
| Organised structure; clear module/service separation; meaningful commits | Met | this is the area the module split improves most |
| README: overview, architecture diagram, setup, API overview, tech stack | Met | one Mermaid diagram in `README.md`; the module-split layout section was rewritten |
| Technical documentation: service/module breakdown, data and event flow, design decisions, assumptions and trade-offs | Met | RFC, 26 ADRs, DR log (DR-020 added) |
| Week 1: service structure, schema, basic APIs, local setup | Met | marked complete by the reviewer |
| Week 2: enrollment, Temporal publishing, state transitions | Met with caveats | reviewer: "fine with minor fixes on elevate"; those fixes have not been seen. Gap against the brief's wording: `VerifyAssets` (A, 2.2). The concurrent-withdraw defect that dented "enrollment flow working reliably" (F-01) is fixed |
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

## E. Test evidence

| What | Result |
|---|---|
| Full integration suite, every module, `-race -tags=integration`, on the final code | passes (exit 0), 19 modules |
| Link-graph, module-graph, Dockerfile-modules, module-hygiene guards | pass |
| `scripts/test-guards.sh` (injects 9 violations; checks the right guard fails) | 18 passed, 0 failed |
| Static policy scans `check-dev-defaults.sh`, `check-http-hardening.sh` | pass (both failed before the fixes) |
| `go vet -tags=integration` in every module; `gofmt` | clean |
| `golangci-lint` v2.14.0 in every module | 0 issues when run after the split; not re-run after the bug fixes (CI runs it) |
| Standalone `GOWORK=off` build of all 19 modules; four Docker images (non-root) | pass |
| Live stack on a freshly reset database from the final images: migrations including the new analytics one, full seed, health, security headers, uniform register 202, login, `/me`, 413 on a 2 MiB body, spoofed `X-Forwarded-For` now rate-limited, non-owner 404, analytics rules (student: platform totals and enrolled courses only), worker log quiet after startup | pass |
| Tests | 314 test functions now (125 before the adversarial phase) plus 4 fuzz targets; 48 new test files |
| Bug reproducers | all 46 now pass and run as normal regression tests; no `bugrepro`-tagged test remains |
| Defects found by the adversarial phase | **36 found, 36 fixed** (`docs/plans/module-split-findings.md`, with how each was fixed and what stays imperfect) |
| Not verified | CI on a GitHub runner; a real Temporal server under fault; a true `kill -9` of the relay; dependency CVEs; a second adversarial pass after the fixes |

Two things went wrong and were caught while verifying, which is worth recording: two tests asserted pre-fix behaviour once the real request-body cap applied (a 1 MiB token and a 1 MiB password now get 413), and a relay test checked too early after the new retry cooldown. Both were test assumptions, not product defects, and were corrected without weakening what they assert.

## F. Rating

These are one engineer's judgment from the evidence above, not measurements. They are meant to be argued with.

| Against | Standing | Why |
|---|---|---|
| **The module-split review comments** | **Strong, with two open confirmations.** Done: per-service wiring, a `go.mod` per service, no shared bootstrap, `identity-api` carries no Temporal code (589 to 388 packages, 61 to 0 Temporal), guarded in CI. Different from the literal ask: one module per library instead of a single `libs/common` (DR-020), unconfirmed with the supervisor. Open: the worker still imports two services' handler packages until M3; the "minor fixes on elevate" are unseen | The structural complaint is fixed and measurable. The two judgment calls were made on the project's side |
| **Baseline requirements (32 rows)** | **19 Met, 7 Partial, 6 Not built.** About 60% fully met | The Not built rows are the Week 3 and observability stack the project has openly scheduled (Kafka, Schema Registry, MongoDB, Asynq, notifications, OpenTelemetry/Prometheus/Jaeger) plus a deliberate `sync.RWMutex` decision. The Partial rows are where code exists but the brief's wording is not fully satisfied (asset verification, 7 of 9 analytics metrics, traceability, monitoring) |
| **Execution Guidelines: Week 1** | **Complete** | Reviewer-confirmed |
| **Execution Guidelines: Week 2** | **Complete in scope** | Temporal publishing and enrollment work and are well tested, including under adversarial attack; the one gap against the brief's wording is `VerifyAssets` |
| **Execution Guidelines: Week 3** | **Not started** | One documentation-only branch |
| **Documentation** | **Strong** | RFC, 27 ADRs, a DR log (DR-020, DR-021), READMEs per module, three onboarding docs, a testing guide, a versioning guide, a findings register, this assessment |
| **Robustness of what is built** | **Good, and better evidenced than before.** Two adversarial passes' worth of defects were fixed and are covered by regression tests; nothing found was left open | What remains is listed as imperfect, not open: instructor ownership of analytics, unbounded argon2 concurrency, no dead-letter store, plus the judgment calls awaiting confirmation (integer bounds, rate-limit numbers) |

**Overall.** The architecture, documentation and now the implementation's robustness are in good shape for the first two weeks of the brief. The module split is complete and verified. About two of the three weeks of the brief are in place; the third (events and observability) has not started. The honest summary for a reviewer is: foundations and structure good, core flows work and have been attacked, Week 3 absent, and a short list of known, documented trade-offs.

## G. What to do next (suggested order)

1. **Confirm the judgment calls** the fixers made (listed in the findings register): integer bounds, rate-limit numbers, 404 for a withdrawn student on analytics, self-demotion allowed when another admin exists.
2. **Decide whether instructors should see only their own courses' analytics.** It needs `instructor_id` added to the `course_published` event, a mirror column and a backfill; until then instructors can read any course.
3. **Decide whether Most Popular Courses and Failed Events are Week 3 scope** (the "ten metrics" miscount itself is corrected, DR-021), and whether to add a dead-letter store before M3.
4. **Ask the supervisor** the two open questions in DR-020, and whether the unseen Week 2 "minor fixes" overlap with the defects found here.
5. **Run a second adversarial pass after the next feature** (republish and Kafka are the likely sources of new defects), and set `APP_ENV` and `TRUSTED_PROXIES` explicitly in any real deployment.
