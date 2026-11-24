# Plan: Week 3 (events, background workers, analytics, observability), rough edges, E2E, and a deliverables audit

**Status:** scope confirmed by the project owner 2026-11-18; execution starts with the review-independent work (section 4a) · **Date:** 2026-11-18 · **Base:** `refactor/module-split-fixes` (PR #18) · **Companions:** RFC §6, §9, §12, §21; ADR-0006/0007/0008/0013/0014/0017/0021; DR-018, DR-019; `docs/plans/module-split-assessment.md`

## 1. What "done" means

**Week 3 is the last stage before external review and a defense.** Nothing is deferred past it: the project owner has confirmed that observability, certificates and asset upload are all in scope. The cut line in section 8 is therefore an ordering (what to finish first so the most important work is safe if time runs short), not permission to drop things; anything that does not make it is recorded as Not built with the reason, never left implicit.


Week 3 is done when every row of the acceptance matrix (section 7) has a passing check that someone other than the author can re-run, and the final assessment re-rates the project against the baseline, the Execution Guidelines and the supervisor's reviews with no row resting on "believed to work".

Week 3's deliverables in the Execution Guidelines are three sentences, and each has to be demonstrable:

| Guideline deliverable | What we will be able to show |
|---|---|
| Event-driven flows working | An enrollment, a publish, a withdrawal and a lesson completion each travel outbox → Kafka → consumers, with duplicates, restarts and poison messages handled |
| Analytics pipeline initialized | All nine baseline metrics served, kept current by events, and self-corrected by a scheduled reconciliation |
| Basic monitoring and tracing available | One Jaeger trace across HTTP → outbox → Kafka → consumer → job; Prometheus metrics; a provisioned Grafana dashboard |

## 2. Starting point (verified in this repo on 2026-11-18)

- The outbox relay (`libs/relay`) polls `outbox`, runs in-process handlers with `processed_events` idempotency, and parks nothing: a poison event past max attempts is marked published and logged (ADR-0021).
- Handlers exist for analytics (`services/analytics`) and enrollment-course-archival (`services/course/enrollment`).
- Kafka, Schema Registry and MongoDB are **compose profile entries only** (`events` profile); no code uses them. Prometheus, Grafana and Jaeger are likewise profile entries (`observability`).
- `services/worker/{notification,audit,search}` contain only READMEs.
- The `outbox` table has `schema_version` but **no `trace_id`/`span_id` columns**, so trace propagation (RFC §12.2) needs a migration.
- Analytics serves 7 of 9 metrics. Missing: Most Popular Courses, Failed Events.
- Known imperfections carried over (assessment, section 6): instructor analytics ownership, unbounded argon2 concurrency, no dead-letter store, 15-minute publish lease, brief `token_stale` window after a Redis flush, `VerifyAssets` only checks content size, no `Idempotency-Key` middleware, no fan-out/fan-in pipeline.

## 3. Decisions this plan makes (ADRs 0029 to 0032 drafted as Proposed on 2026-11-18; each reversible)

Confirmed by the project owner on 2026-11-18: D1 (observability in Week 3), D6 (Mailpit as the notification sink, no real mail provider or SMTP integration, documented as a deliberate limit), certificates and asset upload in scope.

| # | Decision | Reason | Supersedes / touches |
|---|---|---|---|
| D1 | **Observability is in Week 3**, not deferred to a later milestone | The guideline's third deliverable is "basic monitoring and tracing available"; DR-019 deferred it and itself says to supersede it if the reviewer reads that line as required. The risk of being wrong is higher than the cost of a thin slice | Supersedes DR-019 |
| D2 | **Two event transports behind one seam**: `EVENT_TRANSPORT=inprocess` (today's relay; the `core` compose profile has no Kafka, ADR-0018) and `kafka` (events profile and CI e2e) | Keeps the fast dev loop and the existing tests; gives a rollback path; lets the same handlers be tested on both | ADR-0021 reversal trigger |
| D3 | The relay's "publish" step becomes a Kafka producer; **the relay stays the only path from outbox to Kafka** (RFC §6.2 hard rule). Consumers are separate consumer groups in the worker; each opens a transaction, checks and writes `processed_events` with its effect, commits, then commits the offset | Preserves exactly-once-effect with at-least-once delivery (RFC §6.6); handler signature barely changes | ADR-0021, ADR-0027 |
| D4 | Protobuf with Schema Registry (BACKWARD), generated code committed (RFC §6.8). The `course_published` payload gains `instructor_id` here, which also closes the instructor-ownership gap in analytics | One schema change instead of two | F-18 caveat |
| D5 | Client libraries proposed, to be settled in an ADR before use: a pure-Go Kafka client (franz-go is the leading candidate; avoids cgo and its Docker/CI cost), Asynq (ADR-0013), the official MongoDB Go driver, OpenTelemetry Go SDK, Prometheus client | Pure-Go keeps `CGO_ENABLED=0` non-root images and the per-module Docker guard intact | New ADR |
| D6 | Notification delivery targets a **self-hosted SMTP sink in compose (Mailpit)** for local and e2e; the provider is an interface so a real one is a config change | No third-party account needed for evidence; Emumba guidance prefers self-hosted where scope allows | ADR-0014 |
| D7 | New shared code goes in **new library modules** (`libs/events`, `libs/kafka`, `libs/asynqx`, `libs/telemetry`, `libs/mongostore`), registered in `go.work`, the module-graph guard and `CODEOWNERS`, with the link-graph budget extended so identity-api still links no Kafka/Asynq/Mongo code | Keeps the structure the supervisor asked for; the guards stay meaningful | ADR-0026 |
| D8 | search-indexer and cache-invalidator **stay inside the publish workflow** | Already decided and still valid | DR-018 |
| D9 | `sync.RWMutex` is not forced in; fan-out/fan-in is built only where real (the audit-archiver and reconciliation batch work) | Same anti-pattern argument as ADR-0021 and DR-016 | DR-016 |

## 4a. Order of execution while the module-split PRs await review

PRs #12 to #18 have not been reviewed by the supervisor. If the review changes the module layout, work that edits `wire.go`, the relay or the service packages gets rebased. So the work is sequenced by how exposed it is:

| Tier | Work | Why |
|---|---|---|
| **Start now (new files or docs only)** | Phase 0 (ADRs 0029 to 0032, drafted); Phase 2's new `libs/events` module, `.proto` files, registry bootstrap script, the outbox trace/actor migration (a new file); scaffolding for `libs/mongostore`, `libs/asynqx`, `libs/telemetry` with their own tests; Mailpit, Kafka topic provisioning and Prometheus/Grafana provisioning files in compose; the skeleton of `scripts/verify-deliverables.sh`; the E2E scenario specs (test names and assertions, not yet runnable) | Nothing here edits an existing module's code, so a layout change costs a path fix, not a rewrite |
| **Start now, low exposure (edits inside one existing package)** | Phase 1 items: publish lease reconciliation, `VerifyAssets` and asset upload, `Idempotency-Key`, argon2 bound, `token_stale` re-seed, seed `.env` fix, `govulncheck`, cleanup | Each is a local change with its own test; rebasing across a rename is mechanical |
| **Wait for the supervisor's review** | Phase 3 (Kafka in the relay, consumer runner in the worker, rehoming handlers), Phase 4 and 5 wiring into services, Phase 7 instrumentation of existing handlers | These change `wire.go`, the worker and handler packages, exactly where the review could move things (the DR-020 questions: one module per library, and the worker importing the course and analytics handlers) |

If the supervisor answers early, the third tier starts immediately; the first two tiers are already done by then.

## 4. Work breakdown

Sizes: S under a day, M 1 to 3 days, L over 3 days, for one engineer plus agents. Each phase ends on its own branch, stacked in order, with CI green before the next starts. Phases 1 and 2 are independent and can run in parallel.

### Phase 0: Setup (S)
- Branch base `week3/plan` from PR #18; later phases stack on it. Rebase onto `main` once #1, #11 and #12 to #18 merge.
- Draft ADR-0029 (event transport and dual mode), ADR-0030 (Protobuf and registry, client libraries), ADR-0031 (MongoDB archive and dead letters), ADR-0032 (observability; supersedes DR-019). Status "Proposed" until the matching phase passes.
- Decide the cut line (section 8) with the project owner.
- **Exit:** ADRs drafted, scope confirmed.

### Phase 1: Rough edges, independent of the event work (M)
Every item has a test that fails before the fix. Source: the assessment's "imperfections" list and the first-pass findings.

| Item | Fix | Test |
|---|---|---|
| Publish lease (a crash between claim and workflow start blocks republish for up to 15 min) | Reconcile on a schedule: a claimed version whose workflow ID is not running in Temporal is released (uses Temporal describe) | Kill the worker mid-publish against real Temporal; republish succeeds without the 15 minute wait |
| `VerifyAssets` only checks size | Build `POST /assets/upload-url` (MinIO pre-signed URL, RFC §7.4/§11.7) and make `VerifyAssets` check the object exists and its size/type | Upload, publish ok; reference a missing asset, publish fails cleanly with no live-version change |
| `Idempotency-Key` handling (RFC §15.1) | Middleware on the write endpoints that the RFC names, backed by the existing idempotency table | Same key twice returns the stored response; concurrent same key executes once |
| Unbounded argon2 concurrency | Semaphore sized from config; excess requests wait or get 429 | 200 concurrent logins stay under a heap ceiling |
| `token_stale` window after a Redis flush | Re-seed lazily on the first stale rejection when the DB version still matches the token | Existing e2e test extended; the window disappears |
| Relay pool needs about 2x workers in connections | Document and enforce in config validation | Config test |
| `.env` `TEMPORAL_HOST_PORT` breaks host-run `make seed` | `make seed` overrides to `localhost:7233`; documented | Seed from the host works on a clean checkout |
| Leftover `worktree-agent-*` local branches, `.idea/` | Delete branches, add `.idea/` to `.gitignore` (not `ONBOARDING.md`, which stays uncommitted by the owner's choice) | n/a |
| CI never run on a real runner | Already running; keep green; add the new guards as they appear | CI |
| Dependency CVEs never scanned | `govulncheck` in every module, in CI | CI step |

**Exit:** all above merged, assessment imperfections list updated.

### Phase 2: Event contracts (M)
- `libs/events`: `.proto` files for every event in RFC §6.5 (identity, course, enrollment, progress), the envelope, generated Go code committed, a codec that writes the Confluent wire format.
- Outbox migration: add `trace_id`, `span_id`, `actor_id` (RFC §6.4) if missing, as a new platform migration with a verified down; populate from the request context at write time.
- Schema Registry bootstrap script (idempotent) registering every subject with BACKWARD compatibility.
- **Tests:** round-trip of every event; golden files so a field rename fails CI; the registry rejects an incompatible schema (live, against the compose registry); old-version payloads still decode.
- **Exit:** every event the services emit has a schema, and `instructor_id` is on `course_published`.

### Phase 3: Kafka transport (L)
- `libs/kafka`: producer and consumer-group wrappers, context-aware shutdown, bounded in-flight work, retries with backoff.
- Relay "publish" step produces to the per-aggregate topics (RFC §6.5), keyed by aggregate id; ordering per aggregate preserved. In-process mode unchanged behind `EVENT_TRANSPORT`.
- Consumer runner: open transaction → `processed_events` check → handler → commit → commit offset. Poison messages after the retry budget go to the dead-letter path (Phase 4) and the offset advances; the partition never blocks (RFC §6.9).
- Topic provisioning and partition counts as code.
- **Tests (real Kafka via testcontainers):** per-aggregate ordering; duplicate delivery produces one effect; consumer killed mid-handle resumes without loss; broker down then up, relay backlog drains with no loss and no duplicates beyond what idempotency absorbs; poison message does not stall a healthy one behind it.
- **Exit:** analytics and enrollment-course-archival run as Kafka consumers with the existing integration suites passing against both transports.

### Phase 4: MongoDB archive and dead letters (M)
- `libs/mongostore`: `events` archive, `audit_log`, `failed_events`.
- `audit-archiver` consumer (all topics) writes the event, with the schema id so it stays interpretable (RFC §6.8). This is a real fan-out of independent work, so it is where a genuine fan-out/fan-in pipeline can live (D9).
- Dead-letter path: a consumer or relay exhausting retries writes `failed_events` (payload, error history, consumer, attempts) in a way that cannot itself block the stream.
- Admin endpoints (course/analytics API or a small admin surface): list failed events, replay one by `event_id`, with the replay going through the normal idempotent path.
- Fix the relay's "mark published anyway" for poison events: park, never drop.
- **Tests:** poison event lands in `failed_events` with its history; replay reapplies once and is a no-op the second time; archive is complete after a restart; Mongo down degrades the archiver only, never analytics.

### Phase 5: Asynq workers, notifications, scheduled jobs, certificates (L)
- `libs/asynqx`: server with weighted queues critical:default:low at 6:3:1 and bounded pools (RFC §6.7/§10.2).
- `notification-dispatcher`: consumes enrollment, progress and course events, enqueues a delivery job and commits its offset immediately (ADR-0014). Delivery through a provider interface; Mailpit in compose. Delivery record prevents most duplicate sends; the rare duplicate is documented, not denied.
- Scheduled jobs: analytics reconciliation every 60 s, counter reconciliation, expired idempotency-key cleanup, outbox pruning, stale-workflow detection.
- Certificates (RFC §8.4): issued by a consumer of `CourseCompleted`, idempotent, one per enrollment.
- **Tests:** provider down → retries with backoff, queue depth rises, recovers, exactly the expected sends; duplicate event does not double-send; reconciliation repairs counters corrupted by hand; certificate issued once under replay.

### Phase 6: Analytics completion (M)
- **Most Popular Courses:** a Redis sorted set reconciled from Postgres every 60 s (RFC §9.1), served by a new endpoint, access rule per the existing analytics rules.
- **Failed Events / Workflow Issues:** served from `failed_events` plus failed workflow counts.
- Instructor ownership enforced for per-course analytics (uses the new `instructor_id`), with a backfill migration for existing mirror rows.
- Daily enrollments, completion rate and the averages now fed by the Kafka path; reconciliation compares to a recompute from the mirrors.
- **Exit:** 9 of 9 metrics, each with a test that changes source data and watches the metric move, and a drift test that corrupts a counter and watches reconciliation heal it.

### Phase 7: Observability (L)
- `libs/telemetry`: OpenTelemetry setup, exporter to Jaeger (OTLP), W3C context stored on the outbox row and carried in the envelope, continued by the relay, Kafka consumers and Asynq jobs (RFC §12.2). Errors always sampled.
- Instrumented: HTTP, database, Redis, Kafka produce/consume, Asynq, Temporal.
- Prometheus: RED on every route; `outbox_backlog`, `outbox_oldest_unpublished_seconds`, `kafka_consumer_lag`, `asynq_queue_depth`, `asynq_retry_count`, `dead_letter_total`, `workflow_started/completed/failed`, `db_connections_in_use`; `/metrics` on every service. Metric names and labels reviewed for cardinality (route templates, never raw ids).
- Grafana dashboards provisioned as code; Prometheus alert rules for the rate of dead-lettering and for outbox age.
- Logging already carries `request_id`; add `trace_id`, `event_id`, `job_id` per RFC §12.4.
- Health: `/health/ready` keeps Postgres and Redis; add Kafka and Mongo as non-fatal checks for the services that need them.
- **Tests:** an e2e test that enrolls a student and asserts one Jaeger trace contains HTTP, outbox, relay, consumer and job spans; a metrics test that stops Kafka and watches `outbox_backlog` rise and then fall; a cardinality test that fails on unbounded labels.

### Phase 8: End-to-end and adversarial verification (L)
Section 5. A separate agent session, with the same "break the system" brief, runs a second adversarial pass against the finished events and workers layer. The first pass found two defects it missed, so this pass is scoped to what Week 3 added (consumers, DLQ and replay, Asynq jobs, reconciliation, admin endpoints, trace propagation) plus a re-run of the earlier reproducers.

### Phase 9: Documentation, the deliverables audit, and defense readiness (M)
Section 7. README, ADRs 0029 to 0032 moved to Accepted or revised, RFC §6/§9/§12/§21 updated, `docs/rfc/differences-from-requirements.md` (DR-019 superseded, new entries for anything that differs), `api/openapi.yaml` for the new endpoints, `env.example`, per-module READMEs and CHANGELOGs, `GETTING_STARTED.md`, `docs/guides/testing.md`, `ONBOARDING.md` for the owner (not committed), and the hosted onboarding docs. A final assessment replaces `module-split-assessment.md`.

**Defense readiness** (the project ends in an external review and a defense, and the Execution Guidelines evaluate "depth of understanding" and "comfortable explaining your design"):
- A demo script that runs the end-to-end story from a clean `docker compose up`, with the expected output of each step, rehearsed against the built images.
- A defense brief for the owner: the architecture in five minutes, each ADR's one-line reason and its rejected alternative, the honest weak points (the assessment's imperfections list) with what was done about each, and the questions a reviewer is most likely to ask, with answers grounded in the code and the findings register.
- A short "what we would do next" list, so open items are shown as chosen, not forgotten.
- `ONBOARDING.md` and the hosted study docs refreshed for the owner's study; not committed.

## 5. End-to-end checks

They live in `test/e2e` (module 20 today), tagged `integration`, run against real Postgres, Redis, Kafka, Schema Registry, MongoDB and Temporal through testcontainers, plus one compose-based smoke script for the full stack from the built images. They are black-box: HTTP in, assertions on responses, Kafka topics, Mongo, Redis, Mailpit, Jaeger and `/metrics` out.

| # | Scenario | Asserts |
|---|---|---|
| E1 | Happy path: register, promote, create, publish, enroll, complete lessons | Publish reaches `ready`; enrollment, progress and certificate rows exist; the event reaches analytics, the archive and the notification sink; all nine metrics reflect it |
| E2 | One trace across the pipeline (D1) | A single trace id spans the HTTP request, outbox write, relay publish, consumer and Asynq job |
| E3 | Duplicate delivery | The same event delivered twice changes counters once and sends one email |
| E4 | Poison message | Lands in `failed_events`, healthy events behind it still process, replay by id works once |
| E5 | Kafka down, then up | The API stays available, `outbox_backlog` rises and drains, nothing lost |
| E6 | Consumer or worker killed mid-flow (`kill -9`) | Restart resumes; no lost or double effect |
| E7 | Provider (SMTP) down | Jobs retry with backoff, queue depth visible, delivery on recovery |
| E8 | Counter drift | Corrupt `platform_snapshot` by hand; reconciliation repairs it within its interval |
| E9 | Enrollment under load | 150 contenders for 25 seats stay exact under the Kafka path; backpressure visible; no duplicate or lost enrollment |
| E10 | Redis flush and restart | The existing e2e still passes with the new services in the stack |
| E11 | Schema incompatibility | A breaking proto change is rejected by the registry and fails the CI check |
| E12 | Publish under fault | Temporal activity fault and a worker killed mid-publish leave the live version intact; republish works |
| E13 | Archive and audit | Every event type appears in MongoDB with its schema id; a replay does not duplicate the archive |
| E14 | Auth and access on the new surface | Failed-events, replay and analytics endpoints enforce roles; non-admins cannot replay |

Every new E2E check is mutation-checked: break the thing it guards and confirm it fails, as was done for F-38.

CI cost needs care: the integration job already takes about 23 minutes. The Kafka-backed e2e tier runs in its own job (parallel), on pull requests touching events code and nightly, not on every module.

## 6. Rough-edge sweep (cross-cutting)

Beyond Phase 1, each phase leaves its code in the same state the module split did: lint clean in every module, `gofmt`, `go vet -tags=integration`, the link, module, Dockerfile and hygiene guards, mutation test of the guards, and a standalone `GOWORK=off` build. Additional sweeps at the end:

- Grep for `TODO`/`FIXME`, commented-out code, unused config variables, dead routes in `openapi.yaml`.
- Error contract: every new endpoint returns the standard error shape; no 5xx on bad input (fuzz targets for the new decoders).
- Config: every new variable is in `env.example`, validated at startup, and has a safe default or fails fast.
- Docker: new images non-root, pinned, with the Dockerfile closure check passing.
- Dependency review and `govulncheck` clean or each finding recorded.
- Secrets: nothing live in the repo (the org rule); new credentials are placeholders rejected outside development, like the existing ones.

## 7. Deliverables audit (the acceptance matrix)

A table in the final assessment, one row per item, each with a command or test id that proves it. A script, `scripts/verify-deliverables.sh`, runs the live checks against a running stack and prints pass/fail per row so the audit can be re-run, not just read.

| Source | Items |
|---|---|
| Execution Guidelines, Week 3 | The three deliverables in section 1, each with its E2E ids |
| Execution Guidelines, Weeks 1 and 2 | Re-verified, not assumed: service structure, schema, basic APIs, local setup; enrollment with duplicate handling and validations; Temporal publishing; content structure; state transitions |
| Execution Guidelines, general | README (overview, diagram, setup, API overview, stack), technical documentation (breakdown, data and event flow, decisions, assumptions, trade-offs), meaningful commits, codebase structure |
| Baseline functional 1 to 5 | Each row of the 32-row assessment re-rated, including 1.2 and 2.2 (assets), 3.2 and 4.2 (notifications), 4.3 (traceable), 4.4 (Kafka), 5 (nine metrics) |
| Baseline 6 and 7 | Monitoring, diagnosis of failures, concurrency primitives (including the RWMutex decision), no leaks or races under the new workers |
| Baseline tech stack | Kafka, Schema Registry, MongoDB, Asynq, Prometheus, Grafana, Jaeger, OpenTelemetry present and used, not merely in compose |
| Supervisor reviews | Module split asks, the publish and withdraw bugs, the Week 2 minor fixes (taken from the owner's statement; re-confirm in review), and anything raised on PRs #12 to #18 |
| Own commitments | Every ADR "Reversal trigger" or "Negative consequences" item that Week 3 was supposed to close (ADR-0021's no-dead-letter and no-reconciliation, ADR-0017 reconciliation, ADR-0025's search/cache reconciliation) marked closed or still open |

The audit ends with an honest rating in the same format as before (Met / Partial / Not built, with counts), listing what is not verified. Anything not run in CI on a real runner is labelled as such.

## 8. Scope, cut line and risks

Everything above is in scope. The order below is the order to finish things in, so that if the clock runs short the most important work is already done and tested:

1. **Must:** Phases 2 to 4 (contracts, Kafka transport, archive and dead letters), Phase 6 (nine metrics), Phase 7 core (trace through the envelope, `/metrics`, one dashboard), Phase 8 (E1 to E6, E9), Phase 9.
2. **Should:** Phase 5 notifications and reconciliation jobs, E7, E8, E11 to E14, Phase 1's publish lease and `VerifyAssets`.
3. **Could:** certificates, `Idempotency-Key` middleware, argon2 bound, alert rules, `govulncheck` gating.

Anything that does not make it is written down in the assessment as Not built with the reason, rather than quietly dropped. Certificates, asset upload and observability are confirmed in scope and are not candidates for silent removal.

**Risks**
- **Size.** This is much larger than the module split. The phases are ordered so each ends in a working, tested state; the cut line above is the pressure valve.
- **Dual-transport drift.** Two transports can diverge. Mitigation: the same handler suites run against both, and E2E runs on Kafka.
- **CI time and flakiness** from Kafka containers. Mitigation: separate job, health-wait helpers in `libs/testenv`, no sleeps.
- **A repeat of F-38** (two correct fixes interacting). Mitigation: E2E across real service boundaries first, unit tests second, and a live stack run at the end of every phase.
- **Schema-per-service friction.** Consumers must write only to their own schema; the module-graph guard stays on.
- **Unreviewed foundations.** The module-split stack (#12 to #18) is not yet reviewed by the supervisor. If the layout changes in review, Week 3 branches rebase. Mitigation: keep Week 3 changes in new modules where possible.
- **Facts not verified here:** library versions and APIs for Kafka, Asynq, OpenTelemetry and the MongoDB driver are to be confirmed against their current documentation when each ADR is written, not assumed from this plan.

## 9. Questions for the project owner (answered 2026-11-18)

1. Observability in this week: **yes** (D1, ADR-0032). Week 3 is the last stage before external review and defense.
2. Certificates and asset upload: **in scope**.
3. Mailpit as the notification sink: **yes**; no real mail provider or SMTP integration. Document it as a deliberate limit.
4. Supervisor review of PRs #12 to #18 before the review-exposed work starts: **yes**; start with what the review cannot affect (section 4a).
5. Second adversarial pass: **open**; the owner asked for a refresher on how the first pass was set up (see the conversation); decision pending.

## 10. Progress log

### 2026-11-18: the "start now" tier (section 4a) is built and merged on `week3/start-now`

Nine parallel agents, each in an isolated worktree with separate file ownership, then merged by the lead. Each agent's numbers below are its own report, re-checked at merge where noted.

| Piece | Result |
|---|---|
| `libs/events` | Protobuf contracts for the 8 event types that exist, envelope, Confluent wire codec, registry client, `register-schemas` tool, golden compatibility guard, fuzzed decoder. Live registry run passed |
| Outbox trace/actor | Migration `platform/000003`, `libs/outbox` and `libs/relay` carry `trace_id`, `span_id`, `actor_id`; nothing populates them yet |
| `libs/mongostore` | Archive, audit log, failed-event parking and replay state, with concurrency tests; MongoDB test helper in `libs/testenv` |
| `libs/asynqx` | Deduplicated enqueue, 6:3:1 weighted queues, bounded server, exhausted hook, inspector, replica-safe scheduler |
| `libs/telemetry` | OpenTelemetry, trace propagation through the envelope, RED and async metrics with a cardinality guard; **not wired into any service** |
| Provisioning | Compose: Mailpit, Kafka topic provisioner; `observability/` with Prometheus rules, Grafana dashboard; verified against the real containers |
| Verification scaffolding | 122-row acceptance matrix, `scripts/verify-deliverables.sh` (12 live checks passed against the stack), E1 to E14 specs as skipped tests |
| Course | Publish-lease reconciliation, `POST /assets/upload-url` with a real `VerifyAssets`, `instructor_id` on `course_published` |
| Hygiene | Argon2 concurrency bound, Redis-flush token reseeder, `make seed` host fix, relay pool check, `govulncheck` in CI plus dependency bumps |

**Merge results:** the lead resolved four conflicts (three CHANGELOGs and one `go.mod`), re-tidied all 24 modules, and aligned every skewed dependency to its highest version so the hygiene guard passes. Dockerfiles and the release workflow moved to Go 1.26.11 (the stdlib advisories `govulncheck` found are reachable on 1.26.5).

### Deviations from the plan, and open items created by this tier

- **The `token_stale` fix is a mitigation, not an elimination.** Identity re-seeds Redis from a background loop every 15 seconds, so the window shrinks from "until the next login" to about 15 seconds. A single evicted key while the loop's sentinel survives still waits for the next login or the sentinel's expiry. Chosen because the alternative (course-api asking identity at runtime) adds a cross-service dependency on every Redis miss.
- **The relay pool check is a hard startup error for every binary** (`POSTGRES_MAX_OPEN_CONNS` must be at least 2 x `RELAY_WORKERS`), because a starved pool burns attempts and eventually gives events up. Defaults pass.
- **`AddLesson` now rejects an `asset_key` not issued by `upload-url`,** and a lesson that references an asset fails closed when no store is attached. Both are API behaviour changes.
- **Search and cache reconciliation (ADR-0025's open gap) is assigned to Phase 5**, alongside analytics reconciliation, as scheduled Asynq jobs.
- **`sync.RWMutex`:** kept as decided in DR-016 and D9; the matrix row says so.
- **`govulncheck` can fail an unrelated PR** when a new advisory is published. Accepted for now; move it to a scheduled job if it becomes noisy.

### Wiring backlog (the review no longer gates it: see the 2026-11-19 entry; status of each item is noted there)

1. `services/worker/cmd/worker/wire.go`: attach the asset store (`courseSvc.WithAssetStore(assets.New(cfg.MinIO))`) or `VerifyAssets` fails every publish whose lessons reference an asset; pass `RELAY_WORKERS` to the relay; schedule `ReconcileStalePublishes` (every 30 to 60 seconds).
2. Compose and `env.example`: set `MINIO_PUBLIC_ENDPOINT` (a browser cannot reach `minio:9000`); the bucket needs CORS allowing `PUT`.
3. Asynq pools per queue (RFC §10.2 sizes) need one `Server` per queue; not built.
4. Wire `libs/telemetry` into every service, expose `/metrics`, decide the worker's metrics port (the 9100 in `prometheus.yml` is an assumption), and extend the link-graph budget for the gRPC and protobuf packages it brings.
5. Phase 3: a real Confluent client must read `libs/events` bytes (interop is checked only against documentation and registry acceptance so far); the relay routes by `event_type` prefix; the Kafka client library is chosen in ADR-0030.
6. CI: a parallel job for the Kafka e2e tier; a step that fails when a pending E2E spec is still skipped past its phase; `make verify-deliverables` against a stack.
7. `scripts/verify-deliverables.sh` writes test users and courses into whatever database the stack uses. Run it against a disposable compose project name, not the developer database.
8. The link-graph guard now forbids `libs/mongostore` and `libs/asynqx` in identity-api and analytics-api. When Kafka client code lands, add it to the same rule.

### Verification of the merged tier (2026-11-18, quiet machine, local only)

| Check | Result |
|---|---|
| `make test-integration`, every module, `-race -tags=integration` | exit 0, 25 packages ok, no FAIL or panic lines |
| `golangci-lint` v2.14.0 in all 24 modules | 0 issues |
| `go vet -tags=integration`, `gofmt` | clean |
| Standalone `GOWORK=off` build of all 24 modules | pass |
| Docker build of identity-api, course-api, analytics-api, worker on Go 1.26.11 | built |
| Guards: module graph, link graph, Dockerfile closure, module hygiene, dev defaults, HTTP hardening | pass |
| `scripts/test-guards.sh` | 18 passed, 0 failed |

Not verified: CI on a GitHub runner for this branch; the built images running as a stack with the new code; a trace through real Jaeger; `ReconcileStalePublishes` against a real Temporal server.

### Known test flakiness to resolve before the defense

Under heavy machine load (nine agents sharing Docker), single runs of the relay suite, `services/analytics` (`TestGetPlatformSnapshot_NoAuthHeader_Rejected`) and two `services/course` packages failed once and passed on rerun. The cause was not diagnosed. The full suite was re-run on a quiet machine and passed in one run (above); that is one run, not proof the tests are stable, so repeat runs are still wanted before the defense, and any repeat failure is a defect to fix, not noise.

### 2026-11-18: `Idempotency-Key` and the rough-edge checks (second wave; committed and pushed on `week3/start-now`)

**`Idempotency-Key` (Phase 1, acceptance rows AD-14 and P1-3).** New module `libs/idempotency`; wired into the course-api `POST` routes (create course, add module, add lesson, publish, archive, `POST /assets/upload-url`, `POST /enrollments`) through an optional argument to `RegisterRoutes`, so callers and tests that pass nothing behave as before. The scope and replay rules are DR-023 (Proposed); the OpenAPI documents the header on the seven operations.

| Evidence | Result |
|---|---|
| `libs/idempotency` integration tests (11), `-race` | Pass: replay, mismatch 422, per-user scoping, anonymous passthrough, 10 concurrent requests run the handler once, release on 4xx/5xx/panic, invalid keys, takeover of an abandoned key, expiry and pruning |
| `services/course/enrollment` HTTP tests (2), `-race` | Pass: same key twice returns the stored 201 and one row; 10 concurrent requests leave one row |
| `check_P1_3` in `verify-deliverables.sh`, live | Pass against the built images in a disposable compose project |
| Module, link, Dockerfile, hygiene and HTTP-hardening guards | Pass (25 modules) |

Not done: the scheduled job that calls `PruneExpired` (Phase 5, worker wiring); identity routes are not covered (DR-023); 4xx responses are not stored (DR-023).

**Rough-edge checks.**

| Check | Result |
|---|---|
| Built images as a full stack | `core` and `workflow` profiles brought up from the working tree in a disposable project (`sc-verify`, own volumes, removed afterwards); migrations and seed applied; `verify-deliverables.sh`: 13 passed, 0 failed, 29 pending, 0 skipped. The dev `smartcourse` project and its volumes were not touched |
| Flaky tests, repeated | `TestGetPlatformSnapshot_NoAuthHeader_Rejected` 15 runs and the full `libs/relay` integration suite 3 runs, all with `-race`: no failure. This lowers the concern; it does not prove stability |
| `services/course` full integration run | One run failed two tests and a rerun failed a different one, each with `start postgres container ... context deadline exceeded` (Docker did not answer in 60 s while many containers started). Each test passes alone (about 5 s) and both packages pass on a quiet rerun. This is container start-up under load, not an assertion failure; the test setup has no retry or longer timeout, which is a known weakness of the harness |
| Telemetry licences | Re-checked against the LICENSE files of the resolved module versions: OpenTelemetry modules, `prometheus/client_golang` and `grpc` are Apache-2.0; `protobuf` and `grpc-gateway` are BSD. The ADR's earlier statement held |
| Prometheus client version | ADR-0032 notes and the telemetry README said v1.25.0; the module uses v1.24.1. v1.25.0 needs protobuf v1.36.12, which would break the single-version rule, so the documents were corrected, not the module |
| Hygiene drift found | `go mod tidy` drift in `libs/asynqx`, `libs/cache`, `libs/relay`, `libs/telemetry`, `libs/testenv` (`testenv` gained the Mongo driver) was present on the merged branch and went unreported by my earlier "guards pass" statement; fixed by tidying |

### 2026-11-19: the review no longer gates the wiring tier; wave 1 of the wiring is merged

**Context.** The supervisor asked for all Week 2 work in one PR (done: #11 now carries the publish workflow, the module split and the 38 fixes; #12 to #18 are folded in). The project owner then told us to assume no critical feedback on Week 2 and start building Week 3, so the "wait for the review" tier of section 4a is released. The module layout is fixed as it stands.

**Wave 1 (five agents, isolated worktrees, disjoint file ownership), merged into `week3/start-now`:**

| Piece | What exists now | Not done or not verified |
|---|---|---|
| `libs/kafka` (Phase 3 library) | franz-go v1.22.3 (BSD-3-Clause), synchronous idempotent producer, consumer-group runner (bounded in-flight, offset commit after the handler, exhaustion hook, graceful drain, rebalance), topic provisioning as code (a test fails if it drifts from compose's `kafka-init`), readiness check, `testenv.SetupKafka`. Tested against a real `apache/kafka:3.7.0`, including a broker stop and restart with producers in flight (no loss, no duplicates, in order per key). The restart test found that franz-go will not retry a first-read EOF; both clients now set `AlwaysRetryEOF` | Multi-broker, TLS/SASL, throughput and lag, and a Confluent-format consumer reading `libs/events` bytes through it |
| Worker jobs and Asynq (Phase 1 and 5) | One Asynq server per queue (critical 10, default 5, low 3), a handler registry that does not import asynq, scheduled jobs on the low queue (stale-publish reconciliation, analytics reconciliation, enrollment-counter reconciliation, idempotency-key pruning, outbox pruning), the asset store attached to the worker, `RELAY_WORKERS` passed to the relay | `ReconcileStalePublishes` against a real Temporal; Redis going down after start; archived tasks from a failing minute-interval job accumulate until deleted. The enrollment counter counts non-withdrawn enrollments, not only active ones, because a completed enrollment still holds its seat; `outbox.Prune` keeps rows with `last_error` because there is no dead-letter store yet |
| Analytics (Phase 6) | 8 of 9 metrics: Most Popular Courses with a Redis sorted set and a Postgres fallback; per-course ownership gate; `NewReconciler(...).Run` with a drift test and a deterministic lock-discipline test; `/analytics/issues` and the source interfaces | The ninth metric (Failed Events / Workflow Issues) has no real source yet; DR-024 records the ownership fallback for courses published before the migration |
| Notifications (Phase 5) | Provider interface and a standard-library SMTP provider (STARTTLS verified, credentials refused over cleartext), a recipient mirror and delivery table in a new `notification` schema, relay handlers for welcome, enrollment, completion and publish mails, a delivery task, and a sweeper that is the single enqueue path. Tested against a real `axllent/mailpit:v1.34.0` | The header-injection test does not prove the control-character stripping on its own (the standard encoding also neutralises it); no notification for publish failure or certificate issued (the events do not exist) |
| Certificates (Phase 5) | Issue-once consumer (`UNIQUE(enrollment_id)`), owner-only list and get, a public verify route with a rate limit, migration 000007 | Users registered before `instructor_mirror` existed may have no mirror row, so their certificate would retry until the relay gives up; no `progress.certificate_issued` event |

**Lead wiring done in `ccef152`:** the worker registers the certificate and notification handlers in the relay, registers the delivery task on the critical queue (a permanent provider failure is archived, not retried), starts the sweeper, and runs the analytics reconciler as a job. Mailpit moves to the `core` compose profile and the worker points `SMTP_HOST` at it. `libs/ratelimit` joins `Dockerfile.course-api`, `Dockerfile.worker` and the worker `go.mod`. A new integration test drives real outbox events through the relay on the worker's own search path and checks one certificate and the delivered mail end to end.

**Wiring backlog status.** Item 1 (asset store, `RELAY_WORKERS`, stale-publish schedule): done. Item 2 (`MINIO_PUBLIC_ENDPOINT`, bucket CORS): open. Item 3 (per-queue Asynq pools): done. Item 4 (telemetry in every service): open. Item 5 (real Confluent client reading `libs/events` bytes; relay routing): open, in progress as the Kafka transport. Item 6 (CI jobs for the Kafka tier and a pending-spec check): open. Item 7 (run `verify-deliverables.sh` on a disposable project): done on 2026-11-18. Item 8 (link-graph rule for Kafka client code): done in `libs/kafka`.

**Verification of the merged tree.** All guards pass (module graph, link graph, Dockerfile modules, hygiene, dev defaults, HTTP hardening), `go vet` is clean in every module, and `golangci-lint` v2.14.0 reports 0 issues in all 26 modules. The new end-to-end test and the worker module's tests pass. A full `make test-integration` across every module has not been run on the merged tree yet; it is scheduled after wave 2.

**Decisions made while briefing wave 2.** (1) `analytics-api` may link `libs/mongostore` (the Failed Events metric is an analytics feature served from `failed_events`); `identity-api` stays forbidden. (2) Failed-event listing and replay live in a small admin HTTP surface inside the worker, because replay must re-run the worker's own handlers. (3) The audit-archiver is registered only when `EVENT_TRANSPORT=kafka`, because in in-process mode a failing handler fails the whole relay row and a Mongo outage would stall analytics. (4) The dead-letter contract (`relay.DeadLetter`, `relay.DeadLetterSink`, `relay.WithDeadLetterSink`) is defined by the Kafka-transport work and implemented by the Mongo work.

**Wave 2 (in progress):** the Kafka transport (relay publish step, consumer runner, `EVENT_TRANSPORT`) and the MongoDB archive, dead letters, replay and admin surface. After them: observability instrumentation (Phase 7), the E2E tier and the second adversarial pass (Phase 8), and the documentation and deliverables audit (Phase 9).
