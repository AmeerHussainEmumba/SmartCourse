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
