# test/

Cross-service test material that doesn't belong to any one service. Most tests live *inside* each service module (`services/<service>/<package>/*_test.go`, right next to the code they test) — see `docs/guides/testing.md` for the full picture of what's tested and why.

## Where the shared test infrastructure is

**`libs/testenv/`** (it used to be `test/integration/testenv/`) spins up real, disposable Postgres and Redis containers via `testcontainers-go`, applies every service's migrations from `migrations/`, and hands back connected clients. Every `*_integration_test.go` file calls `testenv.Setup(t)` rather than hand-rolling container setup, so container lifecycle and migration order are written once. It finds `migrations/` relative to its own source file, so it must stay two directories below the repo root.

## Running tests

`go test ./...` does not cross Go module boundaries, so run tests through the repo's targets, which run the command in every module with `GOWORK=off`:

```
make test               # unit tests, every module
make test-integration   # race detector + integration tag (needs Docker)
```

## Why integration tests live in their service, not here

A test for `services/course/enrollment`'s seat-limit logic belongs next to that code, not in a parallel directory tree kept in sync by hand. This directory holds only what genuinely spans services.

## Cross-service tests: `test/e2e`

`test/e2e` is its own Go module and requires **no service module** (`scripts/check-module-graph.sh`; only `libs/*`). It builds the service binaries with `go build`, runs them as processes and talks to them over HTTP, Kafka, Postgres, MongoDB, Redis, Mailpit and Temporal. It never imports service code.

### Tiers and how pending specs are tracked

1. **Unit and integration tests** live in each service and lib (`make test`, `make test-integration`, Docker for the latter).
2. **Container tier** (`test/e2e`, build tag `e2e`): the scenarios `TestE2E_E01_` to `TestE2E_E14_`, the F-38 test `TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks` and `TestE2E_DataIndependence_RolesAndOutboxes`, against the ten-process topology (the ten long-running binaries, plus `identity-admin`). Own CI job (`e2e` in `.github/workflows/ci.yml`), not part of `make test-integration`. See "The container harness".
3. **Compose tier**: `scripts/verify-deliverables.sh` against a stack started from the built images. `--list` shows the rows, `--only <id>` runs one.

**Ported.** Wave C3a ported E01 and E03 to E07, the F-38 test (`redis_flush_e2e_test.go`) and added the data-independence test; Wave C3b ported E02 (`scenarios_trace_e2e_test.go`), E08 and E09 (`scenarios_correctness_e2e_test.go`), E10 (`redis_flush_scenarios_e2e_test.go`), E11 (`scenarios_schema_e2e_test.go`), E12 (`scenarios_publish_e2e_test.go`), E13 (`scenarios_archive_e2e_test.go`) and E14 (`scenarios_access_e2e_test.go`), and the adversarial scenarios (`adversarial_*_e2e_test.go`: surface, certificate mirror stall, two replicas; the MongoDB-outage reproducer is behind the extra tag `bugrepro` because it fails today, B-06 and B-07). No spec is pending: the allowlist of `scripts/check-pending-e2e.sh` is empty. A SKIP is not proof: a matrix row closes only when the test runs and passes. `scripts/check-pending-e2e.sh` (`make check-pending-e2e`, a step of the main CI job) fails when a spec is pending and not allowlisted, when an allowlisted spec was implemented, or when a spec function is missing or duplicated.

## The container harness (`test/e2e/harness`)

`harness.Start(t, harness.Options{})` brings up a whole system for a test and returns a `*Stack`; its cleanup stops everything and, if the test failed, prints the output of every process.

```
make test-e2e                                  # every scenario, tag e2e
make test-e2e E2E_RUN=TestE2E_E05             # one scenario
cd test/e2e && GOWORK=off go test -tags=e2e -count=1 -v -run 'TestE2E_E01_' .
cd test/e2e && GOWORK=off go vet -tags=e2e ./... && golangci-lint run --build-tags e2e ./...
```

Docker is required. Use a private `TMPDIR` if two checkouts run the suite at once (the binaries are built into `$TMPDIR/smartcourse-e2e-bin-<hash of the checkout>`, so checkouts do not overwrite each other, but go build caches and containers are shared).

**What runs.** Per scenario (fresh, so no state leaks): Postgres with the real `scripts/db-roles.sh` and every migration (`libs/testenv`: roles `svc_identity`, `svc_course`, `svc_analytics`, `svc_notification`, owner connection `Env.DB` with search path `public`), Redis, a Kafka broker (`libs/testenv`), MongoDB (a harness container on a fixed host port), Mailpit, Jaeger when `Options{Tracing: true}`, then the ten long-running binaries as subprocesses (`identity-admin` is run once for the first admin). Shared by the whole test binary and started on first use, stopped by `TestMain` (`harness.Main`): a real Confluent Schema Registry with a Kafka of its own, the schemas registered by the built `register-schemas` tool, and Temporal (`auto-setup`). Each stack uses its own pair of Temporal task queues (workflow and activities).

**The binaries and their environment** (`topology.go`). `build.go` runs `go build` in each module directory once per test process (`identity-api`, `identity-worker`, `identity-admin`, `course-api`, `course-worker`, `analytics-api`, `analytics-worker`, `notification-worker`, `publishing-worker`, `audit-api`, `audit-worker`, `register-schemas`). Each process gets exactly the variables of its configuration table (`env.example` section 2 and its cmd README), from a `spec` of flags: its own Postgres role DSN and `POSTGRES_SEARCH_PATH=<schema>,public`, an internal listener `METRICS_HTTP_ADDR` on a port the harness picked, Kafka brokers, `SCHEMA_REGISTRY_URL` where the binary reads it (not the audit binaries), `MONGO_URI`, SMTP to Mailpit for notification-worker, Temporal host and the two queues, JWT for the APIs, `TRUSTED_PROXIES=127.0.0.1` on the APIs. A variable a binary does not read is never set. The first admin is created by running the built `identity-admin` (`IDENTITY_ADMIN_EMAIL`/`PASSWORD`), not by SQL. Readiness waits on each binary's internal `/health/ready` (and the number of `kafka consumer started` log lines for a binary with consumers).

**Controls and readers** (all on `*Stack`):

| Need | Use |
|---|---|
| Start, stop, crash a binary | `Start(t, name)` (waits ready), `Stop(name)` (SIGTERM), `Kill(name)` (kill -9); names `harness.IdentityAPI`, `CourseWorker`, ... (`Binaries`); `Proc(name)` for its log (`LogCount`, `LogContains`); `NewProc(t, name, env)` a second, not started process of a binary (its `Exited(timeout)`) |
| Dependencies down and up | `StopKafka(t)`/`StartKafka(t)`, `StopSMTP(t)`/`StartSMTP(t)`, `StopMongo(t)`/`StartMongo(t)` (same address, data kept), `FlushRedis(t)` |
| Stop a handler mid-transaction without a hook in the product | `HoldRowLock(t, "SELECT 1 ... FOR UPDATE", args...)`, `WaitBlockedOn`, `WaitNoBlockedBackends` |
| Users and the API | `NewStudent`, `NewInstructor`, `AdminToken`, `NewCourse`, `PublishedCourse`, `Enroll`, `Withdraw`, `CompleteAll`, `NewStudents`, `Certificates`, `PlatformStats`, `CourseStatsFor`, `PopularCourses`, `DailyEnrollments`, `Issues`; `harness.Do` and `harness.DoFull` for anything else; public URLs `IdentityURL`, `CourseURL`, `AnalyticsURL`, `AuditURL` |
| Failed events (audit-api) | `ListFailedEvents`, `ReplayFailedEvent` (audit-api republishes to `retry.<consumer>`; the consumer's own group applies it afterwards, so wait for the effect), `RetryTopic(consumer)` |
| Mail | `Mailpit.Messages`, `MessagesTo`, `MessagesToWithSubject` |
| MongoDB | `Mongo.Count`, `Find`, `ArchivedEvents`, `FailedEvents` |
| Traces | `Jaeger.Trace`, `FindErrored` (with `Options{Tracing: true}`) |
| Kafka | `ReadTopic(s)`, `ConsumerLag(group)`, `NewEventProducer` with `Redeliver` and `Produce` (a new event framed with the real registry ids), `EventTopics`, `ConsumerGroups`, `ConsumerOwner` (which binary runs a group) |
| Metrics | `Metrics(binary)`, `ScrapeMetrics(url)`; `Metrics.Sum`, `SumWhere`. Metrics are per binary: outbox gauges on `course-api`/`identity-worker`, `dead_letter_total` and `event_processed_total` on the consuming worker, `asynq_*` gauges only on the workers that export them (identity-worker, course-worker), labelled with the worker queue name, `critical` or with the per-service namespace `notification:critical` (match the fragment `critical"`); a scenario whose worker exports none asserts on the delivery rows instead |
| SQL (as the owner) | `DB()` (qualify tables: `course.enrollments`), `Count`, `MustCount`, `OutboxRows` (both outboxes), `OutboxUnpublished`, `ProcessedCount(consumer, eventID)` (the consumer's own service table; Mongo archive for `audit-archiver`), `EventIDFor` |
| Waiting | `Eventually`, `WaitFor`, `Never`, `WaitSettled` (both outboxes drained and every consumer group at lag 0) |

**Options.** `WorkerEnv` (every `*-worker`), `APIEnv` (every `*-api`), `BinaryEnv[name]` (one binary, wins), `Tracing`. A scenario tunes the process that owns the behaviour (for example `ASYNQ_BACKOFF_*` on `notification-worker`).

**Conventions.** A scenario waits on a condition with a deadline, never on a fixed sleep alone; baselines are read after `WaitSettled`, and "nothing changed" checks run after it too. Users, courses and event ids are fresh per scenario. Each request sends its own `X-Forwarded-For` and the APIs trust the loopback proxy, so per-IP rate limits do not throttle a suite. Email verification is not required to log in.

**How to extend the harness** (Wave C3b and later). A scenario of the old four-binary harness (see git history before Wave C3b) maps onto the new one by replacing: `KillWorker`/`StartWorker` with `Kill(name)`/`Start(t, name)` on the binary that owns the behaviour; `WorkerMetrics()` with `Metrics(name)`; `WorkerAdminURL` with `AuditURL` (failed events) and `ReplayFailedEvent` semantics (republish, asynchronous apply); `platform.outbox`, `platform.processed_events` with `identity.outbox`/`course.outbox` (`OutboxRows`) and `ProcessedCount`; `WorkerEnv` knobs with `BinaryEnv`; `EventTransport` is gone (Kafka only). Add a new control to `harness`, not to the test; add a variable a binary newly reads to its `spec` in `topology.go`. A spec that has to be skipped again needs a `t.Skip("pending: ...")` and a line in the allowlist of `scripts/check-pending-e2e.sh`.

**Run time.** Per scenario: containers ready in about 15 to 35 s, ten binaries ready 10 to 25 s later; see the acceptance matrix for each scenario's measured time. The slow ones wait on real clocks (E06 on a consumer group session). Docker Desktop needs about 8 GiB (the shared Schema Registry, its Kafka and Temporal, plus per scenario Postgres, Redis, Kafka, MongoDB, Mailpit).

Add a test here when the behaviour under test depends on two services agreeing, for example one service writing something to Redis or the outbox that another reads. If it only concerns one service, put it in that service.
