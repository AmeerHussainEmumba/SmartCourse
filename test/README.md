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

`test/e2e` is its own Go module (it is allowed to depend on more than one service, see `scripts/check-module-graph.sh`) holding tests that run services side by side. It boots the real identity routes and the real course routes as two separate engines, each on the production router stack and each with its own database connection restricted to its own schemas (`identity,platform,public` and `course,platform,public`, as in compose), against one shared Postgres and one shared Redis. Only the Temporal client is faked. Run it with the rest: `make test-integration`, or `cd test/e2e && go test -tags=integration ./...`.

It has the in-process test `TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks` (F-38, tag `integration`), the container-tier scenarios E01 and E03 to E07 (tag `e2e`, below), and the specs still pending (E02, E08 to E14). The F-38 test uses a real admin role change, real logins and a real refresh to check that after a Redis restart an instructor whose token version is above 1 is refused cleanly, regains access by logging in or refreshing, and stays revoked after a demotion. It cannot live in either service's own tests because `course-api` cannot import identity.

### The Week 3 E2E tiers and how pending specs are tracked

Week 3's end-to-end checks (E1 to E14, `docs/plans/week-3.md` section 5) come in three tiers, from cheapest to most complete:

1. **In-process tier** (today's test): real identity and course routes, real Postgres and Redis through `libs/testenv`, nothing else. Runs in `make test-integration`.
2. **Container tier** (the `TestE2E_E01_` to `TestE2E_E14_` specs, build tag `e2e`): real Postgres, Redis, Kafka, Schema Registry, MongoDB, Mailpit and Temporal through testcontainers, with the real worker and API binaries run as subprocesses (the worker so a test can `kill -9` it). Black box: HTTP in, assertions on responses, topics, Mongo, Redis, Mailpit and `/metrics`. It has its own CI job (`e2e` in `.github/workflows/ci.yml`, parallel to the main job, 45 minute timeout), not part of `make test-integration`. See "The container harness" below.
3. **Compose tier**: `scripts/verify-deliverables.sh` against a stack started from the built images (`make dev-core`, `make dev-workflow`, later the events and observability profiles). It checks the rows of `docs/plans/week-3-acceptance-matrix.md` that can be checked over HTTP and prints `PASS`, `FAIL`, `PENDING` or `SKIP` per row. `--list` shows the rows, `--only <id>` runs one. It creates users and a published course in whichever database the stack uses, through the public API.

**A spec that is not implemented yet is a skipped test, not a missing one.** Each of E1 to E14 exists today in `test/e2e/scenarios_*_test.go` as a test that starts with `t.Skip("pending: Phase N - <what it needs>")`. Its doc comment is the exact procedure (steps, assertions, the helper it needs from a later phase), so the engineer who delivers that phase fills in the body and deletes the `t.Skip` line. Nothing is hidden: `go test -tags=integration -v ./...` reports each as SKIP with its phase, and `grep -rn 't.Skip("pending:' test/e2e` lists what is still open. A SKIP is not proof; a matrix row (`E01` to `E14`) closes only when the test runs and passes, and has been mutation-checked (break the guarded behaviour, see it fail).

Specs must not import modules that do not exist yet. When a spec needs something a later phase will add (a Kafka reader, a Mailpit client, a worker subprocess harness), the comment says so, and the helper is added with that phase.

`scripts/check-pending-e2e.sh` (`make check-pending-e2e`, a step of the main CI job and a case of `scripts/test-guards.sh`) keeps this honest: it fails when a spec is pending and not in its allowlist (E02 and E08 to E14 today, each with the phase that owns it), when an allowlisted spec has been implemented (remove its line), or when a spec function is missing. The implemented scenarios are in `scenarios_*_e2e_test.go` (tag `e2e`); the skipped specs stay in `scenarios_*_test.go` (tag `integration`).

## The container harness (`test/e2e/harness`)

The scenarios of the container tier are written against one package. `harness.Start(t, harness.Options{})` brings up a whole system for a test and returns a `*Stack`; its cleanup stops everything and, if the test failed, prints the output of every process.

**Run it** (Docker required; the first run builds the four service binaries and pulls images):

```
make test-e2e                                  # every scenario, tag e2e
make test-e2e E2E_RUN=TestE2E_E05             # one scenario
cd test/e2e && GOWORK=off go test -tags=e2e -count=1 -v -run 'TestE2E_E01_' .
cd test/e2e && GOWORK=off go vet -tags=e2e ./... && golangci-lint run --build-tags e2e ./...
```

**What runs.** Per scenario (fresh, so no state leaks between tests): Postgres with every migration, Redis, a Kafka broker (`libs/testenv`), MongoDB, Mailpit, and the four binaries as subprocesses with the environment docker-compose gives them (worker with `EVENT_TRANSPORT=kafka`; identity-api, course-api, analytics-api). Shared by the whole test binary and started on first use, stopped by `TestMain` (`harness.Main`): a real Confluent Schema Registry with a Kafka of its own (so a scenario can stop its broker without taking the registry down; the registry needs a broker reachable from another container, which `testenv`'s is not), the schemas registered by `libs/events/cmd/register-schemas`, and Temporal (the `auto-setup` image compose uses, with a small Postgres of its own). Each stack uses its own Temporal task queue. Typical cost on a quiet laptop: containers and binaries ready in 15 to 25 seconds per scenario after the first, 30 to 130 seconds per scenario in total.

**The binaries.** `build.go` runs `go build` inside each module's directory (so `GOWORK=off`, as CI and Docker build) into `$TMPDIR/smartcourse-e2e-bin`, once per test process. test/e2e may require only `libs/*`, `services/identity` and `services/course` (`scripts/check-module-graph.sh`), so the worker and analytics-api are processes, never imports. `Proc` (proc.go) captures output (the tail is dumped into the test log on failure), starts in its own process group, and supports `Kill` (SIGKILL) and `Stop` (SIGTERM, then kill).

**Controls and readers** (all on `*Stack`):

| Need | Use |
|---|---|
| Crash and restart the worker | `KillWorker()` (kill -9), `StartWorker(t)` (same ports and environment) |
| Kafka down and up | `StopKafka(t)`, `StartKafka(t)` (same address, data kept) |
| SMTP provider down and up | `StopSMTP(t)`, `StartSMTP(t)` (Mailpit; its inbox is empty after a restart) |
| Stop a handler mid-transaction without a hook in the product | `HoldRowLock(t, "SELECT 1 ... FOR UPDATE", args...)`, `WaitBlockedOn(t, fragment, timeout)`, `WaitNoBlockedBackends` (faults.go) |
| Users and the API | `NewStudent`, `NewInstructor` (register, admin promotion over the API, re-login), `AdminToken`, `NewCourse`, `PublishedCourse` (publish through Temporal and wait for `ready`), `Enroll`, `Withdraw`, `CompleteAll`, `Certificates`, `PlatformStats`, `CourseStatsFor`, `PopularCourses`, `DailyEnrollments`, `Issues`; `harness.Do` for anything else |
| Worker admin API | `ListFailedEvents`, `ReplayFailedEvent`, `WorkerHealth` |
| Mail | `Mailpit.Messages`, `MessagesTo`, `MessagesToWithSubject` |
| MongoDB | `Mongo.Count`, `Find`, `ArchivedEvents`, `FailedEvents` |
| Kafka | `ReadTopic(s)` (a fresh client, no group), `ConsumerLag(group)`, `NewEventProducer` with `Redeliver` (the same record again) and `Produce` (a new event framed with the real registry ids), `EventTopics` |
| Metrics | `WorkerMetrics()`, `CourseMetrics()`, `ScrapeMetrics(url)`; `Metrics.Sum`, `SumWhere` |
| SQL | `DB()` (every schema visible), `Count`, `MustCount`, `OutboxUnpublished`, `ProcessedCount`, `EventIDFor` |
| Waiting | `Eventually(t, timeout, what, cond)` (fails with the last observed value), `WaitFor`, `Never(t, window, what, holds)`, `WaitSettled(t, timeout)` (outbox drained and every consumer group at lag 0) |

**Conventions.** A scenario waits on a condition with a deadline, never on a fixed sleep alone; baselines are read after `WaitSettled`, and "nothing changed" checks run after it too, so a quiet system is not mistaken for a settled one. Users, courses and event ids are fresh per scenario. The services trust the loopback proxy and each request sends its own `X-Forwarded-For`, so the per-IP rate limits on register and login (5 and 20 a minute) do not throttle a suite that registers dozens of users. Email verification is not required to log in, so there is no verify step. The bootstrap admin is the one account the API cannot create: `Start` registers it and sets its role in the database.

**Adding E08 to E14** (a later agent): create `scenarios_*_e2e_test.go` files with the `e2e` tag; move the function out of the `integration`-tagged file together with its spec comment (the pending-spec script requires exactly one function per id) and remove its line from `ALLOWED` in `scripts/check-pending-e2e.sh`. What the specs need, and where it is:
- *Counter drift (E08)*: `Options.WorkerEnv` with `JOBS_ANALYTICS_RECONCILE_INTERVAL` (the cron minimum is one minute: the spec's 5 s needs a product change), then `DB().Exec` on `analytics.platform_snapshot`.
- *Load (E09)*: `NewStudent` in a loop and `harness.Do` from goroutines (return errors from the goroutines; `t.Fatal` is only for the test goroutine); `ConsumerLag` and `WorkerMetrics` for backpressure.
- *Redis flush (E10)*: `s.Env.RDB.FlushAll`; the stack already has the new services next to Redis.
- *Schema incompatibility (E11)*: `harness.Shared(t).RegistryURL` is the real registry; use `libs/events/registry` (`IsCompatible`, `Register`) against it.
- *Publish under fault (E12)*: `StopKafka`, `KillWorker`, and the Temporal address in `s.TemporalAddr`.
- *Archive and audit (E13)*: `Mongo.Find("audit_log", ...)`, `Mongo.Count("events", ...)`.
- *Access checks (E14)*: `harness.Do` with tokens from `NewStudent`, `NewInstructor`, `AdminToken`; the worker admin API on `s.WorkerAdminURL`.
- Tracing (E02) needs a Jaeger container; add it next to `startMailpit` in the harness and pass `OTEL_EXPORTER_OTLP_ENDPOINT` in `baseEnv`.

Add a test here when the behaviour under test depends on two services agreeing, for example one service writing something to Redis or the outbox that another reads. If it only concerns one service, put it in that service.
