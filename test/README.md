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

Today it has one implemented test (plus the 14 pending specs described below), `TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks` (F-38). It uses a real admin role change, real logins and a real refresh to check that after a Redis restart an instructor whose token version is above 1 is refused cleanly, regains access by logging in or refreshing, and stays revoked after a demotion. It cannot live in either service's own tests because `course-api` cannot import identity.

### The Week 3 E2E tiers and how pending specs are tracked

Week 3's end-to-end checks (E1 to E14, `docs/plans/week-3.md` section 5) come in three tiers, from cheapest to most complete:

1. **In-process tier** (today's test): real identity and course routes, real Postgres and Redis through `libs/testenv`, nothing else. Runs in `make test-integration`.
2. **Container tier** (the `TestE2E_E01_` to `TestE2E_E14_` specs): real Postgres, Redis, Kafka, Schema Registry, MongoDB and Temporal through testcontainers, with the services run from their real wiring (the worker as a separate process where a test must `kill -9` it). Black box: HTTP in, assertions on responses, topics, Mongo, Redis, Mailpit, Jaeger and `/metrics`. Planned as its own CI job (parallel; on pull requests touching events code and nightly), not on every module.
3. **Compose tier**: `scripts/verify-deliverables.sh` against a stack started from the built images (`make dev-core`, `make dev-workflow`, later the events and observability profiles). It checks the rows of `docs/plans/week-3-acceptance-matrix.md` that can be checked over HTTP and prints `PASS`, `FAIL`, `PENDING` or `SKIP` per row. `--list` shows the rows, `--only <id>` runs one. It creates users and a published course in whichever database the stack uses, through the public API.

**A spec that is not implemented yet is a skipped test, not a missing one.** Each of E1 to E14 exists today in `test/e2e/scenarios_*_test.go` as a test that starts with `t.Skip("pending: Phase N - <what it needs>")`. Its doc comment is the exact procedure (steps, assertions, the helper it needs from a later phase), so the engineer who delivers that phase fills in the body and deletes the `t.Skip` line. Nothing is hidden: `go test -tags=integration -v ./...` reports each as SKIP with its phase, and `grep -rn 't.Skip("pending:' test/e2e` lists what is still open. A SKIP is not proof; a matrix row (`E01` to `E14`) closes only when the test runs and passes, and has been mutation-checked (break the guarded behaviour, see it fail).

Specs must not import modules that do not exist yet. When a spec needs something a later phase will add (a Kafka reader, a Mailpit client, a worker subprocess harness), the comment says so, and the helper is added with that phase.

Add a test here when the behaviour under test depends on two services agreeing, for example one service writing something to Redis or the outbox that another reads. If it only concerns one service, put it in that service.
