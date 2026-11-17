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

Today it has one test, `TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks` (F-38). It uses a real admin role change, real logins and a real refresh to check that after a Redis restart an instructor whose token version is above 1 is refused cleanly, regains access by logging in or refreshing, and stays revoked after a demotion. It cannot live in either service's own tests because `course-api` cannot import identity.

Add a test here when the behaviour under test depends on two services agreeing, for example one service writing something to Redis or the outbox that another reads. If it only concerns one service, put it in that service.
