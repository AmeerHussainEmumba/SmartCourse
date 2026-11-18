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
