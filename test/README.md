# test/

Cross-cutting test infrastructure that doesn't belong to any one module. Most tests actually live *inside* each module (`internal/<module>/*_test.go`, right next to the code they test) — see `docs/guides/testing.md` for the full picture of what's tested and why. This directory holds the shared plumbing those tests depend on.

## What's here

- **`integration/testenv/`** — spins up real, disposable Postgres and Redis containers via `testcontainers-go`, applies every migration, and hands back connected clients. Every module's `*_integration_test.go` file calls `testenv.Setup(t)` rather than hand-rolling container setup, so container lifecycle (start, migrate, teardown) is written exactly once. See the package doc comment in `testenv.go` for the full reasoning, and `docs/guides/testing.md` for how to run the tests that use it.

## Why integration tests live in their module, not here

A test for `internal/enrollment`'s seat-limit logic belongs next to `internal/enrollment`'s code, not in a parallel directory tree that has to be kept in sync by hand. `test/` holds only what's genuinely shared — today, that's container setup. If cross-module end-to-end tests (spanning more than one domain module in a single test) become necessary later, they'd live here in `test/integration/`, separate from `testenv/`.
