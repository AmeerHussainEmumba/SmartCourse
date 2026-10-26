# cmd/

Entry points. By Go convention, everything under `cmd/` is a `package main` that produces a runnable binary; all the actual logic lives in `internal/`. Five entry points today (ADR-0022, ADR-0023):

- **`cmd/identity-api/`** — HTTP service for `internal/identity` only. See its own README.
- **`cmd/course-api/`** — HTTP service for `internal/course`, `internal/enrollment`, and `internal/progress` together (kept in one process because enrollment locks the `courses` row in the same transaction it writes an enrollment row in — ADR-0012). See its own README.
- **`cmd/analytics-api/`** — HTTP service for `internal/analytics` only — read-only, already decoupled via the relay/outbox. See its own README.
- **`cmd/worker/`** — the background service (outbox relay today; Kafka consumers, Asynq pools, and the Temporal worker land in M2/M3). See its own README.
- **`cmd/seed/`** — `make seed`: populates a local database with a realistic dataset through the real service layer, not raw SQL. See its own README.

Every `cmd/*-api` and `cmd/worker` binary is thin — a `main.go` that loads config, connects to Postgres/Redis, and calls into `internal/platform/bootstrap` (the composition root, RFC §4.4) to wire up its slice of domain modules. None contain real logic; if you're looking for where a route or a background job actually does something, you want `internal/`, not here.

This used to be one binary (`cmd/smartcourse/`) selecting its mode via a `--mode` flag, then two (`cmd/api`, `cmd/worker`) — see ADR-0022 for that change, and ADR-0023 for why `cmd/api` was later split three ways into the services above.

If another binary is ever needed (a one-off migration tool, etc.), it gets its own directory here, following the same pattern.
