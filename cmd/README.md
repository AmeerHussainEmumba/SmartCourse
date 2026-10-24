# cmd/

Entry points. By Go convention, everything under `cmd/` is a `package main` that produces a runnable binary; all the actual logic lives in `internal/`. Three entry points today (ADR-0022):

- **`cmd/api/`** — the HTTP service. See its own README.
- **`cmd/worker/`** — the background service (outbox relay today; Kafka consumers, Asynq pools, and the Temporal worker land in M2/M3). See its own README.
- **`cmd/seed/`** — `make seed`: populates a local database with a realistic dataset through the real service layer, not raw SQL. See its own README.

`cmd/api` and `cmd/worker` are thin — a `main.go` that loads config, connects to Postgres/Redis, and calls into `internal/platform/bootstrap` (the composition root, RFC §4.4) to wire up every domain module. Neither contains real logic; if you're looking for where a route or a background job actually does something, you want `internal/`, not here.

This split used to be one binary (`cmd/smartcourse/`) selecting its mode via a `--mode` flag — see ADR-0022 for why that changed and what it cost.

If another binary is ever needed (a one-off migration tool, etc.), it gets its own directory here, following the same pattern.
