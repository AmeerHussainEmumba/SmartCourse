# cmd/

Entry points. By Go convention, everything under `cmd/` is a `package main` that produces a runnable binary; all the actual logic lives in `internal/`. Two entry points today (ADR-0022):

- **`cmd/api/`** — the HTTP service. See its own README.
- **`cmd/worker/`** — the background service (outbox relay today; Kafka consumers, Asynq pools, and the Temporal worker land in M2/M3). See its own README.

Both are thin — a `main.go` that loads config, connects to Postgres/Redis, and calls into `internal/platform/bootstrap` (the composition root, RFC §4.4) to wire up every domain module. Neither binary contains real logic; if you're looking for where a route or a background job actually does something, you want `internal/`, not here.

This split used to be one binary (`cmd/smartcourse/`) selecting its mode via a `--mode` flag — see ADR-0022 for why that changed and what it cost.

A third binary (`cmd/seed/`, a one-off dev data seeder) is the natural next addition here, following the same thin-`main.go` pattern.
