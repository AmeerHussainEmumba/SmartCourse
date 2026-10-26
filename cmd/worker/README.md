# cmd/worker

SmartCourse's background service — one of four independently built, deployed, and run binaries (ADR-0022, ADR-0023; `cmd/identity-api`, `cmd/course-api`, and `cmd/analytics-api` are the other three).

```
go run ./cmd/worker            # natively
docker compose --profile core up -d    # or containerized, alongside the api services + infra
```

## What's here

`main.go` is deliberately thin: load config, connect to Postgres/Redis (`internal/platform/bootstrap.Connect`), build the outbox relay with every module's handlers registered (`bootstrap.BuildRelay`), run it until `SIGTERM`, draining in-flight work before exiting (`internal/platform/relay`'s graceful shutdown — see that package's README for the bug its own tests caught here). No HTTP server at all — this binary never listens on a port.

## Current state

Runs the outbox relay (ADR-0021), with `internal/analytics` and `internal/enrollment` registered as handlers today. Kafka consumers, Asynq pools, and the Temporal worker are still M2/M3 scope (RFC §21.0) — when they land, this is where they get wired in, alongside the relay, not replacing it (the relay itself migrates to publishing-to-Kafka rather than disappearing — see ADR-0021's reversal trigger).

## Why a separate binary from the API services

See `cmd/identity-api/README.md`, `cmd/course-api/README.md`, or `cmd/analytics-api/README.md` — same reasoning, ADR-0022 for the original api/worker split and ADR-0023 for api's further split, just the async half of it.
