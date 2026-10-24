# cmd/worker

SmartCourse's background service — one of two independently built, deployed, and run binaries (ADR-0022; `cmd/api` is the other).

```
go run ./cmd/worker            # natively
docker compose --profile core up -d    # or containerized, alongside api + infra
```

## What's here

`main.go` is deliberately thin: load config, connect to Postgres/Redis (`internal/platform/bootstrap.Connect`), build the outbox relay with every module's handlers registered (`bootstrap.BuildRelay`), run it until `SIGTERM`, draining in-flight work before exiting (`internal/platform/relay`'s graceful shutdown — see that package's README for the bug its own tests caught here). No HTTP server at all — this binary never listens on a port.

## Current state

Runs the outbox relay (ADR-0021), with `internal/analytics` registered as its first handler. Kafka consumers, Asynq pools, and the Temporal worker are still M2/M3 scope (RFC §21.0) — when they land, this is where they get wired in, alongside the relay, not replacing it (the relay itself migrates to publishing-to-Kafka rather than disappearing — see ADR-0021's reversal trigger).

## Why a separate binary from the API

See `cmd/api/README.md` — same reasoning, same ADR-0022, just the other half of the split.
