# cmd/api

SmartCourse's HTTP service — one of two independently built, deployed, and run binaries (ADR-0022; `cmd/worker` is the other).

```
go run ./cmd/api              # natively
docker compose --profile core up -d   # or containerized, alongside worker + infra
```

## What's here

`main.go` is deliberately thin: load config (fail-fast — `internal/platform/config`), connect to Postgres/Redis (`internal/platform/bootstrap.Connect`), build the Gin engine and register health checks + every domain module's routes (`bootstrap.RegisterModules`), serve, handle `SIGTERM` for graceful shutdown. No business logic lives here — if you're looking for what an endpoint actually does, you want the relevant `internal/<module>/handler.go`.

## Why a separate binary from the worker

Until ADR-0022, this was one binary with a `--mode=api`/`--mode=worker` flag. That meant an API replica and a worker replica were always the exact same build, which was simple but gave no independent restart, scale, or deploy — a fair gap flagged in review. Splitting into `cmd/api` and `cmd/worker` fixes that without touching the underlying architecture: both still import the same `internal/` packages and talk to the same PostgreSQL database (ADR-0001's modular monolith, unchanged) via `internal/platform/bootstrap`, the shared composition root neither binary could literally be `package main` and still import (Go doesn't allow importing another `main` package).

## Current state

Serves every synchronous route (RFC §3.4's sync/async boundary). Has no outbox relay, no background processing — that's entirely `cmd/worker`'s job. See `Dockerfile.api` for the container build and `docker-compose.yml`'s `api` service for how it's wired in Docker.
