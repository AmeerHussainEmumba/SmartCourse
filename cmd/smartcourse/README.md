# cmd/smartcourse

The SmartCourse binary. One build, two run modes (RFC §4.2, ADR-0001):

```
go run ./cmd/smartcourse --mode=api      # HTTP server
go run ./cmd/smartcourse --mode=worker   # outbox relay, Kafka consumers, Asynq, Temporal worker
```

## Why one binary, two modes

A shared domain change (a new field, a changed validation rule) can't produce version skew between the API and the workers, because there's only ever one build and one image. `--mode` selects which loop runs at startup; both modes share every package under `internal/`.

## What's here

- **`main.go`** — process bootstrap: load config (fail-fast — see `internal/platform/config`), connect to Postgres/Redis, start the selected mode, handle `SIGTERM` for graceful shutdown.
- **`modules.go`** — `registerModules` wires every domain module's routes onto the shared Gin engine. This is deliberately the *only* file in the entire codebase that imports every `internal/<module>` package — it's the composition root. `internal/platform/router` never depends on domain code, and no module imports another (enforced by `.golangci.yml`'s `depguard` rules); this file is where they're all finally brought together.

## Current state

`--mode=worker` is a real, exercised code path but has no active workers yet — the outbox relay, Kafka consumers, and Asynq pools are M3 scope (`docs/rfc/smartcourse-rfc.md` §21.0). It exists now, doing nothing, specifically so the seam isn't introduced as an afterthought later.
