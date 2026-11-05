# cmd/worker

SmartCourse's background service — one of four independently built, deployed, and run binaries (ADR-0022, ADR-0023; `cmd/identity-api`, `cmd/course-api`, and `cmd/analytics-api` are the other three).

```
go run ./cmd/worker            # natively
docker compose --profile core up -d    # or containerized, alongside the api services + infra
```

## What's here

`main.go` runs two independent things as separate goroutines, both bound to the same shutdown context: the outbox relay (`bootstrap.BuildRelay`, `internal/platform/relay`'s graceful shutdown — see that package's README for the bug its own tests caught here) and, as of M2, a Temporal worker executing `internal/workflows.PublishCourseWorkflow` (`bootstrap.BuildPublishWorker`). They're independent on purpose (ADR-0009/0025): a Temporal outage must never stop the relay, and vice versa. No HTTP server at all — this binary never listens on a port.

**The Temporal worker is built fresh on every connection attempt, not once at startup.** `startPublishWorker` retries with capped exponential backoff if `Start()` fails (e.g. `temporalio/auto-setup` hasn't finished creating the `default` namespace yet when this binary boots alongside it) — but it rebuilds the client and worker from scratch on every retry rather than reusing the pair that just failed. A live bug motivated this: reusing the same client/worker across retries kept failing with the identical stale error for minutes after the Temporal server was verifiably reachable again; a fresh process (and so a fresh client) connected within seconds. See ADR-0025 for the full writeup.

## Current state

Runs the outbox relay (ADR-0021) — `internal/analytics`, `internal/course`, and `internal/enrollment` registered as handlers — and the publish workflow's Temporal worker (ADR-0025, M2). Kafka consumers and Asynq pools are still M3 scope (RFC §21.0) — when they land, this is where they get wired in, as further independent goroutines, not replacing what's here (the relay itself migrates to publishing-to-Kafka rather than disappearing — see ADR-0021's reversal trigger).

This binary's database connection is the only one scoped to `platform`, `course`, and `analytics` schemas together (`POSTGRES_SEARCH_PATH`, ADR-0024) — not `identity`, since no handler here reads or writes identity's own tables, only the `full_name`/`role` fields already carried in identity's event payloads. It polls `platform.outbox` (every producer's events land there regardless of which schema produced them), and writes into `course.instructor_mirror`, `analytics`'s mirror and aggregate tables, and `course.enrollments` (via `enrollment.handleCourseArchived`), depending on which handler runs.

## Why a separate binary from the API services

See `cmd/identity-api/README.md`, `cmd/course-api/README.md`, or `cmd/analytics-api/README.md` — same reasoning, ADR-0022 for the original api/worker split and ADR-0023 for api's further split, just the async half of it.
