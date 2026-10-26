# cmd/course-api

SmartCourse's course/enrollment/progress HTTP service — one of the services `cmd/api` was split into (ADR-0023, extending ADR-0022's api/worker split).

```
go run ./cmd/course-api
docker compose --profile core up -d   # or containerized, alongside identity-api, analytics-api, worker + infra
```

## What's here

`main.go` is deliberately thin, same shape as every other `cmd/*-api`: load config, connect to Postgres/Redis (`internal/platform/bootstrap.Connect`), build the Gin engine, register health checks, register `internal/course`, `internal/enrollment`, and `internal/progress`'s routes together (`bootstrap.RegisterCourseRoutes`), serve, handle `SIGTERM` for graceful shutdown.

## Why these three stay together

Enrollment locks the `courses` row (`SELECT ... FOR UPDATE`) in the exact same database transaction it writes an enrollment row in — that's ADR-0012's seat-limit guarantee, proven by `TestEnroll_SeatLimit_NeverExceededUnderConcurrency`. A same-transaction guarantee requires both operations to run in one process against one connection; splitting `course` and `enrollment` into separate binaries would force that lock to become a distributed transaction or a network round-trip mid-lock, which is exactly the kind of correctness risk the per-service-table proposal was already rejected for (`docs/rfc/differences-from-requirements.md` DR-010). `progress` rides along because progress rows are created at enrollment time and have no independent reason to be elsewhere. See ADR-0023 for the full reasoning.
