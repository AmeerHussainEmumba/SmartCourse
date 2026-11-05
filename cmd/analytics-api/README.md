# cmd/analytics-api

SmartCourse's analytics HTTP service — one of the services `cmd/api` was split into (ADR-0023, extending ADR-0022's api/worker split).

```
go run ./cmd/analytics-api
docker compose --profile core up -d   # or containerized, alongside identity-api, course-api, worker + infra
```

## What's here

`main.go` is deliberately thin, same shape as every other `cmd/*-api`: load config, connect to Postgres/Redis (`internal/platform/bootstrap.Connect`), build the Gin engine, register health checks, register only `internal/analytics`'s routes (`bootstrap.RegisterAnalyticsRoutes`), serve, handle `SIGTERM` for graceful shutdown.

## Why this split out first

Analytics is read-only and was already decoupled from every other module before this split — it learns about enrollments, courses, and progress through the relay/outbox (`internal/analytics`'s handlers, registered in `bootstrap.BuildRelay`), never through a direct call into another module's service layer. Splitting its HTTP surface into its own binary doesn't change any of that; the relay-side consumption is unaffected and still runs inside `cmd/worker`. See ADR-0023 for the full reasoning.
