# cmd/identity-api

SmartCourse's identity HTTP service — one of the services `cmd/api` was split into (ADR-0023, extending ADR-0022's api/worker split).

```
go run ./cmd/identity-api
docker compose --profile core up -d   # or containerized, alongside course-api, analytics-api, worker + infra
```

## What's here

`main.go` is deliberately thin, same shape as every other `cmd/*-api`: load config, connect to Postgres/Redis (`internal/platform/bootstrap.Connect`), build the Gin engine, register health checks, register only `internal/identity`'s routes (`bootstrap.RegisterIdentityRoutes`), serve, handle `SIGTERM` for graceful shutdown.

## Why this split out cleanly

Identity is the most self-contained module in the codebase: no other module reads its tables directly (nor could it now — `identity` is its own Postgres schema, ADR-0024), and every module that needs to know who's calling reads it off the JWT claims (`auth.FromContext`), not a database lookup into `users`. Splitting its HTTP surface into its own binary doesn't introduce any new cross-service call — nothing changes about how `internal/platform/auth` issues or verifies tokens; it's still the same shared package every binary imports. The one thing `identity` does share with other services is its relay events (`identity.user_registered`, `identity.user_role_changed`, `identity.user_profile_updated`) — `internal/course` and `internal/analytics` each keep their own small mirror of fields they need, consumed by `cmd/worker`, never a live query into this service's schema. See ADR-0023 and ADR-0024 for the full reasoning.
