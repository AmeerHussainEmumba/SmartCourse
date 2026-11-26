# services/analytics/cmd/analytics-api

SmartCourse's analytics HTTP service: the public read routes under `/api/v1/analytics/*`. See `services/analytics/README.md` for the service as a whole (schema, consumers, jobs).

```
go run ./services/analytics/cmd/analytics-api
docker compose --profile core up -d   # containerized, alongside the other services and infra
```

## What's here

`main.go` is one line over `libs/runtime`: `runtime.Main("analytics-api", wire)`. `wire.go` builds the Gin engine (`buildRouter`: CORS, body limit, tracing and RED middleware, the public `/health/*` routes, `registerRoutes`) and registers the HTTP server as the process's one critical component. `registerRoutes` attaches the analytics routes with the Redis popular-courses cache and the two issue sources.

The binary connects as the Postgres role `svc_analytics` (`POSTGRES_SEARCH_PATH=analytics,public`). It runs no consumers and no jobs: those are `analytics-worker`.

## Observability

`/metrics` is served **only on the internal listener** (`METRICS_HTTP_ADDR`, default `:9100`, published on `127.0.0.1:9103` in compose), together with `/health/live` and `/health/ready` (Postgres and Redis). The public API port has no `/metrics` (`TestAnalyticsAPI_PublicRouterHasNoMetrics`). RED metrics by route template, DB pool and Go runtime series; no outbox gauges (analytics has no outbox). `/metrics` is unauthenticated by design: bind the internal listener privately.

## GET /analytics/issues and MongoDB

Admin only, token version checked and **failing closed** (a missing Redis key is 401 `token_stale`, Redis unreachable is 503). The failed-events half reads the audit store's `failed_events` collection read-only through `libs/mongostore`: a documented, deliberate exception to service independence (ADR-0031 deviation note), connected lazily so start-up and every other route never depend on MongoDB. With `MONGO_URI` unset or MongoDB unreachable the route is 503, never zeros. The workflow-issues half is analytics' own `workflow_issue_stats`.

## Configuration

**Required:** `POSTGRES_DSN` (`svc_analytics`), `POSTGRES_SEARCH_PATH=analytics,public`, `REDIS_ADDR`, `JWT_SIGNING_KEY` (groups Postgres, Redis, JWT).

**Optional:** `HTTP_PORT`, `MONGO_URI` and `MONGO_*` (without it the failed-events route answers 503), `JWT_ISSUER`, `CORS_ALLOWED_ORIGINS`, the HTTP hardening settings, `METRICS_HTTP_ADDR`, `OTEL_*`.

**Not read:** Kafka, Schema Registry, MinIO, Argon. Declared in `main.go` as `requirements` and loaded with `config.LoadWith` through `runtime.WithConfig`; `config_test.go` proves the binary loads with only these variables and names a missing one. Per-variable reference: `env.example` and `libs/config/README.md`.
