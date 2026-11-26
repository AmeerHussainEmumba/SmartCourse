# services/course/cmd/course-api

SmartCourse's course/enrollment/progress HTTP service — one of the services `cmd/api` was split into (ADR-0023, extending ADR-0022's api/worker split).

```
go run ./services/course/cmd/course-api
docker compose --profile core up -d   # or containerized, alongside the other services
```

## What's here

`main.go` is one `runtime.Main` call (`libs/runtime`); `wire.go` builds the lazy Temporal client, the public router (`newRouter`) and the HTTP server as a critical component, and registers the course, enrollment, progress and certificate routes together (`registerRoutes`). The bootstrap (config, Postgres as `svc_course`, Redis, telemetry, signals, graceful shutdown) is the runtime's. The background half is `cmd/course-worker`.

**`/metrics` is not on this binary's public router.** The runtime serves `/metrics`, `/health/live` and `/health/ready` on the internal listener (`METRICS_HTTP_ADDR`, default `:9100`); the public port keeps only its `/health` routes. `boot_integration_test.go` and `TestNewRouter_HasNoMetricsRoute` pin it.

## Why these three stay together

Enrollment locks the `courses` row (`SELECT ... FOR UPDATE`) in the exact same database transaction it writes an enrollment row in — that's ADR-0012's seat-limit guarantee, proven by `TestEnroll_SeatLimit_NeverExceededUnderConcurrency`. A same-transaction guarantee requires both operations to run in one process against one connection; splitting `course` and `enrollment` into separate binaries would force that lock to become a distributed transaction or a network round-trip mid-lock, which is exactly the kind of correctness risk the per-service-table proposal was already rejected for (`docs/rfc/differences-from-requirements.md` DR-010). `progress` rides along because progress rows are created at enrollment time and have no independent reason to be elsewhere. See ADR-0023 for the full reasoning.

This service's database connection is the `svc_course` role with search path `course,public` (`POSTGRES_SEARCH_PATH`, ADR-0024): `course`/`enrollment`/`progress`/`certificate` share one Postgres schema for the reason above, their outbox writes land in `course.outbox`, and the role cannot reach `identity` or `analytics` at all. The one place this service used to reach across what's now a schema boundary — `ListCatalogue` joining against `identity.users` for instructor names — now reads `course.instructor_mirror` instead, kept current by `cmd/course-worker` consuming identity's events (`services/course/course/handlers.go`).

## Publishing (M2, ADR-0025)

`course.Service.Publish` holds a lazy Temporal client (`libs/temporal.Connect`, built from `TEMPORAL_HOST_PORT`/`TEMPORAL_NAMESPACE`/`TEMPORAL_TASK_QUEUE`) and starts `PublishCourseWorkflow` by its contract name (`libs/contracts/publishing`) on the workflow queue `TEMPORAL_TASK_QUEUE`, passing `TEMPORAL_ACTIVITY_QUEUE` as the queue for its activities, on `POST /courses/{id}/publish`, returning `202` immediately. Building the client is never fatal to this binary's boot, even under the plain `core` Compose profile where Temporal isn't running at all (it only exists from the `workflow` profile tier up) — only an actual publish call fails, cleanly, as a `503` until `make dev-workflow` brings Temporal up. `services/publishing` runs the workflow and `cmd/course-worker` its activities; this service only starts it and reads its result back from `GET /courses/{id}/publish-status`, straight off `course_versions`, never Temporal's own API.

## Asset upload (ADR-0019, RFC §11.7)

`registerRoutes` builds a `services/course/assets` store from `MINIO_*` and attaches it to the course service for `POST /assets/upload-url`. If it cannot be built the route answers `503 assets_unavailable` and nothing else is affected. Pre-signed URLs are written for `MINIO_PUBLIC_ENDPOINT` when set (the address a browser can reach), otherwise `MINIO_ENDPOINT`; inside Compose `MINIO_ENDPOINT` is `http://minio:9000`, which a browser cannot reach, so set the public one for real uploads. The `smartcourse-assets` bucket must exist (Compose's `minio-init` creates it) and, for browser uploads, allow CORS `PUT` from the client origin.

## Observability

`runtime.Main` calls `deps.Observe` (tracing, RED metrics, outbox gauges via `WithOutbox`); `wire.go` calls `temporal.Observe` (the client interceptor, so `POST /courses/{id}/publish` continues into the workflow run). `/metrics` is served only on the internal listener, unauthenticated by design, so bind it privately. Tracing is a no-op without `OTEL_EXPORTER_OTLP_ENDPOINT` and never delays start-up when the collector is down. `wire_telemetry_test.go` proves a real archive request writes an outbox row with the request's trace id, span id and actor.

## Configuration

**Required:** `POSTGRES_DSN` (`svc_course`), `POSTGRES_SEARCH_PATH=course,public`, `REDIS_ADDR`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY_ID`, `MINIO_SECRET_ACCESS_KEY`, `JWT_SIGNING_KEY` (groups Postgres, Redis, MinIO, JWT).

**Optional:** `HTTP_PORT`, `MINIO_USE_SSL`, `MINIO_BUCKET_ASSETS`, `MINIO_PUBLIC_ENDPOINT`, `TEMPORAL_*` (dialled lazily), `JWT_ISSUER` and TTLs, `CORS_ALLOWED_ORIGINS`, the HTTP hardening settings, `METRICS_HTTP_ADDR`, `OTEL_*`.

**Not read:** Kafka (the outbox is relayed by course-worker), Argon (it hashes no password), MongoDB. Declared in `main.go` as `requirements` and loaded with `config.LoadWith` through `runtime.WithConfig`; `config_test.go` proves the binary loads with only these variables and names a missing one. Per-variable reference: `env.example` and `libs/config/README.md`.
