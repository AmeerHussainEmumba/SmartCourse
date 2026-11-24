# services/course/cmd/course-api

SmartCourse's course/enrollment/progress HTTP service — one of the services `cmd/api` was split into (ADR-0023, extending ADR-0022's api/worker split).

```
go run ./services/course/cmd/course-api
docker compose --profile core up -d   # or containerized, alongside identity-api, analytics-api, worker + infra
```

## What's here

`main.go` is deliberately thin, same shape as every other `cmd/*-api`: load config, connect to Postgres/Redis (`libs/infra.Connect`), build the Gin engine, register health checks, register `services/course/course`, `services/course/enrollment`, and `services/course/progress`'s routes together (`registerRoutes` in `wire.go`), serve, handle `SIGTERM` for graceful shutdown.

## Why these three stay together

Enrollment locks the `courses` row (`SELECT ... FOR UPDATE`) in the exact same database transaction it writes an enrollment row in — that's ADR-0012's seat-limit guarantee, proven by `TestEnroll_SeatLimit_NeverExceededUnderConcurrency`. A same-transaction guarantee requires both operations to run in one process against one connection; splitting `course` and `enrollment` into separate binaries would force that lock to become a distributed transaction or a network round-trip mid-lock, which is exactly the kind of correctness risk the per-service-table proposal was already rejected for (`docs/rfc/differences-from-requirements.md` DR-010). `progress` rides along because progress rows are created at enrollment time and have no independent reason to be elsewhere. See ADR-0023 for the full reasoning.

This service's database connection is scoped to the `course`, `platform`, and `public` Postgres schemas (`POSTGRES_SEARCH_PATH`, ADR-0024) — `course`/`enrollment`/`progress` share one Postgres schema for the reason above, `platform` is where their outbox writes land (ADR-0006), and it never reaches into `identity` or `analytics` directly. The one place this service used to reach across what's now a schema boundary — `ListCatalogue` joining against `identity.users` for instructor names — now reads `course.instructor_mirror` instead, kept current by `services/worker/cmd/worker` consuming identity's events (`services/course/course/handlers.go`).

## Publishing (M2, ADR-0025)

`course.Service.Publish` holds a lazy Temporal client (`libs/temporal.Connect`, built from `TEMPORAL_HOST_PORT`/`TEMPORAL_NAMESPACE`/`TEMPORAL_TASK_QUEUE`) and starts `services/course/workflows.PublishCourseWorkflow` on `POST /courses/{id}/publish`, returning `202` immediately. Building the client is never fatal to this binary's boot, even under the plain `core` Compose profile where Temporal isn't running at all (it only exists from the `workflow` profile tier up) — only an actual publish call fails, cleanly, as a `503` until `make dev-workflow` brings Temporal up. `services/worker/cmd/worker` is what actually runs the workflow; this service only starts it and reads its result back from `GET /courses/{id}/publish-status`, straight off `course_versions`, never Temporal's own API.

## Asset upload (ADR-0019, RFC §11.7)

`registerRoutes` builds a `services/course/assets` store from `MINIO_*` and attaches it to the course service for `POST /assets/upload-url`. If it cannot be built the route answers `503 assets_unavailable` and nothing else is affected. Pre-signed URLs are written for `MINIO_PUBLIC_ENDPOINT` when set (the address a browser can reach), otherwise `MINIO_ENDPOINT`; inside Compose `MINIO_ENDPOINT` is `http://minio:9000`, which a browser cannot reach, so set the public one for real uploads. The `smartcourse-assets` bucket must exist (Compose's `minio-init` creates it) and, for browser uploads, allow CORS `PUT` from the client origin.

## Observability

`main.go` calls `deps.Observe` (tracing, RED metrics, `GET /metrics`, outbox gauges) and `temporal.Observe` (the client interceptor, so `POST /courses/{id}/publish` continues into the workflow run). `/metrics` is unauthenticated by design and on the API port; compose publishes it on `127.0.0.1` only. Tracing is a no-op without `OTEL_EXPORTER_OTLP_ENDPOINT` and never delays start-up when the collector is down. `wire_telemetry_test.go` proves a real archive request writes an outbox row with the request's trace id, span id and actor.
