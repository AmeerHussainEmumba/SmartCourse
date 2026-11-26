# services/analytics

The analytics service (docs/plans/microservices-split.md §2, §6): the platform and course metrics of SmartCourse FR-5, built from events. One Go module, two binaries, its own Postgres schema.

| Binary | What it does | Ports |
|---|---|---|
| `analytics-api` (`cmd/analytics-api`) | The public read routes under `/api/v1/analytics/*` (platform snapshot, per-course stats, daily enrollments, most popular courses, issues) | API port (`HTTP_PORT`, host 8092 in compose); internal `METRICS_HTTP_ADDR` (`:9100`, host 9103) |
| `analytics-worker` (`cmd/analytics-worker`) | The Kafka consumers that feed the mirrors and aggregates, the reconciler job, `processed_events` pruning | internal `METRICS_HTTP_ADDR` only (host 9113) |

Both are a few lines of `main.go` over `libs/runtime` (`runtime.Main`), which loads config, connects Postgres and Redis, wires telemetry, serves `/metrics`, `/health/live` and `/health/ready` on the **internal** listener, and shuts down gracefully. `/metrics` is never on the public API port.

Packages: `analytics` (handlers, repository, service, HTTP routes, reconciler, issues sources; see its README), `failedevents` (the Failed Events source over `libs/mongostore`), `schemaisolation` (the test that the service works on its own schema only).

## Data

Schema `analytics` (migrations in `migrations/analytics`): the event-fed mirrors `user_role_mirror`, `course_mirror`, `enrollment_mirror`, `publish_failures`; the aggregates `platform_snapshot`, `course_stats`, `daily_enrollments`, `workflow_issue_stats`; and `analytics.processed_events` (consumer idempotency, written in the same transaction as each handler's effect). Both binaries connect as the Postgres role `svc_analytics` with `POSTGRES_SEARCH_PATH=analytics,public`, which has no access to any other service's schema. Analytics **produces no events**: there is no outbox and no relay in either binary (a test scans for `relay.New(`, `outbox.Write` and a producer).

## What the worker consumes

`analytics.Handlers()`, all under the consumer name (and Kafka consumer group) `analytics-aggregator`, one consumer set subscribed to the union of the handlers' source topics and to its retry topic `retry.analytics-aggregator` (replay by republish lands there and takes the same idempotent path):

| Event | Effect |
|---|---|
| `identity.user_registered`, `identity.user_role_changed` | `user_role_mirror`, `platform_snapshot` |
| `course.course_published` | `course_mirror` (with the owner), `course_stats` |
| `enrollment.student_enrolled`, `enrollment.student_withdrawn` | `enrollment_mirror`, `course_stats`, `daily_enrollments` |
| `progress.course_completed` | `enrollment_mirror`, `course_stats` |
| `course.publish_failed` | `publish_failures`, `workflow_issue_stats` |

A handler that fails past `KAFKA_CONSUMER_MAX_ATTEMPTS` is parked in MongoDB `failed_events` through `libs/mongostore` before the offset advances, **only when `MONGO_URI` is set**; without it the event is logged in full and dropped (the documented gap, unchanged). MongoDB connects in the background and never blocks start-up; while it is down parking fails, which the consumers log loudly.

## Jobs (Asynq scheduler and low-queue servers, `libs/jobs`)

- **Reconciler**, `maintenance:analytics-reconcile`, every `JOBS_ANALYTICS_RECONCILE_INTERVAL` (default 1 minute): `analytics.NewReconciler(db, rdb).Run` recomputes every aggregate from the mirrors and rebuilds the Most Popular Courses sorted set (RFC §9.1, ADR-0017). Safe to run twice or from two replicas.
- **`PruneProcessedEvents`**, hourly, retention `PROCESSED_EVENTS_RETENTION` (default 14 days). It comes from `libs/config` (`cfg.Jobs.ProcessedEventsRetention`). The retention is the duplicate-absorption window; the `daily_enrollments` counter is the one aggregate the event path increments, so a duplicate that outlives its marker is applied twice until the next reconcile corrects it.

## Readiness

`/health/ready` on the internal listener: Postgres and Redis are fatal (503); Kafka and, when configured, MongoDB are non-fatal (200 `degraded`). A component that fails (`consumers`, `asynq`, `mongo`) is shown as `component:<name>` and does not stop the others.

## The MongoDB read exception (deliberate)

`GET /analytics/issues` (admin only) reports the failed-events metric by reading the audit store's `failed_events` collection **read-only through `libs/mongostore`** (`failedevents.Source`, one aggregation, no payloads), with MongoDB connected lazily on first use. That is a documented, deliberate exception to service independence (ADR-0031's deviation note): analytics-api is the one HTTP service allowed to link `libs/mongostore`. It does not import the audit service's code, only the shared library. With `MONGO_URI` unset or MongoDB unreachable the route answers 503, never zeros. The workflow-issues half comes from analytics' own `workflow_issue_stats`, fed by events, and reads no other service's data.

## Configuration

Per `libs/config` and `env.example`: `POSTGRES_DSN` (the `svc_analytics` DSN), `POSTGRES_SEARCH_PATH=analytics,public`, `REDIS_ADDR`, `JWT_*` (api), `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `KAFKA_CONSUMER_*` and `ASYNQ_*` (worker), `MONGO_URI` and its timeouts (optional), `METRICS_HTTP_ADDR`, `JOBS_ANALYTICS_RECONCILE_INTERVAL`, `PROCESSED_EVENTS_RETENTION`.

## Build and test

`Dockerfile.analytics-api`, `Dockerfile.analytics-worker` (both copy the same module set because both build one module; `scripts/check-dockerfile-modules.sh`). Package budgets in `scripts/check-link-graph.sh`: analytics-api must not link Kafka, Asynq, Temporal, `libs/eventbus` or `libs/jobs`.

`go test ./...` (unit) and `go test -race -tags=integration ./...` (Docker: Postgres, Redis, Kafka, MongoDB; the Schema Registry is the `libs/eventbus/transporttest` stand-in). `-tags='integration bugrepro'` adds the adversarial reproducers. Cross-service scenarios (events from the real producers through to these aggregates) are container E2E tests, not part of this module.
