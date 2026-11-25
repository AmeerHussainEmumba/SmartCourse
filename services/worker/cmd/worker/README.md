# services/worker/cmd/worker

SmartCourse's background service — one of four independently built, deployed, and run binaries (ADR-0022, ADR-0023; `services/identity/cmd/identity-api`, `services/course/cmd/course-api`, and `services/analytics/cmd/analytics-api` are the other three).

```
go run ./services/worker/cmd/worker            # natively
docker compose --profile core up -d    # or containerized, alongside the api services + infra
```

## What's here

`main.go` runs three independent things (the two below, plus the Asynq side described under its own heading) as separate goroutines, both bound to the same shutdown context: the outbox relay (`buildRelay` in `wire.go`, `libs/relay`'s graceful shutdown — see that package's README for the bug its own tests caught here) and, as of M2, a Temporal worker executing `services/course/workflows.PublishCourseWorkflow` (`buildPublishWorker` in `wire.go`). They're independent on purpose (ADR-0009/0025): a Temporal outage must never stop the relay, and vice versa. The only listener is the admin port (`ADMIN_HTTP_ADDR`, default 8093; `services/worker/admin`): health and the admin-only failed-event list and replay, run as a further goroutine beside MongoDB's background connection (`mongo_wire.go`). Neither can stop the relay.

**The Temporal worker is built fresh on every connection attempt, not once at startup.** `startPublishWorker` retries with capped exponential backoff if `Start()` fails (e.g. `temporalio/auto-setup` hasn't finished creating the `default` namespace yet when this binary boots alongside it) — but it rebuilds the client and worker from scratch on every retry rather than reusing the pair that just failed. A live bug motivated this: reusing the same client/worker across retries kept failing with the identical stale error for minutes after the Temporal server was verifiably reachable again; a fresh process (and so a fresh client) connected within seconds. See ADR-0025 for the full writeup.

## Asynq pools and scheduled jobs

`startAsynq` (`main.go`) is the third goroutine. It builds, through `wire.go`, one Asynq server per queue (`libs/asynqx.Servers`: critical, default and low each get their own bounded pool, RFC §10.2, 10/5/3 by default) and the scheduler that enqueues the maintenance jobs in `services/worker/jobs` on the `low` queue. On cancel it stops the scheduler first, then drains the pools (`ASYNQ_SHUTDOWN_TIMEOUT`). Any failure there is logged and ends only that goroutine: the relay and the Temporal worker keep running. Asynq does not fail at start while Redis is down; it retries its connection and logs, and the pools and scheduler recover when Redis returns.

Every replica runs the same scheduler. A tick's task ID is derived from the job name and the tick time, so N replicas enqueue one task per tick and one replica runs it. The jobs (intervals are env-configurable, whole minutes that divide 60 or whole hours that divide 24; see `env.example`):

| Job | Default | What it does |
|---|---|---|
| `maintenance:reconcile-stale-publishes` | 1m | `course.Service.ReconcileStalePublishes` (ADR-0028): releases `publishing` versions whose workflow is gone. Needs Temporal; a Temporal outage fails that tick and Asynq retries it |
| `maintenance:analytics-reconcile` | 1m | an `analytics` reconciler run through `jobs.Runner`. **Not scheduled until one is wired**: `analyticsReconciler` in `wire.go` returns nil today |
| `maintenance:enrollment-counts` | 1h | `enrollment.ReconcileEnrollmentCounts`: recomputes `courses.enrollment_count`, locking the course row |
| `maintenance:idempotency-prune` | 1h | `idempotency.PruneExpired` |
| `maintenance:outbox-prune` | 1h | `outbox.Prune` with `OUTBOX_RETENTION` (default 7 days); never deletes unpublished rows |

Other packages register Asynq handlers on the same servers with `Register(taskType, queue, fn)` (`buildAsynq` in `wire.go`); nothing but the jobs above is registered today (the notification dispatcher's delivery task is not wired here yet). The pools are not checked against `POSTGRES_MAX_OPEN_CONNS`: the relay's `2 x RELAY_WORKERS` is (ADR-0027), but the default and low pools also touch Postgres, so keep their sum plus the relay's within the pool. `RELAY_WORKERS` now reaches the relay (`relay.WithWorkerCount`). The Temporal worker's course service gets the asset store as course-api does (`newCourseService`); if the MinIO settings are unusable it is logged and left off, and `VerifyAssets` then fails any publish whose lessons reference an asset.

## Current state

Runs the outbox relay (ADR-0021) — `services/analytics/analytics`, `services/course/course`, and `services/course/enrollment` registered as handlers —, the publish workflow's Temporal worker (ADR-0025, M2) and the Asynq pools with the scheduled jobs above. With `EVENT_TRANSPORT=kafka` (ADR-0029) the relay publishes to Kafka instead of running the handlers, and a consumer group per consumer name runs them from the topics, as a fourth goroutine (`transport.go`; the code is in `libs/eventbus`). All Kafka wiring lives in `transport.go`: `transportMode(cfg)` says which mode is on, and any handler registered on the relay (for example an audit archiver that exists only in kafka mode: register it inside `if transportMode(cfg) == config.TransportKafka`) gets its own consumer group in kafka mode. `newEventTransport(..., sink)` takes the optional dead-letter sink; pass nil for log-and-drop. `HealthCheck()` is the non-fatal Kafka readiness check for when the worker gets an HTTP surface.

`wire_test.go`, `wire_asynq_test.go` and `wire_integration_test.go` guard the wiring (relay handlers, workflow and activities, `RELAY_WORKERS`, the asset store, the job set, pool sizes, and that a failing Asynq side leaves the relay running). `ReconcileStalePublishes` has not been run against a real Temporal server from here.

This binary's database connection is the only one scoped to `platform`, `course`, and `analytics` schemas together (`POSTGRES_SEARCH_PATH`, ADR-0024) — not `identity`, since no handler here reads or writes identity's own tables, only the `full_name`/`role` fields already carried in identity's event payloads. It polls `platform.outbox` (every producer's events land there regardless of which schema produced them), and writes into `course.instructor_mirror`, `analytics`'s mirror and aggregate tables, and `course.enrollments` (via `enrollment.handleCourseArchived`), depending on which handler runs.

## Why a separate binary from the API services

See `services/identity/cmd/identity-api/README.md`, `services/course/cmd/course-api/README.md`, or `services/analytics/cmd/analytics-api/README.md` — same reasoning, ADR-0022 for the original api/worker split and ADR-0023 for api's further split, just the async half of it.

## Observability (`telemetry_wire.go`)

`startTelemetry` (one deferred line in `main.go`) wires tracing, the DB and Redis hooks, the Temporal interceptors and `workflow_*` counters, the Asynq queue gauges, the outbox gauges and a `/metrics` listener on `METRICS_HTTP_ADDR` (default `:9100`, published on `127.0.0.1` only by compose). That listener serves only `GET /metrics` and has no authentication: it is deliberately not the admin port (8093, admin JWT). A port that cannot be bound is logged and the worker runs without `/metrics`. `startTelemetry` returns the `*infra.Observability`; `main` hands it to `newEventTransport` and builds the dead-letter sink over `countParked(mg, obs.Metrics)` (`mongoWiring.Sink`), so every parked event, from the relay's give-up or a Kafka consumer, counts in `dead_letter_total{consumer}`.

### Trace and metrics through the events (`transport.go`, `libs/eventbus`)

With tracing on (`OTEL_EXPORTER_OTLP_ENDPOINT`), one request is one trace through the pipeline: HTTP server span, the outbox insert, the relay span (`outbox relay publish|process`, child of the HTTP span: the row stores trace and span id), in kafka mode a produce span whose `traceparent` is a Kafka record header and a consume span per consumer group, handler DB spans under the consume span (or relay span in-process), then the notification delivery: the handler stores the traceparent on the delivery row, the sweeper enqueues under it and the Asynq job span is a descendant. Tracing off adds nothing (no header, no stored value, no span).

| Series | Fed by |
|---|---|
| `event_processed_total{consumer,outcome}` | Kafka runner per handler run (`processed`, `duplicate`, `failed`, `dead_lettered`); in-process, a relay hook (no `dead_lettered`: the relay's give-up is counted by `dead_letter_total`) |
| `kafka_consumer_lag{group,topic}` | `Runner.LagGauges` polls the brokers every 15 s; value is the total over the topic's partitions. Labels are the consumer name and the four provisioned topics, never a partition or an offset (that would depend on topic configuration and churn on rebalance). While the broker is unreachable the last value stays (stale, not zero) |
| `dead_letter_total{consumer}` | `countParked` around the Mongo park store |

kafka_consumer_lag exists only in kafka mode; `event_processed_total` for a group appears with its first event.
