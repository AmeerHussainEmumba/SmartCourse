# services/notification

The notification service: it turns events into email without ever blocking the event stream on a mail server (RFC §6.5, §6.7, ADR-0014). One Go module, one binary (`notification-worker`), one Postgres schema (`notification`). It imports only `libs/*`, never another service (`docs/plans/microservices-split.md` §1; `scripts/check-module-graph.sh` enforces it).

| | |
|---|---|
| Module | `github.com/AmeerHussainEmumba/SmartCourse/services/notification` |
| Binary | `cmd/notification-worker` (no HTTP API; internal listener on `METRICS_HTTP_ADDR`, host port 9114 in compose, Wave C) |
| Package | `notification/` (handlers, dispatcher, delivery task, sweeper, SMTP provider, templates, sanitizer; its README has the detail) |
| Schema | `notification` (migrations `migrations/notification`): `recipients`, `course_titles`, `enrollment_courses`, `deliveries`, `processed_events` |
| Postgres role | `svc_notification`, `search_path` `notification,public` |
| Produces | nothing: no outbox, no relay |
| Consumes | `identity.user_registered`, `identity.user_profile_updated`, `enrollment.student_enrolled`, `progress.course_completed`, `progress.certificate_issued`, `course.course_published`, `course.publish_failed`; consumer group `notification-dispatcher` plus `retry.notification-dispatcher` |

## Flow

```
Kafka source topics + retry.notification-dispatcher
   -> eventbus consumers (notification-dispatcher)
      handler + notification.processed_events marker, one transaction
   -> notification.deliveries (status pending)         [rows only, handlers never enqueue]
   -> sweeper (every 2 s) -> Asynq critical queue -> delivery task -> SMTP provider -> sent
```

The delivery row is the source of truth. The sweeper is the single enqueue path: a crash between commit and enqueue, or a flushed Redis, leaves a `pending` row the next sweep enqueues. The task claims the row (`pending` to `sending`, a lease), renders, sends, marks `sent`; a duplicate task finds the row claimed or finished and does nothing. Statuses and failure handling are in `notification/README.md`.

### The duplicate-send window

Exactly-once delivery to an external system is impossible and is not claimed (RFC §6.6). A second mail can be sent when the provider accepted it but marking the row `sent` failed (the row stays `sending` until the lease, 5 minutes, expires), when a send outlives its lease, or when an SMTP connection drops after the final `.` of DATA before the reply. The trade-off is a rare duplicate rather than a lost mail; `Message-ID` is the delivery id, so a duplicate is recognisable. The delivery task timeout is 2 minutes, kept below the lease so a live send is never taken for an abandoned one. A permanent provider failure (a 5xx reply, a bad address) is archived by Asynq, not retried, and the row becomes `failed`.

## The binary

`runtime.Main("notification-worker", wire)` (`libs/runtime`). Components:

| Component | What it runs | Failure |
|---|---|---|
| `consumers` | `eventbus.NewConsumers` over `notification.Handlers()`; `kafka_consumer_lag` gauges when metrics are on | non-critical (logged, shown in readiness) |
| `asynq` | one server per queue (critical, default, low) with the pools from `ASYNQ_*` | non-critical |
| `scheduler` | the `asynqx` scheduler for the housekeeping jobs | non-critical |
| `sweeper` | `notification.RunSweeper`, registered only when a provider exists | non-critical |
| `mongodb` | background connection for the dead-letter sink, registered only when `MONGO_URI` is set | non-critical |

Readiness (`/health/ready` on the internal listener): `postgres` and `redis` are fatal (503); `kafka`, `mongodb` (if configured) and `smtp` (if configured; a TCP dial) are non-fatal (200 `degraded`). Start-up fails (exit 1) if Kafka topics or the schema registry cannot be prepared (`eventbus.Prepare`), if a jobs/Asynq setting is invalid, or if `PROCESSED_EVENTS_RETENTION` is malformed.

- **Dead letters.** An event that exhausts `KAFKA_CONSUMER_MAX_ATTEMPTS` is parked in MongoDB `failed_events` (one history entry per attempt) before the offset advances, only when `MONGO_URI` is set. Without Mongo it is logged in full and dropped (the documented gap), and the partition moves on. While Mongo is down `Park` fails and the consumer keeps the record retryable.
- **Provider.** `SMTP_HOST` unset: no provider, no delivery task, no sweeper; deliveries stay `pending` and a warning says so at start-up. Locally the provider points at Mailpit (SMTP 1025, UI 8025). No real mail provider is integrated, by decision (DR-008, DR-015).
- **Housekeeping.** `PruneProcessedEvents` (`libs/jobs`) on the `low` queue, cadence `JOBS_OUTBOX_PRUNE_INTERVAL` (default 1 h, the shared prune cadence; `libs/config` has no dedicated variable), retention `PROCESSED_EVENTS_RETENTION` (default 336h). The marker is the duplicate window: keep it longer than the Kafka topic retention and than the longest wait before a parked event is replayed.

## Running it locally

```
docker compose --profile core up -d      # Postgres, Redis, Kafka, Mongo, Mailpit (compose is reworked in Wave C)
SMTP_HOST=localhost SMTP_PORT=1025 ... go run ./services/notification/cmd/notification-worker
open http://localhost:8025               # Mailpit UI
```

Environment: `POSTGRES_DSN` (the `svc_notification` login), `POSTGRES_SEARCH_PATH=notification,public`, `REDIS_ADDR`, `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `SMTP_*`, `ASYNQ_*`, optional `MONGO_URI`, `PROCESSED_EVENTS_RETENTION`, `METRICS_HTTP_ADDR`. See `env.example`.

## Tests

`go test ./...` (unit, no Docker) and `go test -race -tags=integration ./...` (Docker: Postgres as `svc_notification`, Redis, Kafka, MongoDB, the Mailpit image). The package tests are described in `notification/README.md`. `cmd/notification-worker` has unit tests (delivery task id bucket, delivery registered on the critical queue, no provider registers nothing and warns, retention parsing, the job set) and integration tests that run the real `wire` through `runtime.Run`: events produced to a source topic and to the retry topic are consumed, swept and mailed through a fake SMTP server and recorded in `notification.processed_events`; readiness and `/metrics` on the internal listener; no provider leaves deliveries pending; a permanent SMTP failure is archived once and never retried; an exhausted event is parked in Mongo, or logged and dropped when Mongo is not configured.

Not verified: behaviour on a multi-broker cluster, a real Schema Registry (the tests use the `transporttest` stand-in), a real SMTP provider with TLS and credentials beyond the package's fake-server tests, container start-up of `Dockerfile.notification-worker` (the build was not run; only the module and link-graph checks), and rolling restarts under load.
