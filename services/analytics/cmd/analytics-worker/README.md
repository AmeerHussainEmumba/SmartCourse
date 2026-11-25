# services/analytics/cmd/analytics-worker

The analytics service's background process. See `services/analytics/README.md` for the service as a whole.

```
go run ./services/analytics/cmd/analytics-worker
```

`main.go` is one line over `libs/runtime`: `runtime.Main("analytics-worker", wire)`. `wire.go` registers the components. There is **no outbox, no relay and no producer**: analytics emits no events (`TestWorkerHasNoOutboxOrRelay`, and `svc_analytics` cannot name an `outbox` table).

| Component | What | On failure |
|---|---|---|
| `consumers` | `eventbus.NewConsumers` over `analytics.Handlers()`: one consumer group, `analytics-aggregator`, on the handlers' source topics and `retry.analytics-aggregator`; the handler and `analytics.processed_events` commit in one transaction, then the offset. Topics are provisioned first (`eventbus.Prepare`), retried with backoff while Kafka is down | non-critical: shown as `component:consumers` in readiness |
| `mongo` (only when `MONGO_URI` is set) | Background connection for the dead-letter sink: an event that exhausts `KAFKA_CONSUMER_MAX_ATTEMPTS` is parked in `failed_events` through `libs/mongostore` before the offset advances | non-critical |
| `asynq` | `libs/asynqx` scheduler and per-queue servers; `libs/jobs` registers the jobs below on the low queue | non-critical |

Jobs: `maintenance:analytics-reconcile` (the reconciler over `analytics.NewReconciler(db, rdb)`, every `JOBS_ANALYTICS_RECONCILE_INTERVAL`, default 1 minute) and `maintenance:processed-events-prune` (hourly, `cfg.Jobs.ProcessedEventsRetention` = `PROCESSED_EVENTS_RETENTION`, default 336h).

Without `MONGO_URI` there is no sink: an exhausted event is logged in full and dropped, as the old worker did (logged at start-up with a warning).

## Readiness and observability

Internal listener only (`METRICS_HTTP_ADDR`, default `:9100`, host 9113 in compose): `/metrics`, `/health/live`, `/health/ready`. Postgres and Redis are fatal checks (503); `kafka` and `mongodb` are non-fatal (200 `degraded`). `event_processed_total{consumer,outcome}`, `kafka_consumer_lag`, `dead_letter_total` and the DB pool series are exported.

## Tests

Unit: consumers' group and topics (source topics plus the retry topic), jobs and their specs, the dead-letter mapping and counting, the no-outbox scan. `-tags=integration`: the binary under `runtime.Run` over real Postgres (as `svc_analytics`), Redis and Kafka: readiness and the internal listener, a record on a source topic and one on the retry topic both applied (a replay absorbed by `processed_events`), a reconcile task rebuilding wiped aggregates, a Kafka outage degrading readiness, MongoDB parking and an unreachable MongoDB. The Schema Registry is the `libs/eventbus/transporttest` stand-in.

## Configuration

**Required:** `POSTGRES_DSN` (`svc_analytics`), `POSTGRES_SEARCH_PATH=analytics,public`, `REDIS_ADDR`, `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL` (groups Postgres, Redis, Kafka).

**Optional:** `MONGO_URI` and `MONGO_*` (dead-letter parking), `KAFKA_*` tuning, `ASYNQ_*`, `JOBS_ANALYTICS_RECONCILE_INTERVAL`, `PROCESSED_EVENTS_RETENTION` (from `libs/config`, default `336h`, zero, negative or unparsable stops startup), `METRICS_HTTP_ADDR`, `OTEL_*`.

**Not read:** `JWT_SIGNING_KEY`, MinIO, Argon. Declared in `main.go` as `requirements` and loaded with `config.LoadWith` through `runtime.WithConfig`; `config_test.go` proves the binary loads with only these variables and names a missing one. Per-variable reference: `env.example` and `libs/config/README.md`.
