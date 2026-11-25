# services/notification/cmd/notification-worker

The notification service's only binary (`docs/plans/microservices-split.md` §6). It has no HTTP API; the only listener is the internal one that `libs/runtime` serves on `METRICS_HTTP_ADDR` (default `:9100`; host port 9114 in compose): `/metrics`, `/health/live`, `/health/ready`. Bind it privately, it is unauthenticated by design.

```
go run ./services/notification/cmd/notification-worker
docker build -f Dockerfile.notification-worker .
```

## Files

| File | Holds |
|---|---|
| `main.go` | `runtime.Main("notification-worker", wire)` |
| `wire.go` | `wire`: the consumers, the Asynq servers and scheduler, the sweeper, readiness checks; `buildAsynq`, `buildJobs`, `processedRetention`, the lag gauges, the SMTP readiness probe |
| `notify.go` | `newProvider`, `registerDelivery` (delivery task on the critical queue, `ErrPermanent` to `SkipRetry`), `deliveryTaskID` (5-minute bucket), the enqueuer adapter |
| `mongo.go` | the dead-letter sink over `libs/mongostore` (`failed_events`), built only when `MONGO_URI` is set |

## Behaviour to know

- Postgres and Redis readiness are fatal; Kafka, MongoDB and SMTP are non-fatal (`degraded`, still 200).
- Without `MONGO_URI` an exhausted event is logged and dropped; with it, it is parked first and dropped only after the park succeeds.
- Without `SMTP_HOST` there is no delivery task and no sweeper; deliveries stay `pending` and a warning says so.
- `PROCESSED_EVENTS_RETENTION` (default 336h) comes from `libs/config` (`cfg.Jobs.ProcessedEventsRetention`); zero, negative or unparsable stops startup.
- The pools, retries and backoff come from the existing `ASYNQ_*` settings; the delivery task timeout (2 minutes) stays below the claim lease (5 minutes).

**Asynq queue namespace.** This worker's queues are prefixed `notification:` (`notification:critical`, `notification:default`, `notification:low`; `asynqx.Namespace`) because every worker shares one Redis database and the same logical queue names; without the prefix a worker would dequeue and archive another service's tasks. The `queue` label of the `asynq_*` gauges carries the prefixed name.

The service and flow are described in `services/notification/README.md`.

## Configuration

**Required:** `POSTGRES_DSN` (`svc_notification`), `POSTGRES_SEARCH_PATH=notification,public`, `REDIS_ADDR`, `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL` (groups Postgres, Redis, Kafka).

**Optional:** `SMTP_*` (no delivery without `SMTP_HOST`), `MONGO_URI` and `MONGO_*` (dead-letter parking), `KAFKA_*` tuning, `ASYNQ_*`, `JOBS_OUTBOX_PRUNE_INTERVAL`, `PROCESSED_EVENTS_RETENTION`, `METRICS_HTTP_ADDR`, `OTEL_*`.

**Not read:** `JWT_SIGNING_KEY`, MinIO, Argon. Declared in `main.go` as `requirements` and loaded with `config.LoadWith` through `runtime.WithConfig`; `config_test.go` proves the binary loads with only these variables and names a missing one. Per-variable reference: `env.example` and `libs/config/README.md`.
