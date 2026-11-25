# Changelog — libs/config

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- `TemporalConfig.ActivityQueue` (`TEMPORAL_ACTIVITY_QUEUE`, default `course-activities`); `TEMPORAL_TASK_QUEUE` is now the WORKFLOW queue and its default changed from `course-publish` to `publish-workflows` (`libs/contracts/publishing`). A deployment that sets `TEMPORAL_TASK_QUEUE` keeps its value for the workflow, and the activities move to the activity queue: the worker serves both.

- Kafka consumer retry defaults lengthened to 8 attempts, 500 ms base, 30 s cap (about a minute in all), because a handler that depends on another consumer group's mirror (the certificate issuer needs the student name) must outlast that group lagging or starting; with the old 5 x 200 ms it could exhaust in about 3 seconds and park the event. Found by E2E scenario E01.
- Added `Config.Telemetry` (`TelemetryConfig`, new file `telemetry.go`): `OTEL_EXPORTER_OTLP_ENDPOINT` (host:port or http(s) URL; empty means tracing off), `OTEL_SDK_DISABLED`, `OTEL_SERVICE_NAME`, `OTEL_TRACES_SAMPLER_ARG` (0 to 1, default 1), `METRICS_ENABLED` (default true), `METRICS_HTTP_ADDR` (default `:9100`, the worker). All optional; shape-validated at `Load`, never dialled; a URL's credentials are refused and never echoed.

- Added `Config.Mongo` (new file `mongo.go`): `MONGO_URI` (optional; `mongodb://` or `mongodb+srv://`, never echoed in errors), `MONGO_DATABASE` (default `smartcourse`), `MONGO_CONNECT_TIMEOUT` (5s), `MONGO_SERVER_SELECTION_TIMEOUT` (3s), `MONGO_OP_TIMEOUT` (5s); timeouts must be above zero and at most 2m. And `Config.Admin` (new file `admin.go`): `ADMIN_HTTP_ADDR`, a `host:port`, default `:8093`. `Load` fails for a bad value in every binary; defaults are unaffected. Additive.
- Added `Config.Kafka` (new file `kafka.go`): `EVENT_TRANSPORT` (`inprocess` default, or `kafka`; any other value fails `Load` in every binary), `KAFKA_BROKERS` (comma-separated host:port) and `SCHEMA_REGISTRY_URL` (http or https), both required and validated only when the transport is `kafka`, and the tuning `KAFKA_CONSUMER_MAX_IN_FLIGHT` (1 to 256, default 4), `KAFKA_CONSUMER_MAX_ATTEMPTS` (1 to 100, default 5), `KAFKA_CONSUMER_BACKOFF_BASE` (200ms) and `KAFKA_CONSUMER_BACKOFF_MAX` (10s, at least the base), `KAFKA_CONSUMER_SHUTDOWN_TIMEOUT` (30s), `KAFKA_PRODUCE_TIMEOUT` (20s, at most 25s). `KafkaConfig.KafkaEnabled()`. The defaults leave behaviour unchanged. Additive.

- Added `Config.Asynq` and `Config.Jobs` (new file `asynq.go`): `ASYNQ_CRITICAL_CONCURRENCY` / `ASYNQ_DEFAULT_CONCURRENCY` / `ASYNQ_LOW_CONCURRENCY` (1 to 256; default 10/5/3, RFC §10.2), `ASYNQ_MAX_RETRY` (0 to 25, default 5), `ASYNQ_BACKOFF_BASE` (2s) and `ASYNQ_BACKOFF_MAX` (10m, at least the base), `ASYNQ_SHUTDOWN_TIMEOUT` (30s), `OUTBOX_RETENTION` (1h to 365d, default 168h) and the five scheduled-job intervals `JOBS_RECONCILE_PUBLISHES_INTERVAL`, `JOBS_ANALYTICS_RECONCILE_INTERVAL` (1m each), `JOBS_ENROLLMENT_COUNT_INTERVAL`, `JOBS_IDEMPOTENCY_PRUNE_INTERVAL`, `JOBS_OUTBOX_PRUNE_INTERVAL` (1h each). An interval must be a whole number of minutes that divides 60, or whole hours that divide 24 (`CronSpec` renders it as the cron expression the asynqx scheduler needs). `Load` fails for a value outside these in every binary; the defaults are unaffected. Additive.

- Added optional `MinIOConfig.PublicEndpoint` (`MINIO_PUBLIC_ENDPOINT`): the endpoint written into pre-signed URLs for browsers, when it differs from the internal `MINIO_ENDPOINT`. Additive; empty means the same as `Endpoint`.

- Added `RELAY_WORKERS` (1 to 64, default 4) as `Config.Relay.Workers`, and a startup check that `POSTGRES_MAX_OPEN_CONNS` is at least 2 x `RELAY_WORKERS` (ADR-0027: each relay worker holds a claim connection while its handlers need another). `Load` now fails for a smaller pool in every binary; the defaults (20 and 4) are unaffected. `services/worker` now passes `Relay.Workers` to `relay.WithWorkerCount`.

- Added `ARGON2_MAX_CONCURRENT` (1 to 1024; default derived from GOMAXPROCS and parallelism, clamped to 2..8) and `ARGON2_QUEUE_TIMEOUT` (default 5s, max 1m), exposed as `Argon2Config.MaxConcurrent` / `QueueTimeout`, plus `DefaultArgon2MaxConcurrent` (F-26 residue). Additive.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
