# Changelog — libs/eventbus

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- README: consumers listed per binary (identity-worker, course-worker, analytics-worker, notification-worker, audit-worker, audit-api); wording from the Wave labels removed. No code change.

- `NewStreamConsumers(cfg, handlers, opts...)`: the consumer runner for a service with no Postgres. A handler is delivered without a transaction or `processed_events` marker (nil `*gorm.DB`), so it must be idempotent itself. Same group per consumer, retry topic, bounded in-flight, retry budget, parking with history, `relay.Source`, tracing and metrics as `NewConsumers`, which is now a thin wrapper over the same runner. It replaces the `services/audit/consume` copy.
- The dead-letter sink's context now carries the record's `relay.Source` (in both modes), so a sink can store topic, partition, offset and schema id as the handlers can.
- Tests: both modes covered in `modes_integration_test.go`; the Postgres stack now acts as the course service (`testenv.Env.ServiceDB`), because the owner connection no longer resolves `outbox` and `processed_events`. `go.mod`: `client_golang` and the OTel SDK are direct (test imports).

- Module created by moving `services/worker/transport` here with its tests (unit, integration against real Kafka and Postgres including the broker stop/start one, the fuzz targets and the bugrepro). Package `transport` is now `eventbus`; error messages start `eventbus:`; the registry stand-in is `eventbus/transporttest`.
- `Runner`/`RunnerConfig`/`NewRunner` became `Consumers` and `NewConsumers(cfg config.KafkaConfig, handlers []relay.Handler, db, opts...)` with options `WithSink`, `WithLogger`, `WithTracing`, `WithMetrics`: a service passes its own handlers and no `*relay.Relay`.
- Every consumer group also subscribes to `retry.<consumer-name>` and creates that topic at start; records from it go through the same `relay.DeliverEvent` path.
- Added `EncodeForRetry` and `Publisher.PublishRetry` (replay by republish), `NewPublisherFor(config.KafkaConfig, ...)`, `Consumers.Topics(group)`.
- Added `PruneProcessed` (`WithPruneBatch`): batched deletion of `processed_events` older than a retention, never a newer row.
- `Prepare` now provisions the retry topics too (via `topics.Ensure`).
