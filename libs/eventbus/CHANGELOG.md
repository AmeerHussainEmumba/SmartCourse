# Changelog — libs/eventbus

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Module created by moving `services/worker/transport` here with its tests (unit, integration against real Kafka and Postgres including the broker stop/start one, the fuzz targets and the bugrepro). Package `transport` is now `eventbus`; error messages start `eventbus:`; the registry stand-in is `eventbus/transporttest`.
- `Runner`/`RunnerConfig`/`NewRunner` became `Consumers` and `NewConsumers(cfg config.KafkaConfig, handlers []relay.Handler, db, opts...)` with options `WithSink`, `WithLogger`, `WithTracing`, `WithMetrics`: a service passes its own handlers and no `*relay.Relay`.
- Every consumer group also subscribes to `retry.<consumer-name>` and creates that topic at start; records from it go through the same `relay.DeliverEvent` path.
- Added `EncodeForRetry` and `Publisher.PublishRetry` (replay by republish), `NewPublisherFor(config.KafkaConfig, ...)`, `Consumers.Topics(group)`.
- Added `PruneProcessed` (`WithPruneBatch`): batched deletion of `processed_events` older than a retention, never a newer row.
- `Prepare` now provisions the retry topics too (via `topics.Ensure`).
