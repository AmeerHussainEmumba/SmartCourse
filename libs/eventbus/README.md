# libs/eventbus

The Kafka side of the event transport, shared by every service (ADR-0029, `docs/plans/microservices-split.md` §4). It was `services/worker/transport`; it now lives here so any service can publish its own outbox and consume the events it needs. `libs/relay` stays free of any Kafka client; this module is the only place the two meet. It builds on `libs/kafka` (producer, consumer-group runner, topics), `libs/events` (Envelope, schema registry client) and `libs/relay` (`Publisher`, `Handler`, `Deliver`, `DeadLetterSink`, `Source`).

**Used by** (non-test imports): `identity-worker` (publisher only), `course-worker` (publisher and transactional consumers), `analytics-worker` and `notification-worker` (transactional consumers over their own `processed_events`; neither produces events), `audit-worker` (stream consumer for the archiver) and `audit-api` (`PublishRetry`, to replay a parked event by republishing to `retry.<consumer>`). `publishing-worker` and the three other APIs do not use it. There is no in-process mode: Kafka is the only transport.

## What a service wires

```go
// Start-up: topics exist (event topics and every retry topic), schemas are registered.
schemas, err := eventbus.Prepare(ctx, k.Brokers, k.SchemaRegistryURL)

// Publishing: the relay only publishes. No handler is registered on it.
pub, _ := eventbus.NewPublisherFor(k, schemas, obs.TracingEnabled())
rl := relay.New(db, relay.WithPublisher(pub), relay.WithObserver(eventbus.RelayObserver()), relay.WithDeadLetterSink(sink))

// Consuming: built from the service's own handlers, no *relay.Relay involved.
consumers, _ := eventbus.NewConsumers(k, handlers, db,
    eventbus.WithSink(sink), eventbus.WithLogger(logger), eventbus.WithTracing(obs.TracingEnabled()), eventbus.WithMetrics(obs.Metrics))
go consumers.Run(ctx)

// Consuming with no Postgres (idempotency lives in the handler's own store):
consumers, _ := eventbus.NewStreamConsumers(k, handlers, eventbus.WithSink(sink), eventbus.WithLogger(logger))
```

`k` is `config.KafkaConfig`. `Prepare` creates the missing topics (`topics.Ensure`, which includes `retry.<consumer>` for every known consumer) and reads the schema ids and versions the producer frames records with; it fails with a message naming the broker or the missing subject. It does not register schemas (`libs/events/cmd/register-schemas` does, at deploy).

## Publisher

`Publisher.Publish` (the relay's `relay.Publisher`) encodes the outbox row into a registry-framed Envelope, keys it by aggregate id and returns nil only after the broker acknowledged it (`acks=all`), which is when the relay marks the row published. With tracing on it starts a PRODUCER span (a child of the relay's) and puts its `traceparent` in the record header. An unencodable row returns its error unmarked, so the relay spends the row's attempt budget and dead-letters it; a broker problem is `relay.Transient` (an outage does not use up the budget of good events), except records the broker rejects itself. `Publisher.PublishRetry` produces a stored event to a consumer's retry topic (below).

A relay with **no handlers** is a supported, tested mode (`libs/relay` `TestRelayPublishOnly_*`): publish mode never runs handlers, per-aggregate order is enforced by the claim query, and a row nobody here handles is published like any other.

## Consumers

`NewConsumers(cfg, handlers, db, opts...)` starts one consumer group per `ConsumerName` (group id = consumer name; several handlers under one name become one group on the union of their topics). Each record is decoded to a `relay.DecodedEvent` and run through `relay.DeliverEvent`: the handler and the `processed_events` marker commit in one transaction, and only then does `libs/kafka` commit the offset. At most `KAFKA_CONSUMER_MAX_IN_FLIGHT` handlers run per group. A handler that fails past `KAFKA_CONSUMER_MAX_ATTEMPTS` is parked through the `relay.DeadLetterSink` (retried three times, one history entry per attempt) and the offset advances, so a poison record never stalls its partition; with no sink the record is logged in full and dropped (the documented gap). The handler's context carries the record's `relay.Source` (topic, partition, offset, key, schema id) for handlers that keep it (`relay.SourceFrom`).

**Retry topic.** Every group also subscribes to `retry.<consumer-name>` (`topics.Retry`). A record from there takes exactly the same path, so a replay of an event the consumer already processed is absorbed by `processed_events`. A group creates its own retry topic at start (idempotent) so a new consumer name works before it is added to `topics.Consumers`.

`Consumers.LagGauges(m)` feeds `kafka_consumer_lag{group,topic}` (summed over partitions; the label set is bounded by the groups and their topics), `Stats()`, `Groups()`, `Topics(group)` and `ParkFailures()` report on the run. `event_processed_total{consumer,outcome}` is counted from `WithMetrics`; `dead_letter_total` is counted by the sink's store.

**Two modes, one runner.** `NewConsumers` is the transactional mode above. `NewStreamConsumers(cfg, handlers, opts...)` is for a service with no database whose idempotency lives elsewhere (the audit archiver dedups by event id in MongoDB): a handler is delivered **without a transaction and without a `processed_events` marker**. `relay.Handler.Handle` receives a nil `*gorm.DB`, so such a handler must not use it, and it must be safe to run more than once for one event: Kafka is at-least-once and a replay through the retry topic redelivers an event that was already handled (the marker would absorb it in the transactional mode; here nothing does, `TestStream_ReplayOfAHandledEventRunsTheHandlerAgain`). A handler panic is recovered into an error. Everything else is shared code, not a copy: the group per consumer name, the retry topic subscription, `MaxInFlight`, the retry budget, parking through the sink with one history entry per attempt, the `relay.Source` in the handler's context (and in the sink's context, which the audit sink stores), tracing and metrics (the `duplicate` outcome never occurs).

## Replay by republish

```go
msg, err := eventbus.EncodeForRetry(consumer, schemaID, evt) // kafka.Message on retry.<consumer>
err = pub.PublishRetry(ctx, consumer, schemaID, evt)         // produce it; nil only after the ack
```

`evt` is the event as the consumer decoded it and the sink stored it (JSON payload with the proto field names, `SchemaVersion` the registry version of the payload subject), `schemaID` the frame's schema id (`relay.Source.SchemaID`). The record decodes back to `evt` (`TestEncodeForRetry_DecodesBackToTheSameEvent`, every event type). An event that cannot be framed (unknown type, payload that does not match, missing ids) is `ErrUnencodable` and must stay parked.

## processed_events retention

`PruneProcessed(ctx, db, retention, opts...)` deletes markers older than the retention in batches (default 1000, `WithPruneBatch`) and never a newer one. Each service calls it on its own table with its own retention. The marker is the duplicate window: keep the retention longer than the Kafka topic retention and than the longest wait before a parked event is replayed, or a late duplicate is applied again.

## Codec and routing

`Codec.Encode` (outbox row to message), `Decode` (record to event, needs no registry state), `TopicFor(eventType)` (the topic follows the event type's prefix, not the aggregate type), `TopicsFor(handler)`, `LoadSchemas`, `Schemas`. Record headers: `event_id`, `event_type`, `outbox_id`, `traceparent`. `RelayObserver()` and `HandlerCounter(m)` are the relay's observability seams.

## transporttest

`transporttest.NewRegistry(t)` is an httptest stand-in for the Schema Registry's `GET /subjects/{subject}/versions/latest`, for tests that need schema ids without a registry container. It is not the real registry (the `libs/events` live tests cover registration against it).

## Tests

Unit (no Docker): `NewStreamConsumers` validation, codec round trips for every event type, routing, validation, `EncodeForRetry`, fuzz targets (`go test -fuzz`). `-tags=integration` (Docker: Postgres, Kafka). `modes_integration_test.go` runs the same scenarios in both modes (the stream mode against a stack with no Postgres): order, source, retry topic, exhaustion parking with history and metrics, a refusing sink, bounded in-flight, opt-in tracing. The rest (Postgres mode): per-aggregate order end to end, duplicate delivery, a consumer killed mid-batch, a broker stopped and started with a backlog, poison records, dead-letter parking, the source seam, the retry topic, publish-only relay feeding consumers built from handlers, `PruneProcessed`. `-tags=bugrepro` keeps one reproducer. `go test -race -tags=integration ./...` takes about two minutes.

Not verified: behaviour on a multi-broker cluster (the tests use one broker, replication factor 1), and the partition count under load (`topics.DefaultPartitions` is a placeholder, RFC §20 D5).
