# libs/kafka

A pure-Go Kafka transport for SmartCourse: ADR-0030 (client choice), ADR-0021 (the relay), RFC §6.2, §6.5, §6.6, §6.9. `libs/eventbus` (formerly `services/worker/transport`) is the library that builds on it (ADR-0029, ADR-0033): each service's relay producer and consumer groups are built on `Producer`, `Consumer`, `topics` and `HealthCheck`. `libs/relay` does not import it. **Used directly (non-test imports) by** `identity-worker`, `course-worker`, `analytics-worker`, `notification-worker`, `audit-worker` and `audit-api` (the readiness `HealthCheck` and `topics`), and indirectly through `libs/eventbus` by the same binaries and `libs/jobs`. The APIs of identity, course and analytics do not use Kafka.

Built against franz-go v1.22.3 (`pkg/kgo`) with `pkg/kadm` v1.20.0 for topic administration, both BSD-3-Clause. Pure Go, `CGO_ENABLED=0`. It depends on no other SmartCourse module (`testenv` only in tests), and not on `libs/telemetry` or `libs/mongostore`: callers wire metrics and dead-letter parking through `Stats` and the `OnExhausted` hook.

It moves bytes. Envelopes, Protobuf and the schema registry live in `libs/events`.

## Producer

```go
p, _ := kafka.NewProducer(kafka.ProducerConfig{Brokers: brokers, ClientID: "relay"})
defer p.Close()

res, err := p.Produce(ctx, kafka.Message{
    Topic:   topics.Course,
    Key:     []byte(courseID), // the partition key: same key, same partition, in order
    Value:   framedEnvelope,
    Headers: []kafka.Header{{Key: "event_id", Value: []byte(eventID)}},
})
// res.Partition, res.Offset: acknowledged by every in-sync replica.
```

- **Synchronous**: `Produce` returns after the broker acknowledges (`acks=all`).
- **Idempotent producer** (franz-go's default; it has no switch to turn it on). A record the client re-sends after a lost ack or a broker restart is stored once.
- **Retries with backoff**: `MaxRetries` (default 10) with `Backoff` (default 200ms doubling to 10s, 20% jitter), inside a `DeliveryTimeout` (default 30s) per record. Size the timeout to the longest broker outage a single `Produce` call should ride out.
- **Ordering per key** holds for one caller that produces one record after another. Two goroutines producing the same key at once have no defined order.
- **Not covered by idempotence**: a caller that retries `Produce` itself after an error. An error after the delivery timeout or a cancelled context means the record may or may not be stored, and a caller retry can duplicate it. Consumers stay idempotent (RFC §6.6). The relay should mark an outbox row published only after `Produce` returns nil, and treat an error as "retry the row".
- `AlwaysRetryEOF` is on, see "What a test found" below.

## Consumer-group runner

```go
c, _ := kafka.NewConsumer(kafka.ConsumerConfig{
    Brokers:     brokers,
    GroupID:     "search-indexer",          // the consumer's name (RFC §6.5)
    Topics:      []string{topics.Course},
    MaxInFlight: 10,                        // default 10
    MaxAttempts: 5,                         // the retry budget, default 5
    Handler: func(ctx context.Context, rec kafka.Record) error { /* idempotent */ return nil },
    OnExhausted: func(ctx context.Context, rec kafka.Record, err error) { /* park in failed_events */ return nil },
    ShutdownTimeout: 30 * time.Second,
})
err := c.Run(ctx) // blocks; cancel ctx to drain
```

- **Order and parallelism**: one goroutine per assigned partition handles that partition's records one at a time, in order (RFC §10.2: one goroutine per partition). `MaxInFlight` caps how many handlers run at once across partitions, so a consumer with 12 partitions and `MaxInFlight` 3 never runs more than 3. A partition whose batch is being handled is paused, so fetched-but-unhandled data does not pile up.
- **At-least-once**: an offset is committed only after its handler returned nil, once per fetched batch (so a crash mid-batch redelivers the batch's already-handled records). Auto-commit is off. Handlers must be idempotent: `processed_events` in the same transaction as the effect (RFC §6.6) is what turns this into exactly-once effect.
- **Retry budget**: a handler error (or panic, recovered as `ErrPanic`) is retried after `Backoff`, up to `MaxAttempts` tries in all. While a record is retried its partition waits (order is kept) and the other partitions carry on; it does not hold a `MaxInFlight` slot while it sleeps.
- **Exhaustion**: after the last failed try `OnExhausted(ctx, rec, err)` runs once and then the offset advances, so a poison record never stalls its partition (RFC §6.9). The hook returns an error: nil means the record is dealt with and the offset advances; a non-nil error means it could not be parked and the record is left uncommitted (redelivered, with a fresh budget, to the next consumer of the group) and the partition's worker stops. A hook whose store is down should therefore hold, retrying with backoff, until it can park the record, and return an error only once its `ctx` is done (shutdown); `libs/eventbus` does this (B-06, B-07). A panicking hook is recovered, logged, and the record is skipped. With no hook the record is only logged.
- **Rebalance**: a partition taken away by a rebalance gets its current handler finished (up to `ShutdownTimeout`, then cancelled), its progress committed, and only then does the rebalance continue, so the next owner starts right after the last handled record. A partition lost without notice (session expired) has its handler cancelled and nothing committed.
- **Shutdown**: cancelling the context given to `Run` stops fetching; each in-flight handler is allowed to finish and its offset is committed; handlers still running at `ShutdownTimeout` get their context cancelled and their records stay uncommitted (redelivered, with a fresh budget, to the next consumer). `Run` then leaves the group and returns nil, or `ErrDrainTimeout` if it had to cancel. Records fetched but not yet started are dropped uncommitted. A handler that ignores its context is abandoned after a further 5 seconds and logged.
- **Handler contexts** are cancelled only when the runner gives up on in-flight work, not when `Run`'s context is cancelled. A handler that returns because of that cancellation is not counted as a failure.
- A new group starts at the beginning of each partition (`StartAtEnd` flips that).
- `Stats()` returns handled, failed, exhausted and panic counters and the in-flight count, for metrics. Consumer lag is not exported here; it needs the group's committed offsets against the log end, which the telemetry phase will derive (via `kadm`).

## Lag

`kafka.NewLagReader(brokers)` then `Lag(ctx, groups...)` returns `[]TopicLag{Group, Topic, Lag}`: how many records a group is behind the log end of each topic, summed over the topic's partitions (records fetched but not yet committed by the handler count as lag). Partition and offset are deliberately not in the result, so a metrics label built from it stays bounded. It errors rather than returning zero when the brokers cannot be asked (a fresh broker answers COORDINATOR_NOT_AVAILABLE until its offsets topic exists). Tested against a real broker (`TestLagReader`).

## Health

```go
checks["kafka"] = kafka.HealthCheck(brokers) // func(context.Context) error, the router.Checker shape
```

`Ping` opens a short-lived client and asks any broker for metadata, failing fast. Register it as a **non-fatal** check: the API keeps serving while Kafka is down because the outbox buffers events (RFC §6.2).

## topics

`topics.All()` is the RFC §6.5 list: `identity.events` (key `user_id`), `course.events` (`course_id`), `enrollment.events` (`course_id`), `progress.events` (`enrollment_id`). `topics.Ensure(ctx, brokers)` creates what is missing, with 3 partitions and replication factor 1: the same as docker-compose's `kafka-init` (`KAFKA_TOPIC_PARTITIONS` and `KAFKA_TOPIC_REPLICATION_FACTOR` defaults), and a unit test fails if the two topic lists diverge. Like `--if-not-exists` it never alters an existing topic: partitions are not grown. `EnsureWith` takes other numbers. The partition count is a placeholder; RFC §20 (D5) settles it under load. Not created: a dead-letter topic (RFC §6.9 parks to MongoDB) and compaction/retention settings (the broker default applies).

## What a test found

A broker that is stopped behind Docker's published port, or restarting behind a load balancer, accepts the TCP connection and closes it at once. franz-go reads an EOF on the first read as a TLS or SASL misconfiguration and does not retry, so a produce during a restart failed with "broker closed the connection immediately during api versions negotiation". `kgo.AlwaysRetryEOF()` opts out; both clients set it. The cost is that, once TLS or SASL is added, a real misconfiguration retries until `DeliveryTimeout` instead of failing at once. The broker-restart test covers the retry; nothing covers a misconfigured TLS client.

## Tests

`go test ./...` runs the unit tests (config validation, backoff, the topic list against docker-compose). `go test -tags=integration ./...` needs Docker and runs the integration suite against a real `apache/kafka:3.7.0` broker (the image docker-compose pins) through `testenv.SetupKafka`: round trip with key and headers, ordering per key across four partitions, a consumer cancelled mid-handler redelivering to the next consumer with nothing lost, a poison record not stalling the healthy record behind it, a graceful drain, bounded in-flight work, a consumer joining mid-stream, `Ping`, topic provisioning, and a broker stopped (gracefully and killed without grace) and restarted while four producers are mid-flight.

Not verified: a multi-broker cluster (replication, leader election, `min.insync.replicas`), TLS or SASL, throughput and lag under load, a Confluent-format consumer reading what the relay will produce (that is `libs/events`' open item), a producer whose ack is lost on a broker crash that also loses the producer-state snapshot (the kill test restarts a single broker with its data intact).

## Retry topics

`topics.Retry(name)` is `retry.<consumer-name>`. Each known consumer (`topics.Consumers()`: certificate-issuer, analytics-aggregator, course-instructor-mirror, enrollment-course-archival, notification-dispatcher, audit-archiver) has one, with the same partition count as the event topics (3). `topics.Ensure` and docker-compose's `kafka-init` create them; `TestAll_MatchesComposeKafkaInit` fails when the two lists differ and `TestRetryTopics_CoverEveryConsumerNameInTheCode` fails when a handler declares a consumer name with no retry topic. A consumer group reads its own retry topic through `libs/eventbus`.
