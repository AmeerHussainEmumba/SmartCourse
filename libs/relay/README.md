# libs/relay

The bounded worker pool that drains a service's `libs/outbox` table and hands each row to a `Publisher` (Kafka, through `libs/eventbus`). See `docs/adr/smartcourse-adr.md` ADR-0021 (why it exists; its in-process delivery was superseded and has been removed), ADR-0029 and ADR-0033 (one publish-only relay per service, Kafka the only transport).

**Current use.** Each service that owns an outbox runs one relay in its worker, publish-only: `identity-worker` (`identity.outbox`) and `course-worker` (`course.outbox`). The relay reads the unqualified `outbox` table, so the service's `search_path` selects its own. Analytics, notification and audit produce no events and run no relay; they import this module only for the `Handler`, `DecodedEvent`, `Deliver`, `DeadLetterSink` and `Source` types that `libs/eventbus` consumers use (`analytics-worker`, `notification-worker`, `audit-worker`, `audit-api`; the API binaries of analytics link it through the analytics package). `publishing-worker` does not use it.

**There is no in-process mode.** Earlier versions could run registered handlers inside the relay's claim transaction when built without a publisher (ADR-0021). ADR-0033 removed that transport from the system and the code has now been removed from this library too (`Register`, `Handlers`, `Publishing`, `WithPublisher`, `WithHandlerHook`, `HandlerHook`, `ModeProcess`, the claim query without the per-aggregate clause). `relay.New(db, publisher, opts...)` takes the `Publisher` as an argument and panics with an explanatory message on a nil publisher. Older ADRs and plans that mention the in-process relay are history.

## The shape

```
claim one row (FOR UPDATE SKIP LOCKED, per-aggregate order) → Publisher.Publish (broker ack) → mark the row published → commit
```

`workerCount` goroutines each claim ONE unpublished row in their own transaction and keep it (and its row lock) until the row is marked published or its attempt count and `last_error` are recorded. A worker takes no new row until it finishes the current one, which is the backpressure: nothing is queued in memory. Shutdown is graceful: cancelling the context stops new claims, and a `sync.WaitGroup` blocks `Run()`'s return until every in-flight worker finishes its row and commits.

## Why each primitive is here, not decorative

- **Goroutines** — the worker pool itself. This is the actual mechanism, not a wrapper around something sequential.
- **`sync.WaitGroup`** — waits for in-flight workers to finish their current row (and commit its result, see below) before `Run()` returns.
- **`sync.Mutex`** guarding `cooldown` — a process-local map of rows this instance just failed, so a failing row is not re-claimed in a hot loop. Cross-replica safety comes from each worker holding the `FOR UPDATE SKIP LOCKED` row lock in its own transaction until the publish is acknowledged and the row is marked published.
- **`sync/atomic`** — processed/failed/given-up counters (`Stats()`), alongside the Prometheus metrics the observer and `libs/eventbus` feed.

## The bug this package's own tests caught

Early versions passed the *shutdown* context straight into a worker's in-flight job. That meant the work could finish its Go code successfully after cancellation began, but its final database commit (marking the outbox row published) would fail with `context canceled` at the last moment. It looked graceful while silently losing the result. Fixed by detaching the per-row context (`context.WithoutCancel`, bounded by its own timeout) from the shutdown signal in `claimAndProcess()`. `TestRelay_GracefulShutdown_DrainsInFlightWork` asserts on the database row, specifically because an earlier version of that test only checked an in-memory flag and would have passed against this exact bug.

## Handlers (consumer side, not run by the relay)

A `Handler` is the unit a consumer runs: `relay.NewHandler(name, eventTypes, fn)` builds one, and a service passes its handlers to `eventbus.NewConsumers` (or `NewStreamConsumers`), which calls `relay.Deliver` / `DeliverEvent` per Kafka record. The relay never runs them.

```go
h := (relay.NewHandler("my-consumer", []string{"course.course_published"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
    // do real work using tx — its commit is what makes this exactly-once
    // from your consumer's perspective, per RFC §6.6
    return nil
}))
consumers, _ := eventbus.NewConsumers(kafkaCfg, []relay.Handler{h}, db)
```

Multiple handlers can handle the same event type — that's the fan-out RFC §6.5's consumer table describes (one event, several independent readers). One handler's failure never blocks another's; each runs in its own transaction with its own idempotency check.

For a test that wants to drive handlers from a real outbox without a broker, `libs/relay/relaytest.DeliverTo(db, handlers...)` is a `Publisher` that runs the matching handlers through `Deliver`, as a consumer would. A relay built on it retries, parks, gives up and keeps per-aggregate order exactly as in production. Production code must not import it.

## Trace and actor on the event

`DecodedEvent` carries `TraceID`, `SpanID` (strings, empty when the writer supplied none) and `ActorID` (`*uuid.UUID`, nil when absent), read from the outbox row (RFC §6.4). Rows written without a span or an authenticated actor have the columns NULL. The relay only passes them through; propagating them into spans or Kafka headers is the producer's job.

## Observability seam (ADR-0032)

```go
rl := relay.New(db, publisher,
    relay.WithObserver(func(ctx context.Context, evt relay.DecodedEvent, mode string) (context.Context, func(error)) {
        ctx, span := start(ctx, evt, mode) // mode is always relay.ModePublish
        return ctx, func(err error) { end(span, err) }
    }),
)
```

The observer runs once per delivery attempt, after the row is claimed, and the context it returns is the one the `Publisher` runs under, so a span started there is its parent. `finish` gets the attempt's error (attributed to `relay.RelayConsumer`) or nil. `DecodedEvent.TraceID`/`SpanID` are what an implementation continues from: the row stores the two ids, not a `traceparent`, and the span id is the writer's HTTP server span, so the relay's span is that span's child (`libs/telemetry.TraceparentFromIDs`, `StartRelay`). `libs/eventbus.RelayObserver` is the implementation. This is a hook, not an import of `libs/telemetry`, so binaries that link the relay without running it (analytics-api, for one) do not link tracing for it. A nil or panicking observer never fails a delivery. The Kafka consumers count their own outcomes in `libs/eventbus`. Relay log lines about a row carry `event_id` (RFC §12.4).

## Publishing (ADR-0029, ADR-0033)

```go
rl := relay.New(db, kafkaPublisher, relay.WithDeadLetterSink(sink))
```

`Publisher.Publish(ctx, DecodedEvent) error` returns nil only after the transport durably accepted the event (Kafka: the broker's acknowledgement); the relay marks the row published only then, and clears any `last_error` left by earlier failed tries (otherwise `outbox.Prune` would keep the row forever, B-09). A crash between the ack and the commit re-publishes the row, a duplicate the consumers absorb through `processed_events`; the other order would lose it. This package imports no Kafka client: the implementation is `libs/eventbus` (encoding with `libs/events`, routing by event-type prefix, the producer, the consumer groups). The relay stays the only path from the outbox to Kafka. The consumers in `libs/eventbus` call `relay.Deliver` (one transaction: skip if the consumer already processed the event, run the effect, write the `processed_events` marker).

- **Per-aggregate order.** The claim adds `AND NOT EXISTS (an earlier unpublished row of the same aggregate)`, so two workers never produce one aggregate's rows concurrently and a retried row cannot be overtaken. Other aggregates are not held back. The outbox has no index on `aggregate_id`, so the check scans the unpublished rows: fine for a small backlog, add an index (a migration) if it grows large.
- **A failed publish.** An ordinary error counts an attempt; at `maxAttempts` the event is parked (below) and the row marked published. An error marked `relay.Transient(err)` (the transport is down, not the event bad) records `last_error` and backs the row off (`WithPublishBackoff`, 1s) without counting an attempt, so an outage never dead-letters good events. A row that cannot be encoded does not block other aggregates; later rows of its own aggregate wait for its bounded retry budget.
- **A relay with no consumers in its process** (identity) is the normal case, covered by `relay_publish_only_integration_test.go`.

## Dead letters

```go
type DeadLetter struct { Event DecodedEvent; Consumer string; Err error; Attempts int }
type DeadLetterSink interface { Park(ctx context.Context, d DeadLetter) error }
relay.WithDeadLetterSink(s)
```

When the relay gives up on an event (after `maxAttempts`) it calls `Park` first (`relay.RelayConsumer` is the consumer, since the publish itself failed), and marks the row published only after `Park` returned nil. If `Park` fails (or panics), the row stays unpublished, the failure is logged and counted (`ParkFailures()`), and the next claim tries again: nothing is dropped unparked. `Park` may be called again for the same event and consumer, so it must be idempotent on (`Event.EventID`, `Consumer`). The Kafka consumers use the same sink: when it refuses a record, `libs/eventbus` holds the record instead of dropping it (B-06, B-07).

With no sink set the relay keeps its original behaviour: log at ERROR, keep the reason in `outbox.last_error`, mark the row published, drop the event. That default is the gap the sink closes (MongoDB `failed_events`, ADR-0031), not a recommendation.

## Known limitations (honest, not hidden)

- **Dead letters need a sink.** Without `WithDeadLetterSink`, a poison event past `maxAttempts` is marked published anyway (see above).
- **Multiple replicas are safe**: the claim transaction stays open through the publish, so a second replica skips a row being handled (F-04). A crashed replica releases the lock and the row is re-published (consumers dedupe).
- **The relay holds a connection per in-flight publish** (ADR-0027), so a slow broker holds `RELAY_WORKERS` connections.

## Testing

All of these use a fake `Publisher` against real Postgres (build tag `integration`) except where noted; the real-broker versions are in `libs/eventbus`.

- `relay_integration_test.go`: bounded concurrency is enforced (not just configured); a publish that fails twice is retried, published once, attempts counted and `last_error` cleared; a poison row stops after `maxAttempts`; shutdown drains the in-flight publish and persists its result.
- `relay_publish_integration_test.go`: a row is marked published only after the publisher returns nil, per-aggregate order with four workers, an outage not consuming the budget, a poison row parked without blocking other aggregates, a refused `Park` keeping the row, no sink still dropping, and `Deliver`'s idempotency and panic recovery.
- `relay_adversarial_integration_test.go`: uncommitted and rolled-back rows never published, two relays on one outbox publish each event once, a poison row at the head does not block the queue, cancel mid-batch then restart publishes every event exactly once, large and binary payloads, no leaked goroutines.
- `relay_regression_integration_test.go`, `relay_bugrepro_test.go`: the claim lock held through the publish (F-04), a publisher panic recovered (F-05), `last_error` kept for given-up rows (F-28) and cleared for rows that were later published (B-09).
- `relay_observer_integration_test.go`, `relay_trace_actor_integration_test.go`: the observer seam, `event_id` in log lines, the trace/actor fields and the trace/actor columns.
- `relay_test.go` (no Docker): `relay.New` panics without a publisher.

Run: `make test-integration`. Full index: `docs/guides/testing.md`.
