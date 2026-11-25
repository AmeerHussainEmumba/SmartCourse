# libs/relay

The bounded worker pool that drains a service's `libs/outbox` table and hands each row to a `Publisher` (Kafka, through `libs/eventbus`). See `docs/adr/smartcourse-adr.md` ADR-0021 (why it exists; its in-process delivery is superseded), ADR-0029 and ADR-0033 (one publish-only relay per service, Kafka the only transport).

**Current use.** Each service that owns an outbox runs one relay in its worker, publish-only: `identity-worker` (`identity.outbox`) and `course-worker` (`course.outbox`). The relay reads the unqualified `outbox` table, so the service's `search_path` selects its own. Analytics, notification and audit produce no events and run no relay; they import this module only for the `Handler`, `DecodedEvent`, `Deliver`, `DeadLetterSink` and `Source` types that `libs/eventbus` consumers use (`analytics-worker`, `notification-worker`, `audit-worker`, `audit-api`; the API binaries of analytics link it through the analytics package). `publishing-worker` does not use it.

## The shape

```
poll (FOR UPDATE SKIP LOCKED) → jobs channel → N worker goroutines → Publisher.Publish (broker ack) → mark the row published
```

A fixed-size worker pool reads from a buffered channel the poller writes to. If every worker is busy, the channel fills and the poller's send blocks — that's the backpressure mechanism, not a separate thing bolted on. Shutdown is graceful: cancelling the context stops new polling, closes the job channel once nothing more will be sent, and a `sync.WaitGroup` blocks `Run()`'s return until every in-flight worker finishes.

## Why each primitive is here, not decorative

- **Goroutines + channel** — the worker pool itself. This is the actual mechanism, not a wrapper around something sequential.
- **`sync.WaitGroup`** — waits for in-flight workers to finish their current job (and commit its result — see below) before `Run()` returns.
- **`sync.Mutex`** guarding `cooldown` — a process-local map of rows this instance just failed, so a failing row is not re-claimed in a hot loop. Cross-replica safety comes from each worker holding the `FOR UPDATE SKIP LOCKED` row lock in its own transaction until the publish is acknowledged and the row is marked published.
- **`sync/atomic`** — processed/failed/given-up counters (`Stats()`), alongside the Prometheus metrics the observer and `libs/eventbus` feed.

## The bug this package's own tests caught

Early versions passed the *shutdown* context straight into a worker's in-flight job. That meant the work could finish its Go code successfully after cancellation began, but its final database commit — marking the outbox row published (and, in the handler path, writing `processed_events`) — would fail with `context canceled` at the last moment. It looked graceful (the handler ran to completion) while silently losing the result. Fixed by detaching the per-job context (`context.WithoutCancel`, bounded by its own timeout) from the shutdown signal in `worker()`. `TestRelay_GracefulShutdown_DrainsInFlightWork` asserts on the database row now, specifically because an earlier version of that test only checked an in-memory flag and would have passed against this exact bug.

## Handlers (consumer side, not run by the relay)

A `Handler` is the unit a consumer runs: `relay.NewHandler(name, eventTypes, fn)` builds one, and a service passes its handlers to `eventbus.NewConsumers` (or `NewStreamConsumers`), which calls `relay.Deliver` / `DeliverEvent` per Kafka record. The service's relay does not run them.

```go
h := (relay.NewHandler("my-consumer", []string{"course.course_published"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
    // do real work using tx — its commit is what makes this exactly-once
    // from your consumer's perspective, per RFC §6.6
    return nil
}))
consumers, _ := eventbus.NewConsumers(kafkaCfg, []relay.Handler{h}, db)
```

Multiple handlers can handle the same event type — that's the fan-out RFC §6.5's consumer table describes (one event, several independent readers). One handler's failure never blocks another's; each runs in its own transaction with its own idempotency check.

## Trace and actor on the event

`DecodedEvent` carries `TraceID`, `SpanID` (strings, empty when the writer supplied none) and `ActorID` (`*uuid.UUID`, nil when absent), read from the outbox row (RFC §6.4). Rows written without a span or an authenticated actor have the columns NULL. The relay only passes them through; propagating them into spans or Kafka headers is the handler's and the producer's job.

## Observability seams (ADR-0032)

```go
rl := relay.New(db,
    relay.WithObserver(func(ctx context.Context, evt relay.DecodedEvent, mode string) (context.Context, func(error)) {
        ctx, span := start(ctx, evt, mode) // mode is relay.ModePublish or relay.ModeProcess
        return ctx, func(err error) { end(span, err) }
    }),
    relay.WithHandlerHook(func(consumer string, duplicate bool, err error) { /* count */ }),
)
```

The observer runs once per delivery attempt, after the row is claimed, and the context it returns is the one the `Publisher` runs under (or the handlers, in the legacy no-publisher path described below), so a span started there is their parent. `finish` gets the attempt's error (the joined per-consumer failures) or nil. `DecodedEvent.TraceID`/`SpanID` are what an implementation continues from: the row stores the two ids, not a `traceparent`, and the span id is the writer's HTTP server span, so the relay's span is that span's child (`libs/telemetry.TraceparentFromIDs`, `StartRelay`). `libs/eventbus.RelayObserver` is the implementation. This is a hook, not an import of `libs/telemetry`, so binaries that link the relay without running it (analytics-api, for one) do not link tracing for it. `WithHandlerHook` only applies to handlers the relay runs itself (the legacy path); the Kafka consumers count their own outcomes. Relay log lines about a row carry `event_id` (RFC §12.4).

## Publish mode: the Kafka transport (ADR-0029, ADR-0033)

`relay.WithPublisher(p)` sets the per-row step to "hand the row to `p`":

```go
rl := relay.New(db, relay.WithPublisher(kafkaPublisher), relay.WithDeadLetterSink(sink))
```

`Publisher.Publish(ctx, DecodedEvent) error` returns nil only after the transport durably accepted the event (Kafka: the broker's acknowledgement); the relay marks the row published only then. A crash between the ack and the commit re-publishes the row, a duplicate the consumers absorb through `processed_events`; the other order would lose it. This package imports no Kafka client: the implementation is `libs/eventbus` (encoding with `libs/events`, routing by event-type prefix, the producer, the consumer groups). The relay stays the only path from the outbox to Kafka. Registered handlers are not run by the relay in this mode; the consumers in `libs/eventbus` call `relay.Deliver` (one transaction: skip if the consumer already processed the event, run the effect, write the `processed_events` marker).

- **Per-aggregate order.** The claim adds `AND NOT EXISTS (an earlier unpublished row of the same aggregate)`, so two workers never produce one aggregate's rows concurrently and a retried row cannot be overtaken. Other aggregates are not held back. The outbox has no index on `aggregate_id`, so the check scans the unpublished rows: fine for a small backlog, add an index (a migration) if it grows large. The legacy path (below) runs the original query without this clause.
- **A failed publish.** An ordinary error counts an attempt; at `maxAttempts` the event is parked (below) and the row marked published. An error marked `relay.Transient(err)` (the transport is down, not the event bad) records `last_error` and backs the row off (`WithPublishBackoff`, 1s) without counting an attempt, so an outage never dead-letters good events. A row that cannot be encoded does not block other aggregates; later rows of its own aggregate wait for its bounded retry budget.

## Legacy in-process path (no binary uses it)

ADR-0033 removed the in-process transport from the system: there is no `EVENT_TRANSPORT`, and no service constructs a relay without a publisher. The library code for it still exists: a `Relay` built without `WithPublisher` and with `Register`ed handlers runs those handlers in the claim transaction (`ModeProcess`, `WithHandlerHook`), and `relay_integration_test.go` and its siblings still exercise it. Treat it as unsupported and a candidate for removal (owner decision); do not build a service on it.

## Dead letters

```go
type DeadLetter struct { Event DecodedEvent; Consumer string; Err error; Attempts int }
type DeadLetterSink interface { Park(ctx context.Context, d DeadLetter) error }
relay.WithDeadLetterSink(s)
```

When the relay gives up on an event (after `maxAttempts`) it calls `Park` first, once per failed consumer (`relay.RelayConsumer` when the publish itself failed), and marks the row published only after every `Park` returned nil. If `Park` fails (or panics), the row stays unpublished, the failure is logged and counted (`ParkFailures()`), and the next claim tries again: nothing is dropped unparked. `Park` may be called again for the same event and consumer, so it must be idempotent on (`Event.EventID`, `Consumer`). The Kafka consumers use the same sink from `OnExhausted`.

With no sink set the relay keeps its original behaviour: log at ERROR, keep the reason in `outbox.last_error`, mark the row published, drop the event. That default is the gap the sink closes (MongoDB `failed_events`, ADR-0031), not a recommendation.

## Known limitations (honest, not hidden)

- **Dead letters need a sink.** Without `WithDeadLetterSink`, a poison event past `maxAttempts` is marked published anyway (see above).
- **Multiple replicas are safe**: the claim transaction stays open through the publish, so a second replica skips a row being handled (F-04). A crashed replica releases the lock and the row is redelivered (idempotent via `processed_events`).
- **Publish mode holds a connection per in-flight publish** (ADR-0027), so a slow broker holds `RELAY_WORKERS` connections.

## Testing

`relay_integration_test.go` (build tag `integration`, real Postgres): bounded concurrency is actually enforced (not just configured), idempotency holds across retries, a poison event stops retrying after the configured max, and shutdown genuinely drains in-flight work's database commit. `relay_publish_integration_test.go` covers publish mode with a fake `Publisher` and sink: a row is marked published only after the publisher returns nil, per-aggregate order with four workers, an outage not consuming the budget, a poison row parked without blocking other aggregates, a refused `Park` keeping the row, no sink still dropping, and `Deliver`'s idempotency and panic recovery. The real-broker versions are in `libs/eventbus`. `relay_trace_actor_integration_test.go` covers the trace/actor fields (persisted, NULL for existing callers, handed to handlers) and the up/down round trip of the trace/actor columns. Run: `make test-integration`. Full index: `docs/guides/testing.md`.

## Publish-only relay

In the per-service topology a relay only publishes its own outbox: `relay.New(db, relay.WithPublisher(eventbus.Publisher), ...)` with no `Register` call. That is a complete mode, covered by `relay_publish_only_integration_test.go`: the consumers of the events run in other processes through `libs/eventbus`, which takes the handlers directly (`eventbus.NewConsumers(cfg, handlers, db)`), not a `*Relay`.
