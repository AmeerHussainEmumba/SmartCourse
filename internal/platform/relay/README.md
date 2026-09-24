# internal/platform/relay

The bounded worker pool that drains `internal/platform/outbox` and dispatches each event to every registered handler. See `docs/adr/smartcourse-adr.md` ADR-0021 for why this exists now, ahead of Kafka (M3), rather than waiting.

## The shape

```
poll (FOR UPDATE SKIP LOCKED) → jobs channel → N worker goroutines → per-handler transaction (idempotency check + effect + processed_events marker)
```

A fixed-size worker pool reads from a buffered channel the poller writes to. If every worker is busy, the channel fills and the poller's send blocks — that's the backpressure mechanism, not a separate thing bolted on. Shutdown is graceful: cancelling the context stops new polling, closes the job channel once nothing more will be sent, and a `sync.WaitGroup` blocks `Run()`'s return until every in-flight worker finishes.

## Why each primitive is here, not decorative

- **Goroutines + channel** — the worker pool itself. This is the actual mechanism, not a wrapper around something sequential.
- **`sync.WaitGroup`** — waits for in-flight workers to finish their current job (and commit its result — see below) before `Run()` returns.
- **`sync.Mutex`** guarding `inFlight` — a legitimately process-local set: which outbox rows *this* relay instance currently has a worker on. Not Redis, on purpose — no other replica needs to see it; `FOR UPDATE SKIP LOCKED` already gives cross-replica safety at the database level, and `inFlight` only closes the narrower single-process gap between "claimed" and "a worker finished it."
- **`sync/atomic`** — processed/failed/given-up counters. A poor-man's metric ahead of Prometheus (M4).

## The bug this package's own tests caught

Early versions passed the *shutdown* context straight into a worker's in-flight job. That meant a handler could finish its Go code successfully after cancellation began, but its final database commit — writing `processed_events`, marking the outbox row published — would fail with `context canceled` at the last moment. It looked graceful (the handler ran to completion) while silently losing the result. Fixed by detaching the per-job context (`context.WithoutCancel`, bounded by its own timeout) from the shutdown signal in `worker()`. `TestRelay_GracefulShutdown_DrainsInFlightWork` asserts on the database row now, specifically because an earlier version of that test only checked an in-memory flag and would have passed against this exact bug.

## Registering a handler

```go
rl := relay.New(db)
rl.Register(relay.NewHandler("my-consumer", []string{"course.course_published"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
    // do real work using tx — its commit is what makes this exactly-once
    // from your consumer's perspective, per RFC §6.6
    return nil
}))
```

Multiple handlers can register for the same event type — that's the fan-out RFC §6.5's consumer table describes (one event, several independent readers). One handler's failure never blocks another's; each runs in its own transaction with its own idempotency check.

## Migrating to Kafka (M3)

When Kafka lands, this package's poll-and-dispatch loop is replaced by "publish to Kafka" at the point where a row is currently marked published. Today's in-process `HandlerFunc` becomes a Kafka consumer handler with the same signature — the fan-out and idempotency logic (check `processed_events`, act, mark processed, all in one transaction) doesn't change.

## Known limitations (honest, not hidden)

- **No dead-letter store.** A poison event past `maxAttempts` is marked published anyway rather than parked for inspection — MongoDB's `failed_events` collection (RFC §6.9) is M3 scope. Logged at `ERROR`, not silent, but not recoverable today.
- **Single relay instance assumed.** `SKIP LOCKED` is safe across multiple relay processes at the SQL level; `inFlight` protects only against one process's own re-polling. Running two relay instances today would rely on `SKIP LOCKED` alone, which is sufficient but untested here.

## Testing

`relay_integration_test.go` (build tag `integration`, real Postgres): bounded concurrency is actually enforced (not just configured), idempotency holds across retries, a poison event stops retrying after the configured max, and shutdown genuinely drains in-flight work's database commit. Run: `make test-integration`. Full index: `docs/guides/testing.md`.
