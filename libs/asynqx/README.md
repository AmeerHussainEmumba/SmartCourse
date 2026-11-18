# libs/asynqx

A thin wrapper over [Asynq](https://pkg.go.dev/github.com/hibiken/asynq) (Redis-backed job queue) for SmartCourse background jobs: ADR-0013 (why Asynq), ADR-0014 (Kafka to Asynq handoff for notifications only), RFC §6.6, §6.7, §10.2, §12.3.

Built against Asynq v0.26.0 (MIT). Pure Go, no cgo. It depends on no other SmartCourse module (testenv only in tests), and not on `libs/telemetry` or `libs/mongostore`: callers wire metrics and failed-event parking through the hooks below.

## Client: idempotent enqueue

```go
c := asynqx.NewClient(asynqx.RedisClient(addr, "", 0))
defer c.Close()

res, err := c.Enqueue(ctx, asynqx.Job{
    Type:      "notification:send",
    Payload:   body,
    ID:        asynqx.DeterministicID("notification", eventID, recipientID), // required
    Queue:     asynqx.QueueCritical,
    MaxRetry:  8,
    Timeout:   20 * time.Second,
    Retention: 24 * time.Hour,
})
// res.Duplicate == true: the same logical job was already queued; not an error.
```

`ID` is required and must be derived from what makes the job logically unique. A repeat enqueue is a no-op and reports `Duplicate`. This only suppresses duplicates; it does not make delivery exactly-once (RFC §6.6: a rare duplicate send remains possible and is accepted).

Scope of the guarantee: a task ID is free again once its task is deleted. A completed task is deleted immediately unless `Retention` is set, so set `Retention` to the window in which duplicates must stay no-ops. An archived (exhausted) task keeps its ID until it is deleted from the archive.

Defaults: queue `default`, `MaxRetry` 5 (`NoRetry: true` for none), `Timeout` 30s.

## Server

```go
srv, _ := asynqx.NewServer(asynqx.ServerConfig{
    Redis:           redis,
    Concurrency:     10,                 // RFC §10.2; default 10
    ShutdownTimeout: 30 * time.Second,
    OnExhausted: func(ctx context.Context, f asynqx.FailedTask) {
        // park f as a failed event; keep it short
    },
})
srv.Handle("notification:send", handler)
err := srv.Run(ctx) // blocks; cancel ctx to drain
```

- **Queues** `critical:default:low` at 6:3:1, weighted (not strict priority), so `low` is never starved. Override with `Queues`.
- **Bounded concurrency**: `Concurrency` is the cap on simultaneously running handlers for the server. Asynq has one pool per server, so RFC §10.2's per-queue sizes (10/5/3) need one `Server` per queue (a single-entry `Queues` map each). The sum of all pools must stay below the database `MaxOpenConns`.
- **Retry delay**: `Base * 2^n` capped at `Max`, with jitter that only shortens (default 2s, 10m cap, 20%). Asynq stores the retry time with one-second resolution, so a retry can run up to a second earlier than the exact delay.
- **Graceful shutdown**: cancelling the context to `Run` stops pulling new tasks and waits up to `ShutdownTimeout` for in-flight handlers. At the deadline their contexts are cancelled and the tasks return to the queue to be retried (at-least-once, so handlers must be idempotent).
- **OnExhausted** is called once, on the final failed attempt (retries used up, or the handler returned `asynqx.SkipRetry`), before Asynq archives the task. A task whose handler returns `asynq.RevokeTask` is a deliberate discard and does not call it. If the process dies between the final attempt and the hook, the hook is not replayed.
- **Panics** in a handler are recovered and returned as an error wrapping `asynqx.ErrPanic`: an ordinary failure (retry, then `OnExhausted`), never a crash. `Server.PanicCount()` counts them.

## Inspector (feeding metrics)

```go
insp := asynqx.NewInspector(redis)
stats, _ := insp.Stats() // critical, default, low
for _, s := range stats {
    queueDepth.WithLabelValues(s.Queue).Set(float64(s.Depth())) // asynq_queue_depth
    retryCount.WithLabelValues(s.Queue).Set(float64(s.Retry))   // asynq_retry_count
}
```

`Retry` is the size of the retry set (failed, waiting for the next attempt), a gauge. If a counter of retry events is wanted instead, count in the handler from `asynqx.MetaFrom(ctx).Retried`. A queue that has never held a task reports zeros.

## Scheduler (periodic jobs)

```go
sc, _ := asynqx.NewScheduler(asynqx.SchedulerConfig{Redis: redis})
_ = sc.Register(asynqx.Entry{Name: "analytics-reconcile", Spec: "* * * * *", Type: "analytics:reconcile"})
_ = sc.Run(ctx)
```

This is a cron loop (`robfig/cron`, the library Asynq's own scheduler uses) feeding `Client.Enqueue`, not `asynq.Scheduler`. Asynq's scheduler fixes a task's options at registration, so it cannot give each tick its own task ID; rewriting the options per tick from its pre-enqueue hook was tried and the race detector caught Asynq's heartbeat reading them concurrently. The trade-off: entries do not show up in Asynq's scheduler view (asynqmon).

Run it in every worker replica. Each tick's task ID is derived from the entry `Name` and the tick time (`TickID`), so N replicas produce one task per tick; the others get a no-op (`DuplicateCount`). Rules that follow from this:

- `Spec` is a 5-field cron expression or `@hourly`/`@daily`. `@every 60s` is rejected: it counts from each process's start time, so replicas would never agree on a tick. For RFC §6.7's 60-second reconciliation use `* * * * *`.
- `Name` must be identical on every replica and unique per entry; a repeat in one process is `ErrDuplicateEntry`.
- Replicas must agree on the wall clock within `TickSkewTolerance` (10s). Completed tick tasks are retained for `TickRetention` (5 min) so a late replica cannot re-enqueue a tick that already ran.
- Entries default to the `low` queue.

## Tests

`go test ./...` runs the unit tests. `go test -tags=integration ./...` needs Docker (real Redis through `libs/testenv`, which also starts Postgres) and takes about 30 seconds, most of it the retry and scheduler tests: Asynq's one-second retry resolution and a real cron minute boundary cannot be sped up.

The weighted-queue test samples 500 dequeues at concurrency 1 and allows +-0.10 / +-0.10 / +-0.06 around 0.6 / 0.3 / 0.1 (4.5 to 5 standard deviations of the binomial share, see the comment on the test).
