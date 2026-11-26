# Changelog — libs/asynqx

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- README: current users (identity-worker, course-worker, analytics-worker, notification-worker) and the real module imports (`libs/logging`, `libs/telemetry`); the "does not depend on telemetry" sentence was stale. No code change.

- Per-service queue namespace (fixes workers of different services consuming and archiving each other's tasks on shared queue names). `ServersConfig.Namespace`, `ServerConfig.Namespace`, `SchedulerConfig.Namespace` and `NewClient/NewInspector(redis, asynqx.WithNamespace(ns))` map the logical queues (`QueueCritical`, ...) to `<ns>:<queue>`; `Servers.Queues()`, `Result.Queue`, `QueueStats.Queue`, `MetaFrom().Queue` and the `queue` metric label carry the prefixed name. Tick task IDs include the namespace (`TickIDIn`). A prefixed name passed as a queue is rejected, so another service's queue cannot be named. The default (empty namespace) is unchanged. `Servers`, `Scheduler`, `Client` and `Inspector` gain `Namespace()`.

- Fixed retry gaps not following the backoff (E07): `ServerConfig.DelayedTaskCheckInterval` (and so `ServersConfig`) defaulted to Asynq's 5 s, so every retry waited for the next 5 s check and the gaps were about 5 s whatever `Backoff` said. Zero now means the new `DefaultDelayedTaskCheckInterval` (1 s); an explicit value is honoured and a negative one is rejected by `NewServer`. Behaviour change: retries now run roughly when scheduled (earlier than before for delays under 5 s).

- Trace propagation through the queue (ADR-0032): `Client.Enqueue` under a context that carries a trace starts an enqueue span and sets the task header `traceparent`; `Server.Handle` continues from that header (the job span is the enqueue span's child). The payload and the task id are untouched, so tasks already queued and untraced enqueues behave as before. New direct dependency in the client: `go.opentelemetry.io/otel/trace`.

- `Server.Handle` runs each attempt in a CONSUMER span (`libs/telemetry.StartJob`) and puts a logger carrying `job_id` in the handler context (`logging.Correlated(ctx)`); the panic log line carries job_id and trace ids. Additive: handlers that ignore the context behave as before.
- Added `Inspector.Gauges(*telemetry.Metrics)` for `asynq_queue_depth` (pending + scheduled) and `asynq_retry_count`.
- Fixed `Inspector.Stats` failing on a queue that never held a task: asynq v0.26.0's `GetQueueInfo` returns an internal NOT_FOUND error that `errors.Is(err, asynq.ErrQueueNotFound)` misses, so the documented zero-values answer never happened.
- New dependencies: `libs/telemetry`, `libs/logging`.

- Added `Servers`: one `Server` per queue, so each queue has its own bounded pool (RFC §10.2: `DefaultPoolSizes` critical 10, default 5, low 3). `Register(taskType, queue, fn)` takes a plain `func(ctx, payload []byte) error`, so packages plug handlers in without importing Asynq; `Start`/`Run` start every pool and `Shutdown` drains them concurrently (one `ShutdownTimeout` in total). Backoff, retry delay and `OnExhausted` apply to every pool. Additive: `Server` is unchanged.
- Added `BackoffConfig.Validate` (negative values, `Max` below `Base`, `Jitter` outside [0,1)); `NewServers` calls it, `NewServer` does not.

- Module created (Week 3 plan, ADR-0026).
- Client with required deterministic task ID (`DeterministicID`), duplicate enqueue reported as `Result.Duplicate`, queue, max-retry, timeout, delay and retention options.
- Server: 6:3:1 weighted queues, bounded concurrency, capped exponential backoff with jitter, `Run(ctx)` graceful drain bounded by `ShutdownTimeout`, `OnExhausted` hook (once per task, also for `SkipRetry`), handler panic recovery (`ErrPanic`, `PanicCount`).
- Inspector: per-queue `QueueStats` (pending, active, scheduled, retry, archived, completed) and `TaskState`.
- Scheduler: cron entries with per-tick deterministic task IDs so replicas enqueue once per tick; `@every` rejected. Built on `robfig/cron` plus `Client`, not `asynq.Scheduler` (see README).
- Depends on `github.com/hibiken/asynq` v0.26.0 (MIT) and `github.com/robfig/cron/v3` v3.0.1 (cron-spec parsing for tick alignment).
