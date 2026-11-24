# Changelog — libs/asynqx

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Added `Servers`: one `Server` per queue, so each queue has its own bounded pool (RFC §10.2: `DefaultPoolSizes` critical 10, default 5, low 3). `Register(taskType, queue, fn)` takes a plain `func(ctx, payload []byte) error`, so packages plug handlers in without importing Asynq; `Start`/`Run` start every pool and `Shutdown` drains them concurrently (one `ShutdownTimeout` in total). Backoff, retry delay and `OnExhausted` apply to every pool. Additive: `Server` is unchanged.
- Added `BackoffConfig.Validate` (negative values, `Max` below `Base`, `Jitter` outside [0,1)); `NewServers` calls it, `NewServer` does not.

- Module created (Week 3 plan, ADR-0026).
- Client with required deterministic task ID (`DeterministicID`), duplicate enqueue reported as `Result.Duplicate`, queue, max-retry, timeout, delay and retention options.
- Server: 6:3:1 weighted queues, bounded concurrency, capped exponential backoff with jitter, `Run(ctx)` graceful drain bounded by `ShutdownTimeout`, `OnExhausted` hook (once per task, also for `SkipRetry`), handler panic recovery (`ErrPanic`, `PanicCount`).
- Inspector: per-queue `QueueStats` (pending, active, scheduled, retry, archived, completed) and `TaskState`.
- Scheduler: cron entries with per-tick deterministic task IDs so replicas enqueue once per tick; `@every` rejected. Built on `robfig/cron` plus `Client`, not `asynq.Scheduler` (see README).
- Depends on `github.com/hibiken/asynq` v0.26.0 (MIT) and `github.com/robfig/cron/v3` v3.0.1 (cron-spec parsing for tick alignment).
