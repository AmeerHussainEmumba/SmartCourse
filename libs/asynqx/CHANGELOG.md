# Changelog — libs/asynqx

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Module created (Week 3 plan, ADR-0026).
- Client with required deterministic task ID (`DeterministicID`), duplicate enqueue reported as `Result.Duplicate`, queue, max-retry, timeout, delay and retention options.
- Server: 6:3:1 weighted queues, bounded concurrency, capped exponential backoff with jitter, `Run(ctx)` graceful drain bounded by `ShutdownTimeout`, `OnExhausted` hook (once per task, also for `SkipRetry`), handler panic recovery (`ErrPanic`, `PanicCount`).
- Inspector: per-queue `QueueStats` (pending, active, scheduled, retry, archived, completed) and `TaskState`.
- Scheduler: cron entries with per-tick deterministic task IDs so replicas enqueue once per tick; `@every` rejected. Built on `robfig/cron` plus `Client`, not `asynq.Scheduler` (see README).
- Depends on `github.com/hibiken/asynq` v0.26.0 (MIT) and `github.com/robfig/cron/v3` v3.0.1 (cron-spec parsing for tick alignment).
