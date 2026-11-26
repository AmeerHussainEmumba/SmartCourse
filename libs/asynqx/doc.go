// Package asynqx wraps Asynq (Redis-backed): weighted queues and bounded
// worker pools (ADR-0013, RFC §6.7, §10.2).
//
// The package deliberately stays thin. What it adds over raw Asynq:
//
//   - Client: every job carries a caller-supplied, deterministic task ID, so
//     enqueuing the same logical job twice is a reported no-op rather than a
//     second job (RFC §6.6: the delivery record makes duplicate sends unlikely;
//     this is the first half of that).
//   - Server: critical:default:low at 6:3:1, a bounded concurrency, a capped
//     exponential retry delay, a graceful drain bounded by a deadline, panic
//     recovery that counts as an ordinary failure, and a hook invoked once when
//     a task exhausts its retries.
//   - Inspector: queue depth and retry-set size for callers that export
//     metrics. This package depends on no metrics library.
//   - Scheduler: periodic jobs whose per-tick task ID is derived from the tick,
//     so N worker replicas registering the same entry enqueue one job per tick.
package asynqx
