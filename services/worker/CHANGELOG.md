# Changelog — services/worker

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Worker wiring (docs/plans/week-3.md wiring backlog 1 and 3): the Temporal worker's course service now has the asset store attached as course-api does (`newCourseService`); `RELAY_WORKERS` reaches the relay (`buildRelay` now takes the config); Asynq runs as a third goroutine, one server per queue with RFC §10.2's pools (`ASYNQ_*_CONCURRENCY`) and a scheduler. An Asynq or scheduler failure is logged and does not stop the relay or the Temporal worker.
- New package `jobs`: scheduled maintenance jobs on the `low` queue: stale-publish reconciliation (`ReconcileStalePublishes`, every minute), analytics reconciliation through a `Runner` interface (every minute; not scheduled until `analyticsReconciler` in `wire.go` returns one), enrollment counter repair, idempotency-key pruning, outbox pruning (`OUTBOX_RETENTION`, default 7 days). Intervals are configurable (`JOBS_*_INTERVAL`).
- `go.mod`: requires `libs/asynqx` and `libs/idempotency`; the Dockerfile copies `libs/asynqx`.

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
