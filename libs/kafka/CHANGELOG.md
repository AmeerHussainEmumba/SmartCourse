# Changelog — libs/kafka

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Fix: `Producer.Produce` no longer relies on franz-go alone to report a record. A test (the worker's broker stop/start with a relay backlog, short delivery timeout, callers retrying) left a record neither delivered nor failed and `ProduceSync` waited for ever (about 1 run in 3 without the fix). `Produce` now returns within `DeliveryTimeout` plus 3s (`ErrDeliveryStuck`) and replaces the client, whose Close fails anything it still buffered. `Close` is idempotent as before. The libs/kafka-level test does not reproduce the wedge (it passes without the fix); the regression test is the worker's `TestTransport_BrokerStoppedThenStarted_BacklogDrains`.

- Module created (Week 3 plan, Phase 3, ADR-0030). Imported by `services/worker/transport` since the transport landed (ADR-0029); no change to this module was needed.
- Producer: synchronous `Produce` with key, headers and context; idempotent producer, acks from all in-sync replicas, bounded retries with capped exponential backoff and jitter, a per-record delivery timeout, `Close`.
- Consumer: consumer-group runner with a `Handler`, one goroutine per assigned partition (order kept), `MaxInFlight` bound across partitions, manual offset commit only after the handler returns nil, handler retry budget (`MaxAttempts`, `Backoff`), `OnExhausted` hook followed by an offset advance, panic recovery (`ErrPanic`), rebalance-safe revoke and lost handling, `Run(ctx)` graceful drain bounded by `ShutdownTimeout` (`ErrDrainTimeout`), `Stats()`.
- `Ping` and `HealthCheck`: broker reachability, shaped for a non-fatal readiness check.
- `topics`: the RFC §6.5 topics with `Ensure` and `EnsureWith` (idempotent, never alters an existing topic); a unit test keeps the list equal to docker-compose's `kafka-init`.
- Both clients set `kgo.AlwaysRetryEOF()` so a broker restart behind a port proxy is retried instead of failing as a suspected TLS error.
- Depends on `github.com/twmb/franz-go` v1.22.3 and `github.com/twmb/franz-go/pkg/kadm` v1.20.0 (BSD-3-Clause). Test-only: `libs/testenv` (`SetupKafka`), `go.uber.org/goleak`.
