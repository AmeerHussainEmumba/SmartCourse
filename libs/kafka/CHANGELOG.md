# Changelog — libs/kafka

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Module created (Week 3 plan, Phase 3, ADR-0030). Not imported by any service yet.
- Producer: synchronous `Produce` with key, headers and context; idempotent producer, acks from all in-sync replicas, bounded retries with capped exponential backoff and jitter, a per-record delivery timeout, `Close`.
- Consumer: consumer-group runner with a `Handler`, one goroutine per assigned partition (order kept), `MaxInFlight` bound across partitions, manual offset commit only after the handler returns nil, handler retry budget (`MaxAttempts`, `Backoff`), `OnExhausted` hook followed by an offset advance, panic recovery (`ErrPanic`), rebalance-safe revoke and lost handling, `Run(ctx)` graceful drain bounded by `ShutdownTimeout` (`ErrDrainTimeout`), `Stats()`.
- `Ping` and `HealthCheck`: broker reachability, shaped for a non-fatal readiness check.
- `topics`: the RFC §6.5 topics with `Ensure` and `EnsureWith` (idempotent, never alters an existing topic); a unit test keeps the list equal to docker-compose's `kafka-init`.
- Both clients set `kgo.AlwaysRetryEOF()` so a broker restart behind a port proxy is retried instead of failing as a suspected TLS error.
- Depends on `github.com/twmb/franz-go` v1.22.3 and `github.com/twmb/franz-go/pkg/kadm` v1.20.0 (BSD-3-Clause). Test-only: `libs/testenv` (`SetupKafka`), `go.uber.org/goleak`.
