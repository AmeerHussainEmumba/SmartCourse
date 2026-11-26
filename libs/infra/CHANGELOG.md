# Changelog — libs/infra

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- README and comments updated for the split: consumers listed per binary; the outbox-gauge option now refers to the service's own outbox table (identity, course), not `platform.outbox`. No behaviour change.

- **Breaking.** `Observability.RegisterMetricsRoute` is removed: every binary serves `/metrics` on the internal listener (`libs/runtime`), never on the public router. `MetricsHandler()` stays.
- New package `libs/infra/observe`: the database-free tracing and metrics runtime (`observe.New`, `Observability`, `Options` with `SQLDB` and `OnTracing`). `infra.Observability` and `infra.ObserveOptions` are now aliases of its types, and `Dependencies.Observe` is a thin wrapper that supplies the database and the client hooks. `QueryOutboxStats` is no longer exported (it had no other caller).
- `Dependencies` gains `ReadinessChecks()` (plain func map), `Close()` and `Of(any) *Dependencies`, so `libs/runtime` can hold it behind an interface (`runtime.WithDependencies(infra.Connect)`) without importing this package.

- `Dependencies.Observe` accepts a nil or empty `Dependencies` (a binary with no database): no DB or Redis hook, no `db_*` or outbox series. Used by `libs/runtime` `WithoutDependencies`. `RegisterMetricsRoute` stays until Wave C.

- Added `Dependencies.Observe` / `Observability` (new file `observability.go`): tracing set-up that can never fail or block start-up, the GORM and go-redis hooks only when tracing is on, the metrics registry with RED, `db_connections_*` and (per binary) the outbox and workflow metrics, `Middleware()`, `RegisterMetricsRoute`, `MetricsHandler`, `Shutdown` (bounded flush). A metric a binary never feeds is not exported. The outbox gauges are refreshed by a 15 s poller, not by scrapes.
- Installs the `libs/auth` identity as the `outbox` actor resolver (`init`, new file `actor.go`).

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
