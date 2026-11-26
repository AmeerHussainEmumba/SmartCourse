# Changelog — libs/runtime

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- **Breaking.** Dependencies are opt-in: `WithoutDependencies` is removed (no option is the new default) and `WithDependencies(infra.Connect)` connects Postgres and Redis. `Process.Deps` is the `runtime.Dependencies` interface (`Observe`, `ReadinessChecks`, `Close`); `infra.Of(p.Deps)` returns the `*infra.Dependencies`. `Process.Obs` is `*observe.Observability` (the same type as `infra.Observability`). The package no longer imports `libs/infra`, only `libs/infra/observe`, so a binary that connects nothing no longer links pgx, GORM or go-redis (publishing-worker 648 to 583 packages).
- Doc: `/metrics` is served only by the internal listener; the public mounting is gone (`libs/infra` `RegisterMetricsRoute` removed).

- Module created (microservices split, Wave A2): `Main`/`Run`, `Process` (`Go`, `Ready`, `OnShutdown`, `Context`), options (`WithOutbox`, `WithWorkflows`, `WithoutDependencies`, `WithShutdownTimeout`, `WithConfig`, `WithLogger`), `Health`, `NewInternalServer`, `ServeInternal`.
- The listener code (`newMetricsServer`, `serveMetrics`) moved here from `services/worker/cmd/worker/telemetry_wire.go`; the worker uses it, and its listener now also serves `/health/live` and `/health/ready`.
