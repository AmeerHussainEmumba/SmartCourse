# Changelog — libs/runtime

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Module created (microservices split, Wave A2): `Main`/`Run`, `Process` (`Go`, `Ready`, `OnShutdown`, `Context`), options (`WithOutbox`, `WithWorkflows`, `WithoutDependencies`, `WithShutdownTimeout`, `WithConfig`, `WithLogger`), `Health`, `NewInternalServer`, `ServeInternal`.
- The listener code (`newMetricsServer`, `serveMetrics`) moved here from `services/worker/cmd/worker/telemetry_wire.go`; the worker uses it, and its listener now also serves `/health/live` and `/health/ready`.
