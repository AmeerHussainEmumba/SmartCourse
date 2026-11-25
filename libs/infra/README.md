# libs/infra

The shared infrastructure clients every `cmd/*` binary needs: `Connect` opens Postgres (`libs/db`) and Redis (`libs/cache`) and returns them as `Dependencies`; `Dependencies.HealthCheckers` builds the checks `/health/ready` runs (`libs/router`).

It replaces the Postgres/Redis half of the old `libs/bootstrap` package. The other half — wiring each domain module — now lives in each binary's own `wire.go`, so a binary only links the modules it runs. This package imports no domain module and no Temporal code, on purpose: `scripts/check-link-graph.sh` (run by `make check-link-graph` and CI) fails if `identity-api` or `analytics-api` ever links either. See `docs/plans/module-split.md`.

## Packages

- `infra`: `Connect`, `Dependencies` (`HealthCheckers`, `ReadinessChecks`, `Close`, `Observe`) and `Of`. It imports `libs/db` and `libs/cache`, so it links pgx, GORM and go-redis. Pass `Connect` to `runtime.WithDependencies`.
- `infra/observe`: the tracing and metrics runtime (`observe.New`, `Observability`, `Options`) with no database code. `libs/runtime` imports only this, so a binary without a database links no driver. `infra.Observability` and `infra.ObserveOptions` are aliases of its types; `Dependencies.Observe` adds the database parts (`db_connections_*`, the outbox gauges, the GORM and go-redis tracing hooks) through `Options.SQLDB` and `Options.OnTracing`.

## Testing

No dedicated test file — `Connect` is exercised indirectly by every test that calls `libs/testenv.Setup`.

## Observability

`libs/runtime` makes the one call (`deps.Observe(ctx, cfg, infra.ObserveOptions{Service, Outbox, Workflows})`, or `observe.New` without dependencies); a binary does not call it itself except in tests. See `libs/telemetry/README.md` ("Wiring") for what it installs and `libs/config/README.md` for the environment. Run `Shutdown` with a deadline at exit. Importing this package also installs the `libs/auth` identity as the outbox actor resolver.

`/metrics` is served by the internal listener of `libs/runtime` (`Observability.MetricsHandler()`); there is no public mounting (`RegisterMetricsRoute` was removed).

**Used by** every binary that has a database: `identity-api`, `identity-worker`, `identity-admin`, `course-api`, `course-worker`, `analytics-api`, `analytics-worker`, `notification-worker`. `publishing-worker` and the audit binaries do not import it.
