# libs/infra

The shared infrastructure clients every `cmd/*` binary needs: `Connect` opens Postgres (`libs/db`) and Redis (`libs/cache`) and returns them as `Dependencies`; `Dependencies.HealthCheckers` builds the checks `/health/ready` runs (`libs/router`).

It replaces the Postgres/Redis half of the old `libs/bootstrap` package. The other half — wiring each domain module — now lives in each binary's own `wire.go`, so a binary only links the modules it runs. This package imports no domain module and no Temporal code, on purpose: `scripts/check-link-graph.sh` (run by `make check-link-graph` and CI) fails if `identity-api` or `analytics-api` ever links either. See `docs/plans/module-split.md`.

## Testing

No dedicated test file — `Connect` is exercised indirectly by every test that calls `libs/testenv.Setup`.
