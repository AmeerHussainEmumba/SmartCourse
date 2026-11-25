# libs/runtime

The process bootstrap every SmartCourse binary shares (`docs/plans/microservices-split.md` §6), so each `main.go` is a few identical lines. Import it with an alias when the file also imports the standard `runtime` package.

```go
func main() {
    runtime.Main("course-worker", func(p *runtime.Process) error {
        // p.Config (*config.Config), p.Logger, p.Obs (*observe.Observability, also infra.Observability),
        // p.Deps (what WithDependencies connected; infra.Of(p.Deps) is the *infra.Dependencies)
        p.Go("relay", func(ctx context.Context) error { rl.Run(ctx); return nil })
        p.Go("activities", runActivities, runtime.Critical())
        p.Ready("kafka", kafka.HealthCheck(p.Config.Kafka.Brokers), runtime.NonFatal())
        p.OnShutdown("producer", func(context.Context) error { pub.Close(); return nil })
        return nil
    }, runtime.WithDependencies(infra.Connect), runtime.WithOutbox())
}
```

**Dependencies are opt-in.** A binary that has a database passes `runtime.WithDependencies(infra.Connect)`; a binary without one (the publishing worker, the audit binaries) passes nothing and links no database driver: `libs/runtime` imports `libs/infra/observe` (telemetry) but never `libs/infra` (which pulls in `libs/db`, `libs/cache`, pgx, GORM and go-redis). `WithDependencies` is generic over anything implementing `runtime.Dependencies` (`Observe`, `ReadinessChecks`, `Close`), `*infra.Dependencies` being the one that exists. Inside the wiring function `p.Deps` is that value behind the interface; `infra.Of(p.Deps)` returns the `*infra.Dependencies` (nil without the option). `scripts/check-link-graph.sh` proves it: `publishing-worker` forbids pgx, gorm and go-redis.

## What Main does

1. `config.Load` (or `WithConfig`), then the logger (`logging.New`, `LOG_LEVEL`).
2. The constructor given to `WithDependencies` (`infra.Connect`: Postgres, Redis), if any. Its error stops start-up.
3. Telemetry: `Deps.Observe` (tracing, metrics, DB and Redis hooks), or `observe.New` without dependencies, with `WithOutbox()` (outbox gauges) and `WithWorkflows()` (workflow counters). A binary that runs the Temporal worker calls `temporal.Observe` itself with `p.Obs` (not imported here, so the Temporal SDK stays out of other binaries).
4. The **internal listener** on `METRICS_HTTP_ADDR` (default `:9100`): `GET /metrics`, `GET /health/live`, `GET /health/ready`, nothing else. Unauthenticated by design; bind it privately. A bind failure is logged loudly and does not stop the process.
5. Your wiring function, then every registered component starts. A wiring error starts nothing, runs the shutdown hooks and exits 1.
6. On SIGTERM or SIGINT (or a critical component's failure): readiness turns 503 `shutting_down`, the context is cancelled, components drain for at most `WithShutdownTimeout` (default 25 s), shutdown hooks run last-registered-first, telemetry is flushed (5 s), connections are closed. Exit 0 on a clean signal shutdown, 1 on anything else. A second signal kills the process at once.

## Components and independence

A component is `func(ctx) error` registered with `p.Go(name, fn, opts...)`. A panic is recovered (with the stack) and counts as an error. A component that fails or returns before shutdown is:

- **non-critical** (default): logged at ERROR (WARN for an early clean return), shown as `component:<name>: failed|stopped` in `/health/ready` (status `degraded`, still 200), and it does **not** stop the others. It is not restarted: a component that should retry owns its loop.
- **`Critical()`**: cancels the process, which shuts down and exits 1 with the component named.

## Readiness

`/health/live` answers 200 while the process runs and never looks at dependencies (an outage must not restart the pod). `/health/ready` runs the checks concurrently within 2 s: the dependencies' checks (Postgres and Redis for `infra.Connect`, fatal) plus everything added with `p.Ready`. A failing fatal check gives 503 `not_ready`; a failing `NonFatal()` check (Kafka: the outbox buffers an outage, RFC §6.2) gives 200 `degraded`. The body holds `ok`/`failed` per name and never the error text (it is unauthenticated); the detail goes to the log. Before the components are launched it is 503 `starting`.

## Reuse

`runtime.NewInternalServer(addr, metricsHandler, health)`, `runtime.ServeInternal(ctx, logger, srv)` and `runtime.NewHealth()` are the listener's parts; `services/worker` (until Wave B removes it) serves its listener through them.

## /metrics is never on a public port

`/metrics` is served only by the internal listener. `libs/infra`'s `RegisterMetricsRoute` (which mounted it on the API router) was removed in Wave C5; each API binary has a test that its public router answers 404 for it.

## Tests

Unit: component failure, panic and critical semantics, wiring error, bounded shutdown, hook order, listener routes, readiness (fatal, non-fatal, starting, shutting down, no error text in the body), bind failure, and `Main` itself in a child process (exit codes and a real SIGTERM). `WithDependencies` (a fake `Dependencies`: connected, observed, checked, closed; a constructor error stops start-up; no option means nil `Deps`). `-tags=integration`: `Run` with `WithDependencies(infra.Connect)` against real Postgres and Redis. Not verified: behaviour under an orchestrator (probe timing, SIGKILL after the grace period) and Windows signals.
