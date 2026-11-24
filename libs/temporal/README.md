# libs/temporal

`Connect` builds a Temporal client with `client.NewLazyClient`, not `client.Dial` — it never fails at boot because Temporal is unreachable, only an actual publish call would (ADR-0009: "Temporal down means publishing unavailable — though nothing else is affected").

Its own package, apart from `libs/infra`, so that `services/identity/cmd/identity-api` and `services/analytics/cmd/analytics-api` don't link the Temporal SDK. Only `services/course/cmd/course-api` (to start the publish workflow), `services/worker/cmd/worker` (to run it) and `tools/seed` import it. `scripts/check-link-graph.sh` enforces that for the two APIs.

## Testing

No dedicated test file — nothing here beyond constructing the client.

## Observability

`Observe(Observability{Tracing, Metrics})` before `Connect` adds interceptors to every client built afterwards (workers built on a client inherit them): the OpenTelemetry tracing interceptor and a counter for `workflow_started/completed/failed_total`. The counter counts a run when it first executes live on this process; replays after a restart or cache eviction are not counted, and a cancelled run counts as failed. Not verified against a real Temporal server; the tests use the SDK's test environment.
