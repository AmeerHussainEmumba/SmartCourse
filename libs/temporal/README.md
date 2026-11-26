# libs/temporal

`Connect` builds a Temporal client with `client.NewLazyClient`, not `client.Dial` — it never fails at boot because Temporal is unreachable, only an actual publish call would (ADR-0009: "Temporal down means publishing unavailable — though nothing else is affected").

Its own package, apart from `libs/infra`, so that binaries that never touch Temporal (for example `services/identity/cmd/identity-api` and `services/analytics/cmd/analytics-api`) don't link the Temporal SDK. Importers: `services/course/cmd/course-api` (to start the publish workflow), `services/course/cmd/course-worker` (the activities worker) and `services/publishing/cmd/publishing-worker` (the orchestrating workflow worker); `test/e2e` imports it in tests. `tools/seed` does not (it drives the HTTP APIs). `scripts/check-link-graph.sh` enforces the no-Temporal rule for the other binaries, including the identity and analytics APIs.

## Testing

No dedicated test file — nothing here beyond constructing the client.

## Observability

`Observe(Observability{Tracing, Metrics})` before `Connect` adds interceptors to every client built afterwards (workers built on a client inherit them): the OpenTelemetry tracing interceptor and a counter for `workflow_started/completed/failed_total`. The counter counts a run when it first executes live on this process; replays after a restart or cache eviction are not counted, and a cancelled run counts as failed. Not verified against a real Temporal server; the tests use the SDK's test environment.

## Queues

The publish workflow and its activities run on different task queues (`libs/contracts/publishing`, `docs/plans/microservices-split.md` §5). `cfg.Temporal.TaskQueue` (`TEMPORAL_TASK_QUEUE`, default `publish-workflows`) is the queue the workflow starts on and the orchestrating worker polls; `cfg.Temporal.ActivityQueue` (`TEMPORAL_ACTIVITY_QUEUE`, default `course-activities`) is the queue the workflow schedules its activities on and the activity worker polls. `Connect` builds the client only; each worker names its queue.
