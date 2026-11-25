# services/course/cmd/course-worker

The background half of the course service, built on `libs/runtime` (`main.go` is one call). It connects as the `svc_course` role (search_path `course,public`). Components run side by side; one failing does not stop the others (it is logged and shows as `component:<name>` in `/health/ready`).

| Component | Does | Critical |
|---|---|---|
| `relay` | `libs/relay` over `course.outbox`, PUBLISH-ONLY: no handler is registered, it only encodes (`libs/eventbus`), produces to Kafka and marks the row published after the broker ack | no |
| `consumers` | one `eventbus.Consumers` over `course.Handlers()` (instructor mirror), `enrollment.Handlers()` (course archival) and `certificate.Handlers()` (certificate issuer); one group per consumer name, each also on `retry.<name>`; exports `kafka_consumer_lag` | no |
| `jobs` | Asynq pool (low queue, `ASYNQ_LOW_CONCURRENCY`) and scheduler for the five jobs listed in `services/course/README.md`; exports the Asynq queue gauges | no |
| `activities` | the Temporal worker on `TEMPORAL_ACTIVITY_QUEUE`, activities registered under the `libs/contracts/publishing` names, asset store attached | yes, see below |
| `mongo` (only with `MONGO_URI`) | connects MongoDB in the background, retrying | no |

`activities` is critical because it returns only for a configuration that can never work (a bad Temporal host or namespace string, which the old worker exited on); a Temporal that is merely down or not ready is retried inside it with backoff. A permanent misconfiguration should not sit behind a healthy readiness probe.

Readiness: Postgres and Redis are fatal, Kafka (`kafka.HealthCheck`) is non-fatal, because the outbox buffers an outage (RFC §6.2). Start-up fails, and nothing runs, when topics cannot be provisioned or a schema subject is missing from the registry (`eventbus.Prepare`). The `workflow_*` counters (`runtime.WithWorkflows`) are not enabled: they belong to the process that runs the workflow worker.

## Dead letters

A consumer that spends its retry budget, or a row the relay cannot publish, is parked in MongoDB `failed_events` (`deadletter.go`) when `MONGO_URI` is set, idempotently per (event, consumer), with `dead_letter_total{consumer}` counted only for events really parked. A failed park returns an error, so the event stays retryable instead of being dropped. Without `MONGO_URI` there is no sink: the transport logs the event in full and drops it (the documented gap). `audit-api` lists and replays parked records by republishing to `retry.<consumer>`; this worker only writes them. Ported from `services/worker/deadletter` and `cmd/worker/mongo_wire.go` (the same park-before-drop contract, with unit and MongoDB tests).

## Config

`libs/config` plus one variable it does not know: `PROCESSED_EVENTS_RETENTION` (Go duration, default `336h`). `RELAY_WORKERS` sizes the relay pool; the Postgres pool must hold it plus the consumers' in-flight handlers.

## Tests

`wiring_test.go` and `deadletter_test.go` (no Docker): consumer groups and retry topics, the activity registration names against the contract, retention parsing, the five jobs and their specs, the Asynq pool, the park sink. `worker_integration_test.go` (Docker): the relay is publish-only over `course.outbox`; the whole worker through `runtime.Run` (readiness with Kafka, an event the course wrote goes through Kafka into the worker's own consumer, marker in `course.processed_events`); start-up fails naming Kafka when it is unreachable; MongoDB parking. Not covered here: the activities worker against a live Temporal server (E2E).
