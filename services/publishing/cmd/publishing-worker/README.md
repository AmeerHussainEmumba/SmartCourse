# services/publishing/cmd/publishing-worker

The Temporal worker for the publish workflow (`docs/plans/microservices-split.md` §5). One component, `publish-worker`, polls `TEMPORAL_TASK_QUEUE` (default `publish-workflows`) and runs `PublishCourseWorkflow`, registered under `publishing.WorkflowName`. The activities it schedules run in the course worker.

```
go run ./services/publishing/cmd/publishing-worker
docker build -f Dockerfile.publishing-worker .
```

## What's here

- `main.go`: `runtime.Main` with `WithoutDependencies`, `WithWorkflows` and `config.LoadWith(config.Requirements{})`. No database, Redis or Kafka, so no `POSTGRES_DSN`, `REDIS_ADDR` or `JWT_SIGNING_KEY` is needed.
- `wire.go`: the start/retry loop (a fresh Temporal client and worker each attempt, capped exponential backoff) and the `temporal` readiness check.

## Configuration

`TEMPORAL_HOST_PORT` (default `temporal:7233`), `TEMPORAL_NAMESPACE`, `TEMPORAL_TASK_QUEUE` (the workflow queue); `METRICS_HTTP_ADDR` (default `:9100`) and the `OTEL_*` settings. `TEMPORAL_ACTIVITY_QUEUE` is read by the starter and the course worker, not here: the workflow takes the activity queue from its input.

## Health

`/health/live` is 200 while the process runs. `/health/ready` is 200 `ready` once the worker started and Temporal answers, 200 `degraded` while Temporal is unreachable or the worker has not started (the process retries), 503 only while starting or shutting down. The compose profile that runs it is `workflow` (Wave C); host port 9115.
