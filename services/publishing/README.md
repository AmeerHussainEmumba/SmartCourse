# services/publishing

The publishing service: the Temporal worker that runs `PublishCourseWorkflow`, and nothing else. Binary: `publishing-worker` (`cmd/publishing-worker`). Design: `docs/plans/microservices-split.md` §2 and §5; ADR-0009 and ADR-0025 for why publishing is the one Temporal workflow.

## Why it exists

Publishing a course touches Postgres, MinIO, the search index and Redis, and the old `services/worker` ran the workflow next to all of that, so it imported `course` code. The workflow itself is only orchestration: an activity graph, retry policies and failure handling. That part lives here, with no database, no Redis, no Kafka and no import of any service. The data-owning work runs in the course worker as Temporal activities.

## The contract

Everything the three parties share is in `libs/contracts/publishing`: the workflow name (`publishing.WorkflowName`), the two task queues (`publish-workflows`, `course-activities`; `TEMPORAL_TASK_QUEUE` and `TEMPORAL_ACTIVITY_QUEUE` override them), the activity names, `PublishWorkflowInput` and `PublishOutline`.

- The workflow is registered under `publishing.WorkflowName` on `cfg.Temporal.TaskQueue` (`publish.Register`).
- It calls every activity BY NAME (the contract constants) with `TaskQueue` = `PublishWorkflowInput.ActivityQueueOrDefault()`. It never imports the activity implementations.
- Activity graph (`publish/workflow.go`): `ValidateMetadata`, then `VerifyAssets` and `ComputeMetadata` in parallel, `BuildOutline`, then `IndexSearch` and `WarmCache` in parallel, `PromoteAndPublish` last. Any final failure runs `MarkPublishFailed` through the activity and returns the original error; nothing is compensated because nothing before `PromoteAndPublish` touches the live version. The failure reason recorded is bounded and fixed-vocabulary (`safeReason`), never raw error text.

## How a publish runs

1. `course.Service.Publish` (course-api) starts the workflow by name with `client.ExecuteWorkflow` on the workflow queue, passing `PublishWorkflowInput` (with `ActivityQueue` when the deployment overrides it).
2. `publishing-worker` picks it up and schedules each activity on the activity queue.
3. The course worker, which owns the data, polls the activity queue and runs the activities (registered by the contract names; they equal the method names of its `Activities` type).

## Wire format: do not rename

Workflow and activity names and the JSON field names of `PublishWorkflowInput` and `PublishOutline` are stored in Temporal history. Workflows already running replay against this code and against the activity worker. Renaming a workflow or activity name, a task queue default, or a JSON field, or reordering or adding a step, breaks workflows in flight (a nondeterminism error or an unknown activity). Changing the graph needs a versioning plan (`workflow.GetVersion` or a new workflow name) and a drain. Tests that pin this: `publish/replay_test.go` (history replay), the activity-name set test against `publishing.ActivityNames()`, the activity-queue test, and the contract's own tests.

## Process

`runtime.Main` with `WithWorkflows` and no `WithDependencies` (it links no pgx, GORM or go-redis) and `config.LoadWith(config.Requirements{})`: the binary requires no `POSTGRES_DSN`, `REDIS_ADDR`, MinIO or JWT variable, only the optional `TEMPORAL_*` and telemetry settings. It serves the internal listener (`METRICS_HTTP_ADDR`, default `:9100`): `/metrics`, `/health/live`, `/health/ready`. It retries until Temporal is reachable, building a fresh client every attempt (capped backoff, 1 s to 30 s); an outage never ends the process and shows as `degraded` (200) in `/health/ready` through the non-fatal `temporal` check.

## Tests

`go test -race ./...` in this module, no Docker: the workflow under Temporal's test environment with activities registered by name, the replayer over a built history, fault injection (`publish/faults_test.go`: flapping, permanent failure and lost-ack retries of every activity against its retry policy), the wiring through `runtime.Run`, and a guard (`independence_test.go`) that fails if this module requires or imports any other service.

Not verified here: the workflow against a live Temporal server together with the real activities (the container E2E scenario E01 runs a publish through both workers; the failure scenario E12 is still pending, `test/README.md`; the 2026-11-25 compose run in `docs/guides/local-stack.md` also published a course to `ready`), and the replay history is built by the test, not recorded from a server.

## Image and budget

`Dockerfile.publishing-worker`. `scripts/check-link-graph.sh` budgets the binary and forbids other services, Kafka, Mongo, Asynq and migration code. The Postgres driver still links through `libs/runtime` -> `libs/infra` (see the script's comment), though the binary never opens a connection.
