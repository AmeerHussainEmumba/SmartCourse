# services/course

The course service: courses and their publish lifecycle, enrollment, progress and certificates. One Go module, two binaries, one Postgres schema.

| Binary | What it is |
|---|---|
| `cmd/course-api` | The public HTTP API: `/courses`, `/enrollments`, `/progress`, `/certificates`, `POST /assets/upload-url`. Starts the publish workflow; never runs it. |
| `cmd/course-worker` | The background half: the publish-only relay over `course.outbox`, the Kafka consumers, the scheduled jobs, the Temporal activities worker. |

Both boot through `libs/runtime` and serve `/metrics`, `/health/live` and `/health/ready` on the INTERNAL listener (`METRICS_HTTP_ADDR`, default `:9100`). `course-api` additionally serves its public health routes on the API port, and never `/metrics`.

The design is `docs/plans/microservices-split.md`; this README is the service's own summary and states what the code does today, not what an operator has verified.

## Data and independence

Both binaries connect as the Postgres role `svc_course` (`POSTGRES_DSN`, `POSTGRES_SEARCH_PATH=course,public`). The service owns `course.*`: its domain tables, `course.outbox`, `course.processed_events` and `course.idempotency_keys`. `libs/outbox`, `libs/relay`, `libs/idempotency` and `libs/eventbus` use unqualified table names, so that search path is what selects the course schema's own tables. The schema has no foreign key into another schema: the instructor's display name comes from `course.instructor_mirror`, fed by identity's events. The service imports no other service, only `libs/*` (`scripts/check-module-graph.sh`).

## Events

| Direction | Event | Where |
|---|---|---|
| produces | `course.*` (`course_published`, `course_archived`, `publish_failed`, ...), `enrollment.*`, `progress.*` (`lesson_completed`, `course_completed`, `certificate_issued`) | written to `course.outbox` in the same transaction as the state change; published by the worker's relay |
| consumes | `identity.user_registered`, `identity.user_profile_updated` | consumer `course-instructor-mirror`: keeps `instructor_mirror` current |
| consumes | `course.course_archived` | consumer `enrollment-course-archival`: withdraws the active enrollments of an archived course |
| consumes | `progress.course_completed` | consumer `certificate-issuer`: issues one certificate per completed enrollment |

Each consumer name is its own Kafka consumer group and also reads `retry.<consumer-name>` (replay is republish, `docs/plans/microservices-split.md` §4.1). The handler and its `course.processed_events` marker commit in one transaction before the offset is committed.

## Temporal

`course-api` starts `PublishCourseWorkflow` by name on `TEMPORAL_TASK_QUEUE` (the workflow queue, default `publish-workflows`) and passes `TEMPORAL_ACTIVITY_QUEUE` (default `course-activities`) in the workflow input. `course-worker` runs the activities on the activity queue, registered under the names in `libs/contracts/publishing`. The workflow runs in `services/publishing` (`activities/README.md`).

## Jobs (course-worker, Asynq on the low queue)

| Job | Interval config |
|---|---|
| stale-publish reconciliation (ADR-0028) | `JOBS_RECONCILE_PUBLISHES_INTERVAL` (1 m) |
| enrollment-count reconciliation | `JOBS_ENROLLMENT_COUNT_INTERVAL` (1 h) |
| idempotency-key pruning | `JOBS_IDEMPOTENCY_PRUNE_INTERVAL` (1 h) |
| outbox pruning (published rows only) | `JOBS_OUTBOX_PRUNE_INTERVAL` (1 h), `OUTBOX_RETENTION` (7 d) |
| `processed_events` pruning | the outbox interval; `PROCESSED_EVENTS_RETENTION` (default 336h = 14 d) |

`PROCESSED_EVENTS_RETENTION` comes from `libs/config` (`cfg.Jobs.ProcessedEventsRetention`; zero, negative or unparsable stops startup). Keep it longer than the Kafka topic retention and than the longest wait before a parked event is replayed, or a late duplicate is applied twice.

## Packages

`course`, `enrollment`, `progress`, `certificate` (domain modules, each with a README), `activities` (the Temporal activities), `assets` (S3/MinIO store for upload URLs and `VerifyAssets`), `internal/miniotest` (MinIO test container), `securitytest` and `schemaisolation` (cross-cutting test suites).

## Run

```
go run ./services/course/cmd/course-api
go run ./services/course/cmd/course-worker
docker build -f Dockerfile.course-api .
docker build -f Dockerfile.course-worker .
```

Config is `libs/config` (see `env.example`). The worker needs Postgres, Redis, Kafka and the Schema Registry at start-up (it exits with a message naming the missing piece), and optionally MongoDB (`MONGO_URI`, dead-letter parking), MinIO (`VerifyAssets`) and Temporal (the activities worker retries until it is up).

## Tests

`go test ./...` (unit) and `go test -race -tags=integration -count=1 ./...` (Docker: Postgres, Redis, Kafka, MongoDB, MinIO, all through `libs/testenv`). Service tests run as the real role: `env.DB = env.ServiceDB(t, "course")`, so a test that touched another schema, or an unqualified table of another service, would fail. The cross-service paths (events reaching notification, one trace across the pipeline, publish through a real Temporal) are container E2E tests, not in this module.
