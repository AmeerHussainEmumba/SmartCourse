# services/course/activities

The Temporal activities of the course publish workflow (RFC §7, ADR-0009, ADR-0025). Not a domain module (no `handler.go`/`dto.go`/`domain.go`): `Activities` is a thin wrapper whose methods do nothing but forward to the Temporal-agnostic `services/course/course` `Service`, so the business logic is identical whether Temporal calls it or a test does, and so this package carries the only Temporal SDK import in the service module.

The package was `services/course/workflows` until the microservice split. The workflow itself, `PublishCourseWorkflow` (the activity graph, the retry policies, `failPublish`), moved to `services/publishing`, which has no database and calls every activity BY NAME on the activity queue (`docs/plans/microservices-split.md` §5). The names, the queues and the DTOs are the contract in `libs/contracts/publishing`.

## What's here

- **`activities.go`**: `Activities` with one method per node of RFC §7.4's graph: `ValidateMetadata`, `VerifyAssets`, `ComputeMetadata`, `BuildOutline`, `IndexSearch`, `WarmCache`, `PromoteAndPublish`, `MarkPublishFailed`. Each failure is a `temporal.ApplicationError` of type `course.PublishFailureType` whose message is a bounded, fixed-vocabulary reason (`course.PublishFailureText`), never raw error text (S-08); the full error goes to the server log. `MarkPublishFailed` takes the failed activity's name as its last argument (becomes `failed_activity` on `course.publish_failed`).

`cmd/course-worker` registers the methods on the activity queue under the contract's names, not the Go method names (`registerActivities`, pinned by a test), with the asset store attached (without one `VerifyAssets` fails closed for lessons with assets).

## Why the dependency only runs one way

`activities` imports `course`; `course` never imports `activities` (RFC §4.4's module boundary). `course.Service.Publish` starts the workflow by its contract name, not by importing a workflow function.

## Testing

- **`activities_integration_test.go`** (`//go:build integration`): the real activities against real Postgres and Redis, called directly in the order of the graph: a draft reaches `ready`, gets a `course_search_index` row, is readable through the service and writes exactly one `course.course_published` row in `course.outbox`; a retried `PromoteAndPublish` emits nothing more; a failing `ValidateMetadata` is a bounded `PublishFailure` that `MarkPublishFailed` records (`publish_failed`, one `course.publish_failed` event).
- **`publish_assets_integration_test.go`**: the real `VerifyAssets` against a real MinIO: a draft referencing an asset that was never uploaded fails cleanly (live version and `course_published` events untouched), and the same version publishes once the asset is uploaded.

What is NOT tested here: the workflow's orchestration (call order, concurrency of `VerifyAssets` and `ComputeMetadata`, retry policy, the failure step) now belongs to `services/publishing`'s tests, and the combined workflow+activities path over a real Temporal server is proven by the container E2E tests (E01 publish is ported; E12 failure is a pending stub, `test/README.md`), not in this module. A full client to server to worker round trip is also what `make dev-workflow && make seed` exercises.
