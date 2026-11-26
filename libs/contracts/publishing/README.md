# libs/contracts/publishing

The contract of the course publish workflow (`docs/plans/microservices-split.md` §5), plain Go with no service and no Temporal import (only `github.com/google/uuid`). The starter (`services/course/course`, run by `course-api`), the orchestrator (`services/publishing/publish`, run by `publishing-worker`) and the activity worker (`services/course/activities`, run by `course-worker`) agree on this module instead of on each other's code.

| Name | Value |
|---|---|
| `WorkflowName` | `PublishCourseWorkflow` |
| `WorkflowQueue` | `publish-workflows` (`TEMPORAL_TASK_QUEUE`) |
| `ActivityQueue` | `course-activities` (`TEMPORAL_ACTIVITY_QUEUE`) |
| `Activity*` | `ValidateMetadata`, `VerifyAssets`, `ComputeMetadata`, `BuildOutline`, `IndexSearch`, `WarmCache`, `PromoteAndPublish`, `MarkPublishFailed` (`ActivityNames()` lists them in graph order) |
| `FailureType` | `PublishFailure`: the `ApplicationError` type whose message the workflow trusts as a bounded reason |

DTOs: `PublishWorkflowInput` (the workflow's sole parameter; an optional `ActivityQueue` lets a starter that runs with a different `TEMPORAL_ACTIVITY_QUEUE` say so, empty means the constant, and `ActivityQueueOrDefault()` resolves it) and `PublishOutline` (BuildOutline's result and IndexSearch/WarmCache's input). Both moved here from `services/course/course`.

The values are wire format: Temporal stores workflow arguments and activity names in history, so a change breaks workflows already running. `publishing_test.go` pins them and the JSON compatibility of `PublishWorkflowInput` with inputs written before `ActivityQueue` existed.

**Used by** `services/course` (the `course` package and `activities`; binary `course-worker`) and `services/publishing` (`publish`; binary `publishing-worker`). The workflow (`services/publishing/publish.PublishCourseWorkflow`) calls every activity by name on the activity queue. `TestPublishCourseWorkflow_ScheduledActivitiesEqualTheContract` (`services/publishing/publish`) runs it under Temporal's test environment and proves the scheduled set equals `ActivityNames()`; that test does not assert the queue itself (the workflow sets it from the input in `workflow.go`).
