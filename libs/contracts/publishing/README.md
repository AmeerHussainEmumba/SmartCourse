# libs/contracts/publishing

The contract of the course publish workflow (`docs/plans/microservices-split.md` §5), plain Go with no service and no Temporal import (only `github.com/google/uuid`). The starter (`course`), the orchestrator (the workflow worker) and the activity worker agree on this module instead of on each other's code.

| Name | Value |
|---|---|
| `WorkflowName` | `PublishCourseWorkflow` |
| `WorkflowQueue` | `publish-workflows` (`TEMPORAL_TASK_QUEUE`) |
| `ActivityQueue` | `course-activities` (`TEMPORAL_ACTIVITY_QUEUE`) |
| `Activity*` | `ValidateMetadata`, `VerifyAssets`, `ComputeMetadata`, `BuildOutline`, `IndexSearch`, `WarmCache`, `PromoteAndPublish`, `MarkPublishFailed` (`ActivityNames()` lists them in graph order) |
| `FailureType` | `PublishFailure`: the `ApplicationError` type whose message the workflow trusts as a bounded reason |

DTOs: `PublishWorkflowInput` (the workflow's sole parameter; an optional `ActivityQueue` lets a starter that runs with a different `TEMPORAL_ACTIVITY_QUEUE` say so, empty means the constant, and `ActivityQueueOrDefault()` resolves it) and `PublishOutline` (BuildOutline's result and IndexSearch/WarmCache's input). Both moved here from `services/course/course`.

The values are wire format: Temporal stores workflow arguments and activity names in history, so a change breaks workflows already running. `publishing_test.go` pins them and the JSON compatibility of `PublishWorkflowInput` with inputs written before `ActivityQueue` existed.

The workflow (`services/course/workflows.PublishCourseWorkflow`) calls every activity by name on the activity queue; `TestPublishCourseWorkflow_CallsActivitiesByNameOnTheActivityQueue` runs it under Temporal's test environment with plain functions registered under these names, with no `Activities` type involved.
