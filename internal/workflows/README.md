# internal/workflows

SmartCourse's one Temporal workflow — course publishing (RFC §7, ADR-0009, ADR-0025). Not a domain module and not shaped like one (no `handler.go`/`dto.go`/`domain.go`): it's an orchestration layer over `internal/course`'s own exported, Temporal-agnostic `Service` methods, the same way `internal/platform/bootstrap` is an orchestration layer over every domain module's wiring.

## What's here

- **`workflow.go`** — `PublishCourseWorkflow`, implementing RFC §7.4's activity graph: `ValidateMetadata` → `{VerifyAssets, ComputeMetadata}` → `BuildOutline` → `{IndexSearch, WarmCache}` → `PromoteAndPublish`. On any activity's final failure, `failPublish` records the error via `MarkPublishFailed` and returns it — no compensation of earlier activities, because none of them touch the live version (RFC §7.4's own stated design property).
- **`activities.go`** — `Activities`, a thin wrapper whose methods do nothing but forward to `course.Service`. The actual business logic lives on `Service` so it's identical whether Temporal calls it or a test calls it directly, and so this package carries the only Temporal SDK import in the codebase — `internal/course` never imports it.

## Why the dependency only runs one way

`internal/workflows` imports `internal/course`; `course` must never import `workflows` back (RFC §4.4's module-boundary discipline, extended to this package the same way `bootstrap` is the one exception allowed to see every domain module). `course.Service.Publish` starts this workflow by its registered name (`course.PublishWorkflowName`, a shared string constant both packages reference), not by importing the workflow function — see that constant's doc comment in `internal/course/domain.go` for the full reasoning.

## Testing

Two tiers, deliberately separate (`docs/guides/testing.md` has the full index):

- **`publish_workflow_test.go`** (no build tag, fast, no infra) — Temporal's own `testsuite.WorkflowTestSuite` with every activity mocked via `env.OnActivity(...)`. Tests pure orchestration: call order, that `VerifyAssets`/`ComputeMetadata` genuinely run concurrently, that a failure stops the graph before `PromoteAndPublish` and still calls `MarkPublishFailed`.
- **`publish_workflow_integration_test.go`** (`//go:build integration`, real Postgres/Redis) — the same `testsuite.TestWorkflowEnvironment`, but with real `Activities` wired to a real `course.Service`. No live Temporal server is started; the SDK's own in-process test harness runs the real activity code without one. Proves the activities' actual effects: the version reaches `ready`, `course_search_index` gets a real row, the Redis cache key holds the *post*-promotion course state (regression test for a bug found live — see ADR-0025), and a real failure (`ValidateMetadata` against a draft with no modules) writes `publish_failed` with a clean error message to `course_versions`, not Temporal's wrapped diagnostic text.

A full client → server → worker round trip (the one thing neither tier exercises) is verified manually — `make dev-workflow && make seed`, which fails loudly if that wiring is broken. See ADR-0025 for why a live Temporal server in CI was judged not worth the cost.
