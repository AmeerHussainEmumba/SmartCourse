# Changelog — services/publishing

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Module created (Wave B5 of `docs/plans/microservices-split.md`): `publish.PublishCourseWorkflow` (copied from `services/course/workflows`, calling every activity by contract name on the activity queue), `publish.Register`, and `cmd/publishing-worker` (runtime.Main, no database, `config.LoadWith(config.Requirements{})`, Temporal start/retry loop, non-fatal Temporal readiness). `Dockerfile.publishing-worker` and a `publishing-worker` entry in `scripts/check-link-graph.sh`.
- Tests: workflow behaviour with activities registered by name, the scheduled activity set equals `publishing.ActivityNames()`, the activity queue is used (and the starter's override), history replay over a built history, wiring, and the guard that this module imports no other service.
- `services/course/workflows` and `services/worker` are untouched; the old workflow and worker are removed by their owners and Wave C.
