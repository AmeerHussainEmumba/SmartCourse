# Changelog — libs/contracts/publishing

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- README: the workflow and its test now live in `services/publishing/publish` (not `services/course/workflows`); consumers listed. No code change.

- Module created (microservices split, Wave A2): the workflow name, the two task queues, the activity names, `FailureType`, `PublishWorkflowInput` and `PublishOutline`, moved out of `services/course/course`. `PublishWorkflowInput` gained the optional `ActivityQueue` field (JSON-compatible with inputs written without it).
