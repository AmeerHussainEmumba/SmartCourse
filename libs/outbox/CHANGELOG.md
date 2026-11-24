# Changelog — libs/outbox

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added `Prune(ctx, db, olderThan)` (new file `prune.go`): deletes published rows whose `published_at` is older than `olderThan`, in batches, and returns the count. It never deletes an unpublished row, and keeps rows with a `last_error` (the relay's give-ups, which have no other record until a dead-letter store exists). A non-positive `olderThan` is an error. The module now has `libs/testenv` as a test dependency (integration tests for `Prune`).

- Added: `Event.TraceID`, `Event.SpanID` (W3C hex strings) and `Event.ActorID` (`*uuid.UUID`), persisted to the new nullable `outbox.trace_id`/`span_id`/`actor_id` columns (migration `platform/000003`; RFC §6.4, §12.2). Empty/nil is stored as NULL, so existing callers compile and behave unchanged. `Row` gained matching pointer fields.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
