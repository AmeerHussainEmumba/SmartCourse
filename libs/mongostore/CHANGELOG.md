# Changelog — libs/mongostore

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- README: current users after the split (audit-worker, audit-api, and the consuming services' dead-letter sinks) and replay-by-republish; the stale `services/worker` references are removed. No code change.

- `Event` gains optional `Topic`, `Key`, `Partition *int32`, `Offset *int64` (the Kafka record an archived event arrived in; stored by `ArchiveEvent` only when set, so partition and offset 0 are kept). Additive. `AppendAudit` (no `event_id`, never deduplicated) is the call for per-attempt administrative records such as a failed-event replay; tests added.

- `ParkInput.History []string`: one message per attempt; when set, each becomes its own error-history entry (numbered 1..n within the parking) instead of a single entry for `Err`. Additive.
- Module created (Week 3 plan, ADR-0026).
- Added `Open`/`Ping`/`Close` with context-bounded timeouts and idempotent index creation (no TTL).
- Added the `events` archive (`ArchiveEvent`, `GetEvent`; idempotent, stores the schema id), `audit_log` (`AppendAudit`) and `failed_events` (`ParkFailedEvent`, `GetFailedEvent`, `ListFailedEvents` with keyset pagination, `MarkReplayed`, `CountFailedSince`).
- Depends on `go.mongodb.org/mongo-driver/v2` v2.10.0 (Apache-2.0).
- Added (Phase 4): `ParkInput`/`FailedEvent` carry replay metadata kept from the first parking (`OutboxID`, `SchemaID`, `SchemaVersion`, `OccurredAt`, `TraceID`, `SpanID`, `ActorID`), so a replayer can rebuild the event. `AppendAuditOnce` (idempotent per `AuditEntry.EventID`, backed by a unique partial index on `audit_log.event_id`; `AppendAudit` is unchanged). `SummarizeFailed` (counts by status, consumer and event type, oldest parked) for the Failed Events metric. Additive; documents parked before this have none of the metadata.
