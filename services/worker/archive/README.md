# services/worker/archive

The `audit-archiver` consumer (RFC §6.5, §6.8; ADR-0031; docs/plans/week-3.md Phase 4). It writes every event to the MongoDB `events` archive and writes an `audit_log` record for the audit-relevant types.

Status: built and tested against real MongoDB; **not registered with any transport yet**. Registration is the transport owner's wiring (see "Registering it").

## What it does

`Handlers(store, opts...)` returns one `relay.Handler` named `audit-archiver` for every event type in `libs/events` (`events.Types()`), so a new event type is archived without a change here.

| Collection | What is written | Key |
|---|---|---|
| `events` | event type, aggregate, `occurred_at`, trace id, the payload bytes as delivered, `schema_version`, `schema_id`, headers (`payload_encoding`, `schema_subject`, `schema_id_source`, span, actor, outbox id), and over Kafka the record's `topic`, `key`, `partition`, `offset` | `_id` = `event_id`; first write wins |
| `audit_log` | actor, action (= event type), resource type and id, the payload as `after`, time, trace id | one per source event (`event_id`, unique partial index) |

Audit-relevant types follow the RFC's event table (§4.6, "archive + audit") and the `audit_log` description (§5.4: role changes, publish and archive, enrollment overrides, certificate issuance, administrative deletion): `identity.user_role_changed`, `course.course_published`, `course.publish_failed`, `course.course_archived`, `progress.certificate_issued`. Every other catalogue type is "archive" only in the table. "Enrollment overrides" and "administrative deletion" have no event type in the catalogue, so nothing can be audited for them from events yet. `AuditTypes()` lists them; `archive_test.go` pins the set and has a test per type. A new sensitive type must be added to `auditSpec` in `archive.go`. The actor is the outbox row's `actor_id`, or `system` when the event had none.

**Interpretability (RFC §6.8).** Over Kafka the runner puts a `relay.Source` in the handler's context (`libs/relay`: topic, partition, offset, key and the registry schema id of the frame, which is the topic's envelope schema); the archiver stores that id as `schema_id` (header `schema_id_source` = `registry_frame`) and the topic, key, partition and offset as top-level fields (partition and offset 0 are stored). In-process there is no Schema Registry and no record, so `schema_id` is 0 (header `schema_id_source` = `none`) and no Kafka field is stored, unless a `SchemaResolver` is supplied (`WithSchemaResolver`, fallback only; header `resolver`). A parked archive event replayed by an admin runs without a Source, so a document first written by a replay has `schema_id` 0 and no coordinates (the dead-letter record does not keep the partition and offset). The archive records `schema_version` and the registry subject (`schema_subject`, the payload message's full name) instead, so the schema is resolvable by name and version. The payload is JSON today (`payload_encoding: json`); when the Kafka transport carries Protobuf the writer must decide whether to store those bytes or the decoded JSON, and set the header to match. **That decision is open.**

## Guarantees

- **Idempotent by `event_id`.** A duplicate, replayed or post-restart redelivery leaves one document and one audit record. Both writes are individually idempotent, so a crash between them, or before the `processed_events` marker, is repaired by the redelivery.
- **MongoDB down returns an error** from the handler (`ErrNoStore` with no store, `mongostore.ErrClosed` for a closed one, the driver error otherwise), within `OpTimeout`. A panic in the store is recovered into an error. It never panics and never drops.
- A payload over 15 MiB is refused with `mongostore.ErrPayloadTooLarge`; it will fail until the transport parks it.

## Registering it

**Register only when `EVENT_TRANSPORT=kafka`, on the Kafka consumer runner.** With the in-process relay a failing handler fails the whole outbox row (`relay.process` joins errors and `claimAndProcess` records the failure), so a MongoDB outage would burn attempts on every row and, at `maxAttempts`, the relay gives the row up for every consumer. In Kafka mode each consumer group has its own offset and retry budget, so the archiver stalls alone. The worker exposes the hook as `mongoWiring.ArchiveHandlers()` in `cmd/worker/mongo_wire.go`; it also belongs in the Replayer's handler list so a parked archive event can be replayed.

## Tests

`go test -race -tags=integration ./archive/` (Docker): every catalogue type archived with schema info; audit records only for the audited types, with the actor (and `archive_test.go`, no Docker: the audited set, one test per audited type, the Kafka source and the in-process case); redelivery after a restart leaves one document per event and does not overwrite the first write; with MongoDB stopped the archiver errors while another consumer's handler for the same event still commits; nil and panicking stores become errors.

## Not verified

- Behaviour under the Kafka runner (it does not exist in this branch).
- The in-process failure mode above is derived from reading `libs/relay` and shown only per handler (the course handler still commits while the archiver fails), not as a full-stall experiment.
- Archive volume and write throughput.
