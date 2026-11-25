# libs/mongostore

MongoDB archive, audit log and failed-event parking (ADR-0031, ADR-0004, RFC §5.4, §6.9).

Status: implemented for Week 3 (`docs/plans/week-3.md`, section 4a). ADR-0031 was accepted by the project owner on 2026-11-26. Used by the audit service (`audit-worker`: the archiver; `audit-api`: failed-event list, get and replay; packages `services/audit/{archive,deadletter,replay,admin,store}`, ADR-0034), as the dead-letter sink of the services that consume events (`course-worker`, `notification-worker`, `analytics-worker`) and by `analytics-api` (failed-events metric). Mongo is the audit service's own store; the other services only park into `failed_events`. See ADR-0031's implementation notes and ADR-0033/0034. Phase 4 additions: replay metadata on parked events, `AppendAuditOnce`, `SummarizeFailed`.

Driver: `go.mongodb.org/mongo-driver/v2` v2.10.0 (Apache-2.0, pure Go, no cgo). Tests run against `mongo:7` through `libs/testenv.SetupMongo`.

## What is here

| API | Purpose |
|---|---|
| `Open(ctx, Config)` / `Ping` / `Close(ctx)` | Connect, verify, create indexes; health check; shutdown bounded by the caller's context |
| `ArchiveEvent`, `GetEvent` | `events`: permanent archive, keyed by `event_id` (stored as `_id`), with `schema_id` so old events stay interpretable (RFC §6.8) |
| `AppendAudit` | `audit_log`: append-only; there is no update or delete in this package |
| `ParkFailedEvent` | `failed_events`: give up on an event without blocking the stream |
| `ListFailedEvents`, `GetFailedEvent` | Operator views; the list omits payloads, `GetFailedEvent` returns them |
| `MarkReplayed` | `parked -> replayed`, exactly once |
| `CountFailedSince` | Feeds the "Failed Events / Workflow Issues" metric (RFC §9.1) |

## Semantics

- **Archive is idempotent, first write wins.** `ArchiveEvent` returns `inserted=false` for an id that exists and does not overwrite it. A redelivered event is harmless. The archive has no TTL index: it is permanent.
- **Parking is idempotent per `(event_id, consumer)`**, enforced by a unique index plus an upsert. Repeated or concurrent parking appends to `errors` and adds to `attempts`; it never creates a second document. The error history keeps the newest `MaxErrorHistory` (100) entries; `attempts` stays the true total. The payload comes from the first parking.
- **`MarkReplayed` is one conditional update** (`status = parked`). Of any number of racing callers exactly one gets `true`; later callers get `false, nil`; an unknown event is `ErrNotFound`. If a replayed event fails again, `ParkFailedEvent` moves it back to `parked` and clears `replayed_at`, so it can be replayed again.
- **Replay itself is not done here.** `audit-api` replays by republishing to `retry.<consumer>` through `libs/eventbus` (produce first), then calls `MarkReplayed`: a crash in between leaves the record parked, and a second replay is absorbed by the consumer's `processed_events` (ADR-0034).
- **Pagination is a keyset on the immutable document id**, newest created first. Pages do not skip or repeat items when documents are parked, replayed or re-parked during the walk. Events created after the walk began are newer than the cursor and are not visited.
- **Limits.** Payloads over `MaxPayloadBytes` (15 MiB) fail with `ErrPayloadTooLarge` before any I/O (MongoDB's document limit is 16 MiB). Page size defaults to 50, max 500.
- **Indexes** are created by `Open` on every start (idempotent): RFC §5.4's indexes, a unique `(event_id, consumer)` on `failed_events`, and `(status, _id)` / `(status, last_failed_at)` for listing and counting. Unlike RFC §5.4's `resolved_at`, the implementation uses `status` and `replayed_at` (per the Week 3 brief). Verify with the RFC owner.
- `Config.URI` may hold credentials: never log it. The package does not.

## What a MongoDB outage means for callers

Every operation takes a context and also applies `Config.OpTimeout` (default 5 s), and server selection gives up after `ServerSelectionTimeout` (default 3 s). With MongoDB down, calls return an error within those bounds, wrapped with `mongostore:` context; they do not hang and do not panic. A `nil` or closed `Store` returns `ErrClosed`.

Per ADR-0004 and ADR-0031, MongoDB is off every synchronous user path, so callers must treat these errors as degradation, not failure of the request:

- **Archiver / audit writers**: retry with backoff or leave the event unacknowledged so it is redelivered; `ArchiveEvent` is safe to repeat.
- **Consumers that cannot park**: do not drop the event. Keep it retryable (do not advance past it) or surface it loudly; parking is the only record that it was given up on. Design the consumer runner's behaviour for "park failed" explicitly (open question for the Phase 3 owner).
- **Admin failed-events view and the failed-events metric**: show unavailable or stale, not zero.
- **Readiness**: use `Ping` for a readiness probe if the service should report degraded; do not make API readiness depend on it unless the owner decides so.

## Not verified

- Behaviour on a replica set or sharded cluster (tests use a single standalone container; no multi-document transactions are used).
- Behaviour under MongoDB authentication and TLS (tests use an unauthenticated container).
- Throughput; no benchmarks were run.
