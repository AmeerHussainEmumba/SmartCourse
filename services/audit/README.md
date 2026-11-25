# services/audit

The audit service (`docs/plans/microservices-split.md` §2, §4.1; ADR-0031). One Go module, two binaries:

| Binary | Does | Port |
|---|---|---|
| `audit-worker` | the `audit-archiver` consumer: archives every event, writes `audit_log`, parks its own failures | no API; internal listener (`METRICS_HTTP_ADDR`, host 9116) |
| `audit-api` | admin failed-events surface: list, inspect, **replay by republish** | 8080 in the container, host 8093; internal listener host 9104 |

It owns the MongoDB store (`events` archive, `audit_log`, `failed_events`). It has **no Postgres tables**; Redis is used by `audit-api` only for the token-version freshness check.

## Why it is its own service

- It is the one consumer of **every** topic (and its retry topic), so it belongs to no domain service.
- It owns the Mongo store. Other services park their failures into it through `libs/mongostore`, and nothing else writes the archive.
- **Replay must not import other services' handlers.** The old worker's `Replayer` re-invoked consumers' handlers by name, which forced the replaying binary to link `course`, `analytics` and `notification`. Replay is now a republish to `retry.<consumer>`, so the audit service needs no handler and no other service's code.

**Alternative considered and rejected: each service owns its own dead letters** (a `failed_events` store and replay endpoint per service). It would give every service a Mongo dependency and an admin surface, scatter the operator's view across five APIs, and still need a shared "audit the replay" story. One owner with one view, fed through a shared library, is smaller. The price is one more service and the dependency of every parking service on the shared Mongo; both are accepted in the design.

## Replay is republish

`POST /api/v1/admin/failed-events/{event_id}/replay`, for every parked record of the event (one per consumer that parked it):

1. Load the record. Already `replayed`: a clean no-op, answered `200` with `replayed=false` and status `already_replayed`. Unknown event id: `404`.
2. Rebuild the event from what was stored when it was parked and **refuse** (`422 replay_refused`, nothing produced, record stays parked, audited) if it cannot be decoded or encoded for a retry: unknown or missing event type (this includes the archiver's stand-in for an undecodable Kafka record), no schema id or version, payload that does not match its schema, malformed ids, or a consumer that is not in `libs/kafka/topics.Consumers()` (nobody reads its retry topic, so producing and marking would lose the event silently). Finding B-01: a replay never applies garbage.
3. Encode with `eventbus.EncodeForRetry` and produce with `Publisher.PublishRetry` to `retry.<consumer>`; it returns after the broker's ack. A produce failure leaves the record parked and answers `503` with no detail.
4. **Only then** `MarkReplayed` (one conditional update) and append an `audit_log` entry (`failed_event_replayed`: the admin's user id from the verified token, event id, consumer, outcome). An audit write failure does not undo the replay: it is logged at error level and counted (`admin.AuditFailures()`).

The consuming service's own consumer group reads `retry.<consumer>` through its normal idempotent path (handler and `processed_events` in one transaction), in `libs/eventbus`.

### The ordering argument (produce, then mark)

The two writes are in different systems, so one can fail between them. Produce-then-mark makes every such failure harmless:

| Failure | State | Consequence |
|---|---|---|
| produce fails | parked, nothing sent | `503`, retry the call; nothing lost |
| crash or Mongo error after the ack, before the mark | parked although sent | a second replay produces again; `processed_events` absorbs the duplicate; nothing lost, nothing applied twice |
| (mark-then-produce, rejected) crash after the mark | `replayed` but never sent | **silent loss** |

Guarantee under N concurrent replays of one record: exactly one caller reports `replayed` (the mark is one conditional update), the others `already_replayed`; the retry record is produced at least once and at most N times (a racing caller may produce before it learns it lost the mark); the consumer applies the event once whatever the count; a call that starts after the mark produces nothing. Tests assert exactly this (`replay/replay_test.go`, `integration/`).

## Auth and errors

Admin role only, `RequireFreshPrivilegeStrict`: a Redis miss on the token-version key is `401 token_stale` (fail closed; DR-022), a Redis error is `503`. Errors are `{code, message, details}`. Router hardening as the other services (body cap, headers, request id, no CORS). `/metrics` is never on the API port. Public `/health/live` and `/health/ready` on the API port.

## Readiness (judgement, unverified against any orchestrator)

- `audit-api`: Redis fatal (without it no token can be checked); MongoDB and Kafka non-fatal (`degraded`, 200): the routes answer 503 while MongoDB is down, replay answers 503 while Kafka is down, nothing crashes.
- `audit-worker`: Kafka fatal (without it the service does nothing); MongoDB non-fatal (events wait in Kafka, the archiver retries then parks).
- MongoDB connects in the background with capped backoff (`store.Wiring`); neither process waits for it or exits because of it.

## Layout

`archive` (archiver handler, audit list), `deadletter` (park sink), `replay` (republish), `admin` (HTTP), `consume` (consumer runner), `store` (lazy Mongo), `auditconfig` (config stopgap), `cmd/audit-api`, `cmd/audit-worker`, `integration` (Kafka + Mongo tests).

## Open items and known gaps

- **`consume` replaces `eventbus.NewConsumers` for now.** `NewConsumers` requires a `*gorm.DB` and writes `processed_events` in Postgres, which this service does not have; its idempotency is the event id in Mongo. `consume` is the same loop without the transaction. A db-less mode in `libs/eventbus` would let it be deleted.
- **`auditconfig` is a stopgap.** `libs/config.Load` still requires `POSTGRES_*`, `MINIO_*` and `SCHEMA_REGISTRY_URL` and reads Kafka brokers only with `EVENT_TRANSPORT=kafka`; `auditconfig` fills inert placeholders and forces the transport. A per-service loader in `libs/config` replaces it.
- No `kafka_consumer_lag` gauge for the archiver and no Prometheus series for audit-write failures (a process counter and an error log only).
- Not verified: behaviour on a multi-broker cluster, MongoDB auth/TLS, a real Schema Registry (tests use the stand-in), orchestrator probe behaviour.

## Tests

`go test ./...` (replay and archive unit tests, no Docker). `go test -race -tags=integration ./...` (Docker: MongoDB, Kafka, Redis, and Postgres only for the downstream consumer group in `integration/`): admin HTTP matrix and adversarial reproducers, archiver end to end (every event type, source, idempotency, restart), the archiver's own parked failure replayed through `retry.audit-archiver`, undecodable refusal, replay applied once by a real consumer group, produce failure, concurrent replay, and the two binaries wired with an unreachable MongoDB.
