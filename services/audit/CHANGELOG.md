# Changelog — services/audit

Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- New module (microservices split, Wave B6; `docs/plans/microservices-split.md` §2, §4.1). Lifted from `services/worker`: `archive` (archiver handler and audit list), `deadletter` (park sink), `admin` (HTTP). `services/worker` is untouched until Wave C removes it.
- `audit-worker`: the `audit-archiver` consumer on every event topic and its retry topic (`consume`, a database-less copy of the `libs/eventbus` consumer loop; see the README for why). The park path now keeps the record's topic and frame schema id so a parked archive failure can be replayed.
- `audit-api`: failed-events list, detail (new optional `consumer` filter) and replay on port 8080 (host 8093), fail-closed admin auth, `/metrics` only on the internal listener.
- **Replay is republish** (new `replay` package replacing the worker's handler-invoking `Replayer`): refuse an undecodable record or unknown consumer (422 `replay_refused`), `EncodeForRetry` and `PublishRetry` to `retry.<consumer>`, mark replayed only after the broker's ack, audit every attempt with the admin's user id; Kafka or Mongo failure is 503 with no detail. The `applied` field and the 422 `replay_failed` response of the old API are gone (no handler runs here); outcomes carry `retry_topic` and a new `refused` status appears in 422 details.
- `auditconfig`: stopgap config loader until `libs/config` has a per-service one.
