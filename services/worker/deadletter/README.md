# services/worker/deadletter

Dead-letter parking and replay (RFC §6.9; ADR-0031; docs/plans/week-3.md Phase 4): the write side `Sink` and the read side `Replayer`, both over `libs/mongostore`'s `failed_events`.

Status: built and tested against real MongoDB, Postgres and Redis; the **`Sink` is not yet called by any transport** (the relay still marks a poison event published after `maxAttempts`; closing that gap needs `libs/relay`'s dead-letter hook, which another change adds). The `Replayer` is wired into the worker's admin surface.

## Sink

`Sink.Park(ctx, Parked) error` records that a consumer gave up on an event: payload, consumer, attempts, the error, and what a replayer needs to rebuild the event (outbox id, schema id and version, occurrence time, trace, actor).

- **Idempotent per `(event_id, consumer)`**: one document; parking again appends to its error history (newest 100 kept) and adds to `attempts`. Safe under concurrency.
- **Cannot block the stream**: each call is bounded (`WithParkTimeout`, default 5 s, plus the store's own operation timeout) and returns an error instead of hanging or panicking when MongoDB is unreachable.
- **An error means "not parked."** The caller must then keep the event retryable and must not advance past it. What a transport does when it cannot park (retry the park, hold the event, alert) is its decision and is not made here; this is ADR-0031's open item.
- The call is detached from the caller's cancellation so a process shutting down still records what it was parking. Messages are cut to 2000 bytes and made valid UTF-8, like the relay's `last_error`.

## Replayer

`Replayer.Replay(ctx, eventID) (*Result, error)` replays every record parked for the event (one per consumer).

1. Finds the records; none is `ErrNotFound`, a malformed id is `ErrInvalidID`.
2. For each parked record, finds the consumer's registered handler for the event type. The registry is injected (`NewReplayer(db, store, handlers)`): the same `relay.Handler` values the transport runs, several may share one consumer name.
3. Re-applies through the normal idempotent path: one transaction that takes an advisory lock on `(consumer, event)`, skips if the `processed_events` marker exists, otherwise runs the handler and writes the marker.
4. Calls `MarkReplayed`, one conditional update, so exactly one of any number of concurrent replays reports the transition.

Outcomes per consumer: `replayed` (this call moved the record; `Applied` says whether the handler actually ran or the consumer had already processed the event), `already_replayed` (no-op, nothing applied), `failed` (see below).

- **A handler failing during replay** leaves the record parked with the new error appended to its history, no marker written, and the outcome `failed`. A handler panic is the same. A consumer with no registered handler is `failed` without touching the record.
- **Crash after the handler committed, before the mark**: the record stays parked; the next replay finds the marker, skips the handler and marks it.
- The advisory lock matters: without it two concurrent replays both run the handler and one loses on the marker's primary key (this was reproduced by removing the lock; see Tests).

## Decisions and limits

- Replay order is apply-then-mark (ADR-0031 left it open): the risk is a duplicate apply after a crash, which the processed-events marker absorbs, rather than a lost replay.
- A replayed event can repeat an external effect that is not idempotent in Postgres (an email, RFC §6.6). Notification consumers dedupe on their own delivery rows; anything else that sends should be checked before replay.
- The rebuilt event carries `OutboxID`, so handlers that order updates by it (the instructor and role mirrors) behave as they did originally. Events parked before the replay metadata existed have none and cannot be replayed reliably.
- `Attempts` on the rebuilt event is 0.

## Tests

`go test -race -tags=integration ./deadletter/`. Mutation-checked: removing the advisory lock and marker check fails the concurrent-replay test (handler ran 8 times, 7 callers failed on the marker key); forcing the transition to true fails it too (8 callers report the transition); making each `Park` use a fresh consumer fails both park-idempotency tests (3 and 12 documents).

## Not verified

- Replay under Kafka (no runner here); replica sets and MongoDB authentication/TLS (the store's own gaps).
- Replay of Protobuf payloads: the payload is stored and handed back as delivered.
