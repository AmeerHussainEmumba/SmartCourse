# internal/platform/outbox

The write side of the transactional outbox pattern (ADR-0006) — the only path from "something changed" to "something was announced." See `internal/platform/relay/README.md` for the read side; the two packages together are the full pattern, but this half is deliberately small.

## The shape

One function:

```go
outbox.Write(ctx, tx, outbox.Event{
    AggregateType: "course",
    AggregateID:   courseID,
    EventType:     "course.course_archived",
    SchemaVersion: 1,
    Payload:       map[string]any{"course_id": courseID},
})
```

`tx` is the caller's own transaction — not a new one this function opens. That's the entire mechanism: the event row and the state-change rows it's written alongside go through the same `COMMIT`, so they either both land or neither does. There's no window where a course gets archived and the fact that it happened fails to get recorded, and no separate system (a message broker, a second database) that could be reachable when Postgres is and vice versa.

## Why this is intentionally the simple half

The relay (read side) is where the real complexity lives — polling, claiming work across possibly-many relay instances, a worker pool, retries, idempotency. This package carries none of that on purpose: a writer calling `outbox.Write` has exactly one job (announce a fact, inside the transaction that made it true) and nothing here should ever require a writer to think about delivery, retries, or who's listening. That separation is also why `Event` (what a writer provides) is a smaller shape than `Row` (what's actually stored) — `PublishedAt`, `Attempts`, and `LastError` are the relay's bookkeeping, not something a writer should ever need to set.

## Why `EventID` is a random UUID, not UUIDv7

Every other primary key in this codebase is UUIDv7 (ADR-0005) specifically for its time-ordering property. `EventID` here is the opposite, deliberately: it's a pure deduplication key for at-least-once delivery (RFC §6.4) — the relay and every consumer use it to recognize "I've already handled this one," never to order anything. Ordering comes from `outbox.id`, a plain auto-incrementing integer the relay polls by (`ORDER BY id`). Giving `EventID` time-ordering semantics it's never used for would just be a free variable nobody's allowed to rely on.

## Payload is JSON today, not final

`Event.Payload` is `any`, marshalled straight to JSON. ADR-0008 schedules a move to Protobuf with Schema Registry enforcement once there are genuinely independent external consumers who need a schema contract enforced at publish time (M3 — Kafka). Until then, JSON is the simplest thing that works for an in-process relay (ADR-0021) with handlers that are themselves Go functions in this same binary. Callers should not treat today's JSON shape as a wire contract that external tooling depends on.

## Testing

No dedicated test file — this package is small enough, and simple enough, that it's exercised honestly rather than redundantly: every module that calls `outbox.Write` inside its own integration tests (`internal/enrollment`, `internal/course`, `internal/identity`, …) proves the write side works as part of proving its own transaction is correct, and `internal/platform/relay`'s integration tests prove the read side can actually consume what gets written here. A bug in `Write` itself would surface as a failure in any of those, not go unnoticed.
