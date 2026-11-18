# libs/outbox

The write side of the transactional outbox pattern (ADR-0006) — the only path from "something changed" to "something was announced." See `libs/relay/README.md` for the read side; the two packages together are the full pattern, but this half is deliberately small.

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

## Trace and actor fields

`Event` also takes optional `TraceID` and `SpanID` (W3C trace-context hex, 32 and 16 chars) and `ActorID` (`*uuid.UUID`, the authenticated user who caused the event), per RFC §6.4 and §12.2. They are stored in nullable columns added by `migrations/platform/000003_outbox_trace_actor`; leave them empty/nil and the row gets NULLs, exactly as before. This package neither generates nor validates them (no OpenTelemetry dependency here; `libs/telemetry` supplies the values), and the relay passes them through on `relay.DecodedEvent`.

## Why this is intentionally the simple half

The relay (read side) is where the real complexity lives — polling, claiming work across possibly-many relay instances, a worker pool, retries, idempotency. This package carries none of that on purpose: a writer calling `outbox.Write` has exactly one job (announce a fact, inside the transaction that made it true) and nothing here should ever require a writer to think about delivery, retries, or who's listening. That separation is also why `Event` (what a writer provides) is a smaller shape than `Row` (what's actually stored) — `PublishedAt`, `Attempts`, and `LastError` are the relay's bookkeeping, not something a writer should ever need to set.

## Why `EventID` is a random UUID, not UUIDv7

Every other primary key in this codebase is UUIDv7 (ADR-0005) specifically for its time-ordering property. `EventID` here is the opposite, deliberately: it's a pure deduplication key for at-least-once delivery (RFC §6.4) — the relay and every consumer use it to recognize "I've already handled this one," never to order anything. Ordering comes from `outbox.id`, a plain auto-incrementing integer the relay polls by (`ORDER BY id`). Giving `EventID` time-ordering semantics it's never used for would just be a free variable nobody's allowed to rely on.

## Payload is JSON today, not final

`Event.Payload` is `any`, marshalled straight to JSON. ADR-0008 schedules a move to Protobuf with Schema Registry enforcement once there are genuinely independent external consumers who need a schema contract enforced at publish time (M3 — Kafka). Until then, JSON is the simplest thing that works for an in-process relay (ADR-0021) with handlers that are themselves Go functions in this same binary. Callers should not treat today's JSON shape as a wire contract that external tooling depends on.

## Testing

No dedicated test file — this package is small enough, and simple enough, that it's exercised honestly rather than redundantly: every module that calls `outbox.Write` inside its own integration tests (`services/course/enrollment`, `services/course/course`, `services/identity/identity`, …) proves the write side works as part of proving its own transaction is correct, and `libs/relay`'s integration tests prove the read side can actually consume what gets written here. A bug in `Write` itself would surface as a failure in any of those, not go unnoticed. The trace/actor persistence is the exception: `libs/relay/relay_trace_actor_integration_test.go` (kept there so this module needs no database test dependencies) checks the round trip and that callers which set nothing get NULLs.
