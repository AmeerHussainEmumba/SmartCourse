# libs/outbox

The write side of the transactional outbox pattern (ADR-0006) — the only path from "something changed" to "something was announced." See `libs/relay/README.md` for the read side; the two packages together are the full pattern, but this half is deliberately small.

**Per service.** Each service that produces events owns its own `outbox` table in its own schema, written in the same transaction as its state change: `identity.outbox` (`migrations/identity/000002_outbox`) and `course.outbox` (`migrations/course/000008_outbox_inbox_idempotency`). The table name is unqualified, so the service's `search_path` selects it. Analytics, notification, publishing and audit produce no events and have no outbox. The shared `platform.outbox` is gone. **Used by** the identity module (identity-api) and the course, enrollment, progress and certificate modules (course-api, course-worker); the relay side runs in `identity-worker` and `course-worker`.

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

`Event.TraceID`, `SpanID` and `ActorID` are optional. `Write` fills them when the caller leaves them unset: `TraceID`/`SpanID` from the span active in `ctx` (`go.opentelemetry.io/otel/trace`, API only), `ActorID` from the installed `ActorResolver`. A value the caller sets always wins; with nothing in `ctx` the columns stay NULL, as before. `libs/infra` installs a resolver that reads the authenticated user from `libs/auth`; this module does not import `libs/auth` itself, because the relay links it and the auth module would add dependencies the relay does not need. A binary that does not link `libs/infra` therefore gets the trace fields but not the actor. The relay-side continuation of the trace (reading `trace_id`/`span_id` back and publishing under them) is a later step.

## Why this is intentionally the simple half

The relay (read side) is where the real complexity lives — polling, claiming work across possibly-many relay instances, a worker pool, retries, idempotency. This package carries none of that on purpose: a writer calling `outbox.Write` has exactly one job (announce a fact, inside the transaction that made it true) and nothing here should ever require a writer to think about delivery, retries, or who's listening. That separation is also why `Event` (what a writer provides) is a smaller shape than `Row` (what's actually stored) — `PublishedAt`, `Attempts`, and `LastError` are the relay's bookkeeping, not something a writer should ever need to set.

## Why `EventID` is a random UUID, not UUIDv7

Every other primary key in this codebase is UUIDv7 (ADR-0005) specifically for its time-ordering property. `EventID` here is the opposite, deliberately: it's a pure deduplication key for at-least-once delivery (RFC §6.4) — the relay and every consumer use it to recognize "I've already handled this one," never to order anything. Ordering comes from `outbox.id`, a plain auto-incrementing integer the relay polls by (`ORDER BY id`). Giving `EventID` time-ordering semantics it's never used for would just be a free variable nobody's allowed to rely on.

## Payload is JSON today, not final

`Event.Payload` is `any`, marshalled straight to JSON. ADR-0008 schedules a move to Protobuf with Schema Registry enforcement once there are genuinely independent external consumers who need a schema contract enforced at publish time (M3 — Kafka). Until then, the relay encodes the row as a Protobuf envelope when it publishes to Kafka (`libs/eventbus`, `libs/events`), so the stored JSON is the producer's own format, not the wire contract. Callers should not treat today's JSON shape as a wire contract that external tooling depends on.

## Pruning

`outbox.Prune(ctx, db, olderThan)` is the scheduled job's body (RFC §9.5; each service's worker runs it hourly through `libs/jobs.PruneOutbox` with `OUTBOX_RETENTION`, default 7 days: `identity-worker` and `course-worker`, the two services that own an outbox). It deletes only rows with `published_at` older than `olderThan`, a thousand at a time. It never touches an unpublished row, however old, and it keeps any row with a `last_error`: the relay marks a row it gave up on as published with the reason in `last_error`, and until a dead-letter store exists that row is the only record, so it is left for an operator. A row that failed and later succeeded keeps its `last_error` too, so it is also kept; both are rare. Zero or negative retention is an error rather than "delete everything".

## Testing

`Prune` has its own tests (`prune_test.go`, and `prune_integration_test.go` against a real Postgres through `libs/testenv`). `Write` has no dedicated test file — it is small enough, and simple enough, that it's exercised honestly rather than redundantly: every module that calls `outbox.Write` inside its own integration tests (`services/course/enrollment`, `services/course/course`, `services/identity/identity`, …) proves the write side works as part of proving its own transaction is correct, and `libs/relay`'s integration tests prove the read side can actually consume what gets written here. A bug in `Write` itself would surface as a failure in any of those, not go unnoticed. The trace/actor persistence is the exception: `libs/relay/relay_trace_actor_integration_test.go` (kept there, next to the relay it exercises) checks the round trip and that callers which set nothing get NULLs.
