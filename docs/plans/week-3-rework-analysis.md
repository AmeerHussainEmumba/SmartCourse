# Week 3: how much had to be redone after the microservices split

## Purpose

Record what the supervisor's microservices feedback cost the Week 3 build, so the next planning round can estimate it.

## Objectives

- Separate what survived the split unchanged from what was re-homed or rewritten.
- Name the causes, so they can be avoided.

## Scope

Engineering only (Week 3 build and the split). This is a draft analysis for the project owner to confirm.

## Method and caveat

The figures below are line counts taken from the branch history when the split landed (preview numbers, not re-measured after the later fixes). Treat them as approximate. Re-run `git diff --stat` against the pre-split Week 3 commit before quoting them externally.

## What was kept as it was

- `libs/*` (events, mongostore, asynqx, telemetry, relay core, outbox, idempotency, auth and the rest): about 7,500 lines of Week 3 library code stayed in place. They were already service-neutral.
- Protobuf schemas, the Schema Registry client and codec, golden tests.
- Observability stack, Mailpit, MinIO wiring, E2E scenario specs and the acceptance matrix (the specs were ported to the new harness, not rewritten).

## What was re-homed (same logic, new location)

- The single worker's production code, about 4,400 lines, moved into per-service binaries (identity, course, analytics, notification, publishing, audit workers). Handlers, activities and the publish workflow kept their logic; packages, wiring, configuration and Dockerfiles changed.
- The shared `platform` outbox and processed-events tables became per-service tables with per-service roles (`migrations/cutover`).

## What was rewritten

- The relay: from an in-process path to a publish-only relay per service, with the legacy path removed.
- Consumers: from one shared set to `eventbus.NewConsumers` (a group per consumer, retry topic, handler and processed-events marker in one transaction).
- The E2E harness: from a single-worker stack to the eleven-binary stack; E01-E14 were ported.
- CI: rewritten for per-module and per-service selection.

## Causes

1. The Week 3 plan assumed one worker and one shared outbox; the decision to split came after the build.
2. Libraries had been written service-neutral, which is why most of the code survived.
3. Cross-service coupling (shared tables, the worker importing service packages) was the expensive part to undo.

## Lessons

- Decide service boundaries before building the transport, not after.
- Keep libraries free of service knowledge; it paid for itself here.
- Per-service tables and roles are cheap to start with and costly to retrofit.

## Open items

Verify the figures, and have the project owner confirm the causes before this is shared outside the team.
