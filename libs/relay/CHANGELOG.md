# Changelog — libs/relay

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- README rewritten for the per-service topology (ADR-0033): every relay is publish-only (`identity-worker`, `course-worker`); the in-process path (a relay with no `Publisher`) is documented as a legacy, unused code path kept only in this library and its tests, pending an owner decision on removal. Package comment updated; `EVENT_TRANSPORT` no longer exists. No behaviour change.

- Tests: publish-only operation is a supported mode. `relay.New(db, relay.WithPublisher(p))` with no handler registered publishes every row in per-aggregate order, marks it published after the acknowledgement, never calls the handler hook or writes `processed_events`, and parks a poison row as the relay's own dead letter (`RelayConsumer`). No code change was needed. `Handlers()` is now only for in-process mode and tests; a service builds its Kafka consumers from its own handlers with `eventbus.NewConsumers`.

- Added `Source`, `WithSource` and `SourceFrom`: the Kafka coordinates (topic, partition, offset, key) and registry schema id of the record an event arrived in, carried in the handler's context by a broker transport. In-process delivery sets none. Additive; imports only `context`.

- `DeadLetter` gains an optional `History []error` (the error of every attempt, in order). Additive; the Kafka consumer fills it.
- Added the observability seams (ADR-0032): `WithObserver(Observer)` (called per delivery attempt of a claimed row, `ModePublish` or `ModeProcess`; returns the context the Publisher or handlers run under and a finish callback with the outcome) and `WithHandlerHook(HandlerHook)` (per in-process handler: processed, duplicate or failed). The package imports no tracing library; the worker implements the observer with `libs/telemetry` (`services/worker/transport.RelayObserver`). A nil, or panicking, observer or hook never fails a delivery.
- Added `DeliverEvent`, `Deliver` that also reports whether the consumer had already processed the event (the `duplicate` outcome of `event_processed_total`); `Deliver` is unchanged for callers.
- The relay's log lines about a row carry `event_id` (and `trace_id`/`span_id` when the observer started a span), through `libs/logging`. New dependency: `libs/logging` (which imports only the OpenTelemetry trace API, already linked by every binary that links this module).

- Added publish mode (ADR-0029): `Publisher`, `WithPublisher`, `WithPublishBackoff`, `Transient` / `IsTransient`, `Relay.Handlers()`, `Relay.Publishing()`. In publish mode the per-row step hands the row to the `Publisher` and marks it published only after it returns nil; the claim adds a `NOT EXISTS` clause so an aggregate's rows are published one at a time, in order; a `Transient` failure does not consume the attempt budget. In-process mode (no publisher) is unchanged. No Kafka dependency is added: the implementation is `services/worker/transport`.
- Added `Deliver(ctx, db, handler, event)` (the former unexported `runOne`: skip if processed, run the handler, write `processed_events`, all in one transaction, a panic returned as an error) and `Handler.Handles`, so Kafka consumers run the same handlers the same way.
- Added the dead-letter contract: `DeadLetter`, `DeadLetterSink`, `WithDeadLetterSink`, `RelayConsumer`, `Relay.ParkFailures()`. When the relay gives up on an event it parks it first and drops it only after `Park` succeeded; if `Park` fails the row stays unpublished and the failure is counted. With no sink the behaviour is the old log-and-drop. Behaviour change worth knowing: the relay's `recordFailure` now takes a context and per-consumer failures (internal; no exported signature changed).

- Added: `DecodedEvent.TraceID`, `SpanID` (empty when absent) and `ActorID` (`*uuid.UUID`, nil when absent), read from the outbox row so handlers and the future Kafka producer can see them. Requires migration `platform/000003`; the poll query selects the new columns.

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
