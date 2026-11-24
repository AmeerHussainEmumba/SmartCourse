# Changelog — libs/relay

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added publish mode (ADR-0029): `Publisher`, `WithPublisher`, `WithPublishBackoff`, `Transient` / `IsTransient`, `Relay.Handlers()`, `Relay.Publishing()`. In publish mode the per-row step hands the row to the `Publisher` and marks it published only after it returns nil; the claim adds a `NOT EXISTS` clause so an aggregate's rows are published one at a time, in order; a `Transient` failure does not consume the attempt budget. In-process mode (no publisher) is unchanged. No Kafka dependency is added: the implementation is `services/worker/transport`.
- Added `Deliver(ctx, db, handler, event)` (the former unexported `runOne`: skip if processed, run the handler, write `processed_events`, all in one transaction, a panic returned as an error) and `Handler.Handles`, so Kafka consumers run the same handlers the same way.
- Added the dead-letter contract: `DeadLetter`, `DeadLetterSink`, `WithDeadLetterSink`, `RelayConsumer`, `Relay.ParkFailures()`. When the relay gives up on an event it parks it first and drops it only after `Park` succeeded; if `Park` fails the row stays unpublished and the failure is counted. With no sink the behaviour is the old log-and-drop. Behaviour change worth knowing: the relay's `recordFailure` now takes a context and per-consumer failures (internal; no exported signature changed).

- Added: `DecodedEvent.TraceID`, `SpanID` (empty when absent) and `ActorID` (`*uuid.UUID`, nil when absent), read from the outbox row so handlers and the future Kafka producer can see them. Requires migration `platform/000003`; the poll query selects the new columns.

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
