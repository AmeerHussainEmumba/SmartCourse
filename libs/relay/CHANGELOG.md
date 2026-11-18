# Changelog — libs/relay

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added: `DecodedEvent.TraceID`, `SpanID` (empty when absent) and `ActorID` (`*uuid.UUID`, nil when absent), read from the outbox row so handlers and the future Kafka producer can see them. Requires migration `platform/000003`; the poll query selects the new columns.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
