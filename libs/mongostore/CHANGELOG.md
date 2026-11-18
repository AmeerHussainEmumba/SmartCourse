# Changelog — libs/mongostore

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Module created (Week 3 plan, ADR-0026).
- Added `Open`/`Ping`/`Close` with context-bounded timeouts and idempotent index creation (no TTL).
- Added the `events` archive (`ArchiveEvent`, `GetEvent`; idempotent, stores the schema id), `audit_log` (`AppendAudit`) and `failed_events` (`ParkFailedEvent`, `GetFailedEvent`, `ListFailedEvents` with keyset pagination, `MarkReplayed`, `CountFailedSince`).
- Depends on `go.mongodb.org/mongo-driver/v2` v2.10.0 (Apache-2.0).
