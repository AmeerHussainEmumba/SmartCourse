# Changelog — libs/db

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- README: states which service binaries use the module (microservices split). No code change.

- Added `EnableTracing(*gorm.DB)`: hand-written GORM callbacks that make each statement run under a request span a CLIENT child span (`<operation> <table>`, db.system.name, statement text with bind placeholders; error type only, never the message). A statement with no active span starts nothing. Never called means no callback and no overhead. No new module (otelgorm and the GORM plugin were not adopted).

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
