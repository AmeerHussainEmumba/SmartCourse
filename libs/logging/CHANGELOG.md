# Changelog — libs/logging

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- README: states which service binaries use the module (microservices split). No code change.

- Added `Correlated(ctx)` (logger from `ctx` plus `trace_id` and `span_id` of the active span), `WithEventID`, `WithJobID` and the key constants (RFC section 12.4).

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
