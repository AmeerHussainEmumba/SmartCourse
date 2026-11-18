# Changelog — libs/config

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added `ARGON2_MAX_CONCURRENT` (1 to 1024; default derived from GOMAXPROCS and parallelism, clamped to 2..8) and `ARGON2_QUEUE_TIMEOUT` (default 5s, max 1m), exposed as `Argon2Config.MaxConcurrent` / `QueueTimeout`, plus `DefaultArgon2MaxConcurrent` (F-26 residue). Additive.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
