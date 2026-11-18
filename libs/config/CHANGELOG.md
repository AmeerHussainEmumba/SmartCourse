# Changelog — libs/config

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added `RELAY_WORKERS` (1 to 64, default 4) as `Config.Relay.Workers`, and a startup check that `POSTGRES_MAX_OPEN_CONNS` is at least 2 x `RELAY_WORKERS` (ADR-0027: each relay worker holds a claim connection while its handlers need another). `Load` now fails for a smaller pool in every binary; the defaults (20 and 4) are unaffected. Nothing reads `Relay.Workers` yet: `services/worker` still builds the relay with its own default of 4 until it passes `relay.WithWorkerCount(cfg.Relay.Workers)`.

- Added `ARGON2_MAX_CONCURRENT` (1 to 1024; default derived from GOMAXPROCS and parallelism, clamped to 2..8) and `ARGON2_QUEUE_TIMEOUT` (default 5s, max 1m), exposed as `Argon2Config.MaxConcurrent` / `QueueTimeout`, plus `DefaultArgon2MaxConcurrent` (F-26 residue). Additive.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
