# Changelog — libs/cache

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- README: states which service binaries use the module (microservices split). No code change.

- Added `EnableTracing(*redis.Client)`: a go-redis hook that records each command run under a request span (command name only, never the key or arguments; `redis.Nil` is not an error). Not called means no hook.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
