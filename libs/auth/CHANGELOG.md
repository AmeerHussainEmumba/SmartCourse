# Changelog — libs/auth

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added `ArgonGate` (`NewArgonGate`, `Acquire`, `Hash`, `Verify`, `InFlight`) and `ErrArgonBusy`: a semaphore bounding concurrent argon2 computations, with waiters bounded by the request context and a queue timeout (F-26 residue). `HashPassword`/`VerifyPassword` are unchanged. Test-only dependency on `go.uber.org/goleak`.

- Added `TokenVersionSource`, `ErrUserNotFound` and `RequireFreshPrivilegeWithSource`: on a Redis miss the token version is read from the durable source and written back (F-03). `RequireFreshPrivilege` is unchanged.
- Added `NewDummyHash(cfg)`: dummy hash built from the configured Argon2 parameters (F-30). `DummyHash` is unchanged.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
