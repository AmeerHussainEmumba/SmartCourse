# Changelog — libs/auth

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

- Added `ArgonGate` (`NewArgonGate`, `Acquire`, `Hash`, `Verify`, `InFlight`) and `ErrArgonBusy`: a semaphore bounding concurrent argon2 computations, with waiters bounded by the request context and a queue timeout (F-26 residue). `HashPassword`/`VerifyPassword` are unchanged. Test-only dependency on `go.uber.org/goleak`.

- Added `TokenVersionSource`, `ErrUserNotFound` and `RequireFreshPrivilegeWithSource`: on a Redis miss the token version is read from the durable source and written back (F-03). `RequireFreshPrivilege` is unchanged.
- Added `NewDummyHash(cfg)`: dummy hash built from the configured Argon2 parameters (F-30). `DummyHash` is unchanged.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
