# Changelog — libs/testenv

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- Added `SetupKafka` (`kafka.go`): an `apache/kafka:3.7.0` testcontainer helper (the image docker-compose pins) for `libs/kafka`, with `Kill` and `Start` so a test can stop and restart the broker. It binds a fixed host port so the advertised address stays valid across a restart. `testenv.Setup` is unchanged; testcontainers-go becomes a direct requirement.
- Added `SetupMongo` (`mongo.go`): a MongoDB testcontainer helper for `libs/mongostore`. It makes every module that imports testenv carry the MongoDB driver and testcontainers' MongoDB module in its `go.mod` (test-only; no binary links them).
- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
