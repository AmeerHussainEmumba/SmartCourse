# Changelog — services/identity

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- identity-api wires telemetry in `cmd/identity-api/main.go`: tracing, RED metrics, `/metrics`, outbox gauges, bounded flush on shutdown (Phase 7).

- Security: bumped indirect dependencies to clear govulncheck findings reachable from this module's code (golang.org/x/net v0.58.0 to v0.60.0 and github.com/quic-go/quic-go v0.59.0 to v0.59.1 where present; github.com/moby/go-archive v0.2.0 to v0.3.0, github.com/klauspost/compress v1.18.6 to v1.20.2 and github.com/moby/sys/user v0.4.0 to v0.4.1 where present, test-only via testcontainers). No API change.

- identity-api now runs `Service.RunTokenVersionReseeder` (every 15s): when Redis has lost the `user:tv:<id>` keys, it re-writes every `token_version` above 1 with `SET NX`, detected via a 10-minute sentinel key. Shrinks the post-flush `token_stale` window on `course-api` from "until the next login/refresh" to about 15 seconds, with no cross-service call and no change to `course-api` (F-38 residue). New: `Service.ReseedTokenVersions`, `Repository.ListTokenVersionsAbove1`. `registerRoutes` in `cmd/identity-api` now takes a context.

- Register, login (including the unknown-email dummy comparison) and password-reset confirm now hash through `auth.ArgonGate`, so at most `ARGON2_MAX_CONCURRENT` argon2 computations run at once. A request that cannot get a slot within `ARGON2_QUEUE_TIMEOUT` (or whose client goes away) is answered 503 `temporarily_unavailable`, never counted as a failed login (F-26 residue).

- Login and refresh now seed the user's current `token_version` in Redis when it is missing (`SET NX`, never overwriting a newer value). Fixes instructors whose version is above 1 being rejected by `course-api` with `token_stale`, even on a freshly issued token, after a Redis restart (F-38).

- `POST /auth/register` now answers 202 with `{"message": ...}` and no session, identically for new and existing emails (F-32); sign in via `/auth/login`. `Service.Register` (direct callers, e.g. tools/seed) is unchanged; the route uses the new `Service.RegisterPublic`.
- Login: a suspended/deleted account answers 401 `invalid_credentials` like a wrong password (F-32); only failed attempts count toward limits: 5/min per account+IP and 20/min per account across IPs (F-31); dummy hash uses configured Argon2 (F-30).
- `/me` and `/admin/users/{id}/role` use `RequireFreshPrivilegeWithSource` (Postgres fallback on Redis miss) (F-03, F-11).
- Reset/verification tokens are single use atomically and a new request invalidates older unused tokens (F-14, F-15); per-IP limits on refresh/logout/reset/verify routes and a silent 5/hour per-address email budget (F-15).
- Role change: 404 before any side effect, 409 `last_admin` when it would remove the last active admin (F-33).

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
