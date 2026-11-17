# Changelog — services/identity

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`. A change that removes or alters an exported identifier, or alters behaviour this module's README documents, is a major-version change once the module is being tagged.

## [Unreleased]

- `POST /auth/register` now answers 202 with `{"message": ...}` and no session, identically for new and existing emails (F-32); sign in via `/auth/login`. `Service.Register` (direct callers, e.g. tools/seed) is unchanged; the route uses the new `Service.RegisterPublic`.
- Login: a suspended/deleted account answers 401 `invalid_credentials` like a wrong password (F-32); only failed attempts count toward limits: 5/min per account+IP and 20/min per account across IPs (F-31); dummy hash uses configured Argon2 (F-30).
- `/me` and `/admin/users/{id}/role` use `RequireFreshPrivilegeWithSource` (Postgres fallback on Redis miss) (F-03, F-11).
- Reset/verification tokens are single use atomically and a new request invalidates older unused tokens (F-14, F-15); per-IP limits on refresh/logout/reset/verify routes and a silent 5/hour per-address email budget (F-15).
- Role change: 404 before any side effect, 409 `last_admin` when it would remove the last active admin (F-33).

## [0.1.0] - not yet tagged

- Module created by the module split (ADR-0026, `docs/plans/module-split.md`). Nothing has been tagged; consumers reference it through a `replace` directive.
