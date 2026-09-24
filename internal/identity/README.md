# internal/identity

Users, credentials, roles, and every token type that authenticates a request: JWT access tokens, rotating refresh tokens, password-reset tokens, email-verification tokens.

## Why this module looks the way it does

**Three token types, one shape.** Refresh, password-reset, and email-verification tokens are all opaque random values handed to the client once; only a SHA-256 hash is ever stored (`internal/platform/auth/tokens.go`). A database disclosure yields no usable token for any of the three. See ADR-0015 and `docs/rfc/differences-from-requirements.md` DR-008 (password reset / email verification weren't in the original brief — added deliberately, not by accident).

**Refresh rotation with reuse detection.** Every refresh issues a new token and marks the old one `replaced_by`. Presenting an already-replaced token means it was stolen — the entire `family_id` is revoked, not just the presented token. `TestRefresh_ReuseDetection_RevokesWholeFamily` in `service_integration_test.go` is the test that proves this actually happens, not just that the code intends it.

**The privilege-revocation check (ADR-0015 addendum).** `UpdateUserRole` bumps `users.token_version` in Postgres *and* writes the new value to Redis in the same request. `internal/platform/auth.RequireFreshPrivilege` reads that Redis value on admin/privilege-mutating routes only — see `internal/platform/README.md` for why this is a third, narrow layer rather than a general per-request check.

**No self-service role escalation.** `RegisterRequest` (`dto.go`) has no `role` field. Every new account is `student`/`active`. Becoming an instructor or admin is exclusively `PATCH /admin/users/{id}/role`, gated by `RequireRole("admin")` *and* `RequireFreshPrivilege` — see `handler.go`.

**Timing-safe unknown-account handling.** `Login` compares against `auth.DummyHash` even when the email doesn't exist, so response timing doesn't leak account existence (RFC §11.3). `TestLogin_UnknownEmail_SameErrorAsWrongPassword` asserts the two failure paths return identical error codes.

**Email delivery is a documented stopgap, not a gap.** `EmailSender` (`email.go`) is a consumer-declared interface; `LoggingEmailSender` just logs. Real delivery via Asynq lands in M3 (RFC §6.7/ADR-0014) — swapping the implementation at wiring time in `cmd/smartcourse` is the only change needed.

**Login is rate-limited twice, on purpose, against different attack shapes (ADR-0020, RFC §11.3).** A per-IP GCRA limit at the route level (`handler.go`) catches many guesses from one source. A separate per-account limit inside `Service.Login` itself (`ratelimit.AccountKey`, checked before the database is even touched) catches one attacker spreading guesses for one account across many IPs — a per-IP-only limit wouldn't. `TestLogin_PerAccountRateLimit_BlocksAfterThreshold` proves the account limit actually blocks the correct password once the budget is exhausted, not just that a limiter type exists.

## Endpoints

| Method & path | Auth | Notes |
|---|---|---|
| `POST /auth/register` | — | Always issues student role |
| `POST /auth/login` | — | |
| `POST /auth/refresh` | — | Rotates the refresh token |
| `POST /auth/logout` | — | Idempotent; revokes the presented token only |
| `POST /auth/password-reset/request` | — | Always `202`, whether or not the email exists |
| `POST /auth/password-reset/confirm` | — | Revokes all refresh tokens + bumps `token_version` on success |
| `POST /auth/verify-email/resend` | — | Always `202` |
| `GET /auth/verify-email/confirm?token=` | — | |
| `GET /me`, `PATCH /me` | `RequireAuth` | |
| `PATCH /admin/users/{id}/role` | `RequireAuth` + `RequireRole(admin)` + `RequireFreshPrivilege` | |

## Testing

- **Unit** (`internal/platform/auth/*_test.go`): password hashing, JWT issue/parse, alg-confusion rejection — no database.
- **Integration** (`service_integration_test.go`, build tag `integration`): duplicate/case-insensitive email rejection, login failure-path equivalence, refresh rotation, reuse detection, and the Redis token-version cache write — against real Postgres and Redis via `test/integration/testenv`.

Run: `make test` (unit) or `make test-integration` (integration, needs Docker). Full index: `docs/guides/testing.md`.
