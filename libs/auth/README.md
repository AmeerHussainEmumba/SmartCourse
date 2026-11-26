# libs/auth

JWT access tokens, opaque refresh/reset/verification tokens, argon2id password hashing, and the three-layer authorization middleware (`RequireAuth`, `RequireRole`, `RequireFreshPrivilege`). See `docs/adr/smartcourse-adr.md` ADR-0015 (and its addendum) for the full reasoning; `services/identity/identity` is this package's only real consumer and owns the actual login/refresh/reset flows — this package is the toolkit, not the flow.

**Used by** the API binaries: `identity-api` (issue and verify, hashing), `course-api` and `analytics-api` (verify, role and freshness middleware) and `audit-api` (`RequireAuth`, `RequireRole`, `RequireFreshPrivilegeStrict`). Workers do not import it. `libs/infra` also imports it to install the outbox actor resolver.

## The shape

Two kinds of token, deliberately not the same thing:

- **Access tokens** (`jwt.go`) are self-contained, signed JWTs — user ID, role, and a `token_version` claim, HMAC-signed, 15 minutes. Verifying one (`ParseAccessToken`) is pure computation: check the signature, check the expiry, done. No database round trip, which is the point — this runs on every single authenticated request.
- **Refresh, password-reset, and email-verification tokens** (`tokens.go`) are opaque: 32 random bytes, handed to the client once, and only their SHA-256 hash is ever stored. A database leak yields nothing usable. These *do* cost a database lookup to redeem — acceptable, because unlike the access token they're not used on every request.

`middleware.go` is where both get turned into authorization decisions, in three separate layers that are never conflated:

```
RequireAuth            → is there a valid, unexpired, correctly-signed token at all
RequireRole(...)        → coarse, route-level ("must be an instructor")
RequireFreshPrivilege   → narrow: does this token's claimed privilege still match
                           what's current, right now, in Redis
```

`RequireFreshPrivilege` is applied only to the small set of routes where a stale claim is actually dangerous (admin actions, anything mutating another user's access) — not to every request, because that would mean a Redis round trip on every single call, defeating the reason access tokens are stateless in the first place. A fourth layer — does this user own *this specific resource* — isn't here at all; it lives in each module's `service.go`, because only the service has actually loaded the resource to check against.

## What a token-version cache miss means (three variants)

The token-version key (`user:tv:<id>`) lives in Redis, and Redis can be flushed or restarted. What a middleware does when the key is absent depends on who is protected and whether the service has a durable source, and the three answers are deliberately different:

| Middleware | Miss means | Used by | Why |
|---|---|---|---|
| `RequireFreshPrivilege` | version 1 (fail open for version 1 tokens) | course-api instructor writes | F-38: a flush must not lock every instructor out of authoring. Cost: the old token of an instructor demoted from version 1 is accepted until they log in. Accepted for these routes, unchanged. |
| `RequireFreshPrivilegeWithSource` | read from the durable source, write back (SET NX) | identity `/me` and admin routes | identity owns `users.token_version`, so it can be exact. |
| `RequireFreshPrivilegeStrict` | refuse: 401 `token_stale` | audit-api admin routes (`/api/v1/admin/failed-events`, `services/audit/admin`), analytics-api `GET /analytics/issues` | These services cannot read identity's tables (ADR-0022 to 0024, ADR-0033: the Postgres roles enforce it) and the surface is admin-only. A demoted admin must not regain access because the cache forgot the demotion (E10). |

The admin and instructor tradeoffs differ on purpose: an instructor lockout blocks teaching until the next login, an admin token wrongly accepted exposes operator data (failed-event payloads, replay). Do not copy the instructor rule onto an admin route, or the strict one onto instructor routes.

Cost of `Strict`: after a flush an admin is refused on those routes until the key returns. Identity's re-seeder (`RunTokenVersionReseeder`, 15 s tick, sentinel TTL 10 min) rewrites every version above 1 within one tick (at most about 15 s after the flush, plus Redis and Postgres latency) and does NOT write version 1 (no key is "version 1" for identity-backed routes). An admin at version 1 therefore stays refused on the strict routes until their key is written: login, refresh, or any call to an identity `/me` or admin route with that token (these use `WithSource`, which writes the key back). Refusal is a clean 401 `token_stale`; the client's normal response is to refresh or log in again. Redis being unreachable (not a miss) answers 503 for all variants.

## Accepted limitation: a suspended instructor's token (S-07, S-09)

Owner decision 2026-11-26: documented, not fixed. A suspended or demoted instructor's already-issued access token (at most 15 minutes of life):

- **cannot write.** Course-api's instructor writes use `RequireFreshPrivilege`, which compares the token's version with Redis and refuses the stale one (401 `token_stale`). The exception is a window of up to about 15 s after a Redis flush: the key is gone, a miss reads as version 1, and the stale token writes until identity-worker's re-seeder (`RunTokenVersionReseeder`, 15 s tick) rewrites every version above 1. Once it has, writes are refused again. This is the price of the first row of the table above (a flush must not lock every instructor out of authoring).
- **can still read their own course analytics** until the token expires. The per-course analytics routes in analytics-api check signature, expiry and role/ownership, not the token version (only the admin issues route does, with `RequireFreshPrivilegeStrict`).

Pinned by `TestAcceptedLimitation_S09_SuspendedInstructorWriteWindowAfterRedisFlush` (`services/course/course`) and `TestAcceptedLimitation_S07_SuspendedInstructorStillReadsOwnCourseAnalyticsUntilTokenExpiry` (`services/analytics/analytics`). If the decision is reopened, those tests change with it. Reversal trigger: a requirement that a suspended instructor lose analytics read access immediately, or that authoring writes survive no Redis loss at all (use `RequireFreshPrivilegeWithSource` with a durable source, which course-api cannot have under ADR-0024).

## Bounding argon2 concurrency

`ArgonGate` is a semaphore around `HashPassword`/`VerifyPassword`. Each argon2 call allocates `ARGON2_MEMORY_KIB`, so peak memory is about `ARGON2_MAX_CONCURRENT x ARGON2_MEMORY_KIB` however many logins arrive (16 unbounded concurrent logins pinned about 1 GiB). A caller that cannot get a slot within `ARGON2_QUEUE_TIMEOUT`, or whose context ends first, gets `ErrArgonBusy` or the context error and holds nothing; identity maps both to 503 `temporarily_unavailable`.

## Why each piece exists

- **argon2id, not bcrypt** (`password.go`) — bcrypt silently truncates input past 72 bytes and is no longer OWASP's first recommendation; argon2id resists GPU/ASIC cracking better and its cost parameters are explicitly tunable as hardware gets faster (`config.Argon2Config`). The encoded hash format (`$argon2id$v=...$m=...,t=...,p=...$salt$hash`) is self-describing specifically so old hashes stay verifiable after the parameters change.
- **`DummyHash`** (`password.go`) — a precomputed hash compared against on a login attempt for an email that doesn't exist, so the response takes the same amount of time either way. Without it, "wrong password" and "no such account" are distinguishable by latency alone, which is exactly the account-enumeration leak RFC §11.3 calls out.
- **Explicit signing-method assertion on parse** (`jwt.go`) — `ParseAccessToken` rejects anything that isn't HMAC before even checking the signature. Skipping this is the `alg: none` class of vulnerability: a library that trusts the token's own header to say how it was signed can be handed a token claiming "no signature," and some implementations historically accepted that. `TestParseAccessToken_RejectsAlgNone` is the regression test.
- **`token_version`, not a token blocklist** (`jwt.go`, `middleware.go`) — an access token can't be individually deleted; it's just signed data that already left the server. Instead, every user has a version number; bumping it (password reset, role change) makes every *previously issued* token's embedded version stale. `RequireFreshPrivilege` is the only thing that checks it, which is why it exists as its own middleware layer rather than being folded into `RequireAuth`.
- **Constant-time comparison everywhere a stored hash meets a client-supplied value** (`password.go`'s `VerifyPassword`, `tokens.go`'s `ConstantTimeEqual`) — a naive `==` on bytes returns as soon as it finds a mismatch, which leaks how many leading bytes matched through response timing. `subtle.ConstantTimeCompare` takes the same time regardless.

## Where the two auth bugs fixed in review actually lived

Both bugs fixed after supervisor review (`services/identity/identity/README.md` has the full account) were really about *using* this package's pieces correctly, not about anything wrong in this package itself:

- Password reset bumped `token_version` (killing access tokens on the routes that check it) but didn't also revoke the user's refresh tokens — so a stolen refresh token could keep minting fresh ones indefinitely. The fix lives in `services/identity/identity/service.go`, calling a repository method this package has no role in.
- Two concurrent refresh requests for the same token could both succeed — a row-locking gap in `services/identity/identity/repository.go`'s query, not in how this package verifies or issues tokens.

Both are documented here because understanding *why* `token_version` and refresh-token revocation are two separate mechanisms (see "The shape" above) is what makes the bugs, and their fixes, make sense.

## Testing

`jwt_test.go`: round-trip issue/parse, expired tokens rejected, wrong signing key rejected, `alg: none` rejected, wrong issuer rejected. `password_test.go`: correct/wrong password, unique salt per call (two hashes of the same password never match byte-for-byte), a malformed encoded hash is rejected cleanly rather than panicking, `DummyHash` itself is a valid hash that always fails verification. Both are plain unit tests (`make test`) — nothing here needs real infrastructure, since `jwt.go`/`password.go` never touch a database or Redis directly.

`middleware.go` had zero test coverage until this session, despite being where every authorization decision in the system actually gets enforced — fixed with two files, split the same way the three-layer model above already splits:

- `middleware_test.go` (unit, no build tag) — `RequireAuth` and `RequireRole` driven over real HTTP via `httptest`+a real `gin.Engine` (the first test anywhere in this repo to do that; a broader per-module HTTP test tier followed from this pattern). Covers a missing/malformed Authorization header, an expired token, a token signed with the wrong key, and — the one thing a unit test of the handler function alone can't prove — that a *successful* `RequireAuth` actually populates `context.Context` with the right `Identity` for a downstream handler to read via `FromContext`, not just that it returns 200. `RequireRole` is exercised both standalone (no identity in context at all) and layered after `RequireAuth` (disallowed vs. allowed role).
- `middleware_strict_integration_test.go` (`//go:build integration`, real Redis) — `RequireFreshPrivilegeStrict`: a miss is 401 `token_stale` even for a version-1 token (the E10 defect), a matching cached version passes, an older token is rejected, Redis down is 503.
- `middleware_integration_test.go` (`//go:build integration`, real Redis via `libs/testenv`) — `RequireFreshPrivilege`, previously unverified at any level despite being a named ADR-0015 security fix. Covers the cache-miss default (`token_version` 1 passes, anything else doesn't), a cached version matching the token's own claim, and — the actual revocation case this middleware exists for — a cached version that's moved past the token's, which must reject with `token_stale` specifically, not just a non-2xx status. The Redis `Set` call in the test setup deliberately matches `services/identity/identity.Service.UpdateUserRole`'s real one (same key format via `TokenVersionRedisKey`, no TTL) so the test is seeding state the same way production code actually writes it, not a shape that happens to be convenient for the test.
