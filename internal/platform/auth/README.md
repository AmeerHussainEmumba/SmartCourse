# internal/platform/auth

JWT access tokens, opaque refresh/reset/verification tokens, argon2id password hashing, and the three-layer authorization middleware (`RequireAuth`, `RequireRole`, `RequireFreshPrivilege`). See `docs/adr/smartcourse-adr.md` ADR-0015 (and its addendum) for the full reasoning; `internal/identity` is this package's only real consumer and owns the actual login/refresh/reset flows — this package is the toolkit, not the flow.

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

## Why each piece exists

- **argon2id, not bcrypt** (`password.go`) — bcrypt silently truncates input past 72 bytes and is no longer OWASP's first recommendation; argon2id resists GPU/ASIC cracking better and its cost parameters are explicitly tunable as hardware gets faster (`config.Argon2Config`). The encoded hash format (`$argon2id$v=...$m=...,t=...,p=...$salt$hash`) is self-describing specifically so old hashes stay verifiable after the parameters change.
- **`DummyHash`** (`password.go`) — a precomputed hash compared against on a login attempt for an email that doesn't exist, so the response takes the same amount of time either way. Without it, "wrong password" and "no such account" are distinguishable by latency alone, which is exactly the account-enumeration leak RFC §11.3 calls out.
- **Explicit signing-method assertion on parse** (`jwt.go`) — `ParseAccessToken` rejects anything that isn't HMAC before even checking the signature. Skipping this is the `alg: none` class of vulnerability: a library that trusts the token's own header to say how it was signed can be handed a token claiming "no signature," and some implementations historically accepted that. `TestParseAccessToken_RejectsAlgNone` is the regression test.
- **`token_version`, not a token blocklist** (`jwt.go`, `middleware.go`) — an access token can't be individually deleted; it's just signed data that already left the server. Instead, every user has a version number; bumping it (password reset, role change) makes every *previously issued* token's embedded version stale. `RequireFreshPrivilege` is the only thing that checks it, which is why it exists as its own middleware layer rather than being folded into `RequireAuth`.
- **Constant-time comparison everywhere a stored hash meets a client-supplied value** (`password.go`'s `VerifyPassword`, `tokens.go`'s `ConstantTimeEqual`) — a naive `==` on bytes returns as soon as it finds a mismatch, which leaks how many leading bytes matched through response timing. `subtle.ConstantTimeCompare` takes the same time regardless.

## Where the two auth bugs fixed in review actually lived

Both bugs fixed after supervisor review (`internal/identity/README.md` has the full account) were really about *using* this package's pieces correctly, not about anything wrong in this package itself:

- Password reset bumped `token_version` (killing access tokens on the routes that check it) but didn't also revoke the user's refresh tokens — so a stolen refresh token could keep minting fresh ones indefinitely. The fix lives in `internal/identity/service.go`, calling a repository method this package has no role in.
- Two concurrent refresh requests for the same token could both succeed — a row-locking gap in `internal/identity/repository.go`'s query, not in how this package verifies or issues tokens.

Both are documented here because understanding *why* `token_version` and refresh-token revocation are two separate mechanisms (see "The shape" above) is what makes the bugs, and their fixes, make sense.

## Testing

`jwt_test.go`: round-trip issue/parse, expired tokens rejected, wrong signing key rejected, `alg: none` rejected, wrong issuer rejected. `password_test.go`: correct/wrong password, unique salt per call (two hashes of the same password never match byte-for-byte), a malformed encoded hash is rejected cleanly rather than panicking, `DummyHash` itself is a valid hash that always fails verification. Both are plain unit tests (`make test`) — nothing here needs real infrastructure, since this package never touches a database or Redis directly.
