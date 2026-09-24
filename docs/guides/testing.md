# Testing guide

Every test category SmartCourse uses, what it checks, why that check matters, and how to run it. This is the index RFC §18.8 requires: a future contributor deciding whether a change is safe should be able to answer "what would break" from this document, not by reading every test file.

## Quick reference

| Command | Runs | Needs Docker? |
|---|---|---|
| `make test` | Unit tests | No |
| `make test-race` | Unit tests, race detector on | No |
| `make test-integration` | Integration + concurrency tests | Yes |
| `go test -tags=integration -race ./...` | Everything, one process | Yes |

CI (`.github/workflows/ci.yml`) runs the full set — build, `go vet`, `golangci-lint` (module-boundary `depguard` rules), migrations against a real Postgres service container, then `go test -race ./...` with `POSTGRES_DSN`/`REDIS_ADDR` set so integration tests run too.

## Test tiers

### Unit — no database, no network

Domain logic tested against pure functions and in-memory state. Fast, run constantly during development.

| Package | What it checks |
|---|---|
| `internal/platform/auth` | argon2id hash/verify round-trips and rejects wrong passwords; the dummy hash (timing-safety, RFC §11.3) is itself valid; JWT issue/parse round-trips; expired tokens rejected; wrong signing key rejected; **`alg:none` / alg-confusion rejected** (`TestParseAccessToken_RejectsAlgNone` — this is the specific check that closes a real vulnerability class, not a generic parser test) |

Run: `go test ./internal/platform/auth/...` or `make test`.

### Integration — real Postgres (and Redis, where relevant), via `testcontainers-go`

RFC §18.2's rule: **mocks are not used for the database.** A mock proves the code called the method it was written to call; it cannot prove a constraint holds, a transaction rolls back correctly, or a partial index behaves as designed. Every test in this tier spins up a real, disposable Postgres (and Redis) container via `test/integration/testenv`, runs every migration in `migrations/` against it, and tears it down after — no shared state between tests, no fixtures to go stale.

Build tag: `integration`. Excluded from `go test ./...` by default so unit tests stay fast; included by `make test-integration`.

| Package | What it checks | Why it matters |
|---|---|---|
| `internal/identity` | Duplicate email rejected (unique constraint); **case-insensitive** duplicate rejected (the `CITEXT` column, not an application `lower()` check that would race); login failure paths return identical error codes for unknown-email vs wrong-password; refresh rotation issues a new token and invalidates the old one; **reuse detection burns the whole token family**, not just the presented token; role change bumps `token_version` in Postgres **and** writes it to Redis in the same operation | Proves ADR-0015's rotation/reuse-detection design actually works, not just that it's documented |
| `internal/course` | Full create → author → publish → catalogue → detail lifecycle; publishing with no modules is rejected (`ValidateMetadata`); a non-owning instructor can't edit another's draft; an invalid slug format is rejected | Proves blue-green promotion (ADR-0010) and ownership checks (RFC §11.4) hold against a real foreign-key-constrained schema — course creation's 3-step transaction order was itself a bug caught by this suite, not by reading the code |
| `internal/enrollment` | **Seat limit never exceeded under real concurrency** (30 goroutines, 10 seats — see below); duplicate concurrent enrollment attempts resolve to exactly one success; withdrawal then re-enrollment is permitted (partial unique index); enrolling in an unpublished course is rejected | This is where ADR-0012 is proven, not documented |
| `internal/progress` | Enrollment transitions to `completed` only once every lesson in the *current* published version is done, not before; marking an already-complete lesson twice is a byte-for-byte no-op; a non-owner can't mark someone else's progress | Proves the idempotency and version-currency rules in RFC §8.3 |
| `internal/platform/relay` | Bounded worker pool actually bounds concurrency, not just configures a number; idempotency holds across retries (fail twice, succeed once, exactly one `processed_events` row); a poison event stops retrying after `maxAttempts`; graceful shutdown drains an in-flight job's **database commit**, not just its Go function return | ADR-0021's worker pool proven, not documented — and the shutdown test caught a real bug during development (see the guide's own note below) |

Run: `make test-integration`, or target one package: `go test -tags=integration -race -v ./internal/enrollment/...`.

### Concurrency — the highest-value tier (RFC §18.3)

Not a separate build tag — these live inside the `integration` tier because they need a real database to be meaningful. Singled out here because they catch a class of bug invisible to every other kind of test.

**`TestEnroll_SeatLimit_NeverExceededUnderConcurrency`** (`internal/enrollment/service_integration_test.go`) is the one to read first if you're new to this codebase. 30 real goroutines call `Enroll` simultaneously against a course with a 10-seat limit. The test asserts three things independently: exactly 10 calls succeed, exactly 20 return `course_full`, and — separately — the database's `enrollment_count` column and the actual count of `active` enrollment rows both equal 10. That third check exists because a bug could make the *count of successful Go function calls* look right while the *database* is still wrong (e.g. a counter that increments but a row that doesn't insert, or vice versa).

Without ADR-0012's row lock, this test doesn't reliably fail — it's *flaky*, which is worse: it passes most runs and fails under load, exactly mirroring how the bug it catches would show up in production (fine until a real launch-day spike).

**`TestEnroll_DuplicateAttempt_ExactlyOneSucceeds`** is the same shape applied to duplicate-enrollment prevention: 10 concurrent identical requests from one student, exactly one succeeds, enforced by the database's partial unique index rather than an application-level check that would itself race.

**`internal/platform/relay/relay_integration_test.go`** is the other one worth reading closely, specifically for `TestRelay_GracefulShutdown_DrainsInFlightWork`. Its first version asserted only that an in-memory flag was set after shutdown — and passed, against a real bug (the worker was handing in-flight jobs the already-cancelled shutdown context, so the handler's Go code finished but its database commit silently failed). The fixed version asserts on the database row instead. The lesson generalizes: a concurrency test that checks "did the goroutine run" is weaker than one that checks "did the goroutine's *effect* persist" — the gap between those two is exactly where this kind of bug hides.

**`TestLogin_PerAccountRateLimit_BlocksAfterThreshold`** (`internal/identity/service_integration_test.go`) rounds this out from the shared-state-synchronization side (RFC §7's "Rate limiter state" example): it burns the per-account GCRA budget with wrong-password attempts, then asserts the *correct* password is still rejected as rate-limited — proving the limiter blocks, not just that it's wired in.

### Contract and query-shape tiers — not yet implemented

Two tiers RFC §18 specifies that don't exist yet, tracked here rather than silently absent:

- **Contract tests** (§18.5) — event schema `BACKWARD`-compatibility checks against the Schema Registry. Blocked on Protobuf schemas existing, which is M3 scope (ADR-0008).
- **Query-shape tests** (§18.4) — mechanical assertions on query count per request, to catch N+1 regressions automatically rather than by code review. Worth adding once there's a second list endpoint to compare against; `internal/course`'s catalogue query is already written as a single raw-SQL join specifically to avoid needing this yet.
- **Workflow tests** (§18.6) — Temporal's test framework, mocked activities. Blocked on the Temporal workflow existing (M2).
- **Security tests** as their own explicit tier (auth bypass, IDOR probing, mass-assignment attempts, injection against raw-SQL paths) — today, ownership and IDOR checks are exercised incidentally by the integration tests above (e.g. `TestUpdateDraft_RejectsNonOwner`, `TestMarkComplete_RejectsNonOwner`), not by a dedicated adversarial suite. A follow-up, not a gap papered over.

## Writing a new test

- **Unit test**: no database dependency, tests one function/method in isolation. Put it in the same package, `_test.go` suffix, no build tag.
- **Integration test**: needs Postgres (and/or Redis). Put it in the same package as `_integration_test.go` (or `service_integration_test.go`, matching this codebase's convention), with `//go:build integration` as the first line, and use `test/integration/testenv.Setup(t)` to get connected clients — never hand-roll container setup.
- **Every test file gets a header comment stating what it checks and why that check matters** (RFC §18.8) — not just what it calls. If you can't articulate what failure the test would catch, that's a sign the test (or the code it covers) needs more thought before either is written.
