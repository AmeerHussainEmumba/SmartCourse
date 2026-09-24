# internal/enrollment

Enrollments, seat limits, and prerequisites (RFC §4.3, §8.1). The module whose correctness under concurrency matters more than any other in this codebase — see ADR-0012.

## Why this module looks the way it does

**`courses` and `lesson_progress` are touched by raw SQL, not by importing `internal/course` or `internal/progress`.** This looks like a boundary violation and isn't one. ADR-0012's row lock (`SELECT ... FOR UPDATE`) must run inside *this* transaction — calling into another module's Go API would mean a second database connection, which defeats the lock entirely. `repository.go`'s package comment explains this in full; the short version is that table ownership for schema purposes and which module's `repository.go` is allowed to issue SQL against a table are two different things when correctness demands it.

**The row lock is the whole point (ADR-0012).** `lockCourseForEnrollment` in `repository.go` runs `SELECT ... FOR UPDATE` on the course row *before* checking the seat count. Skip this and two students racing for the last seat both read the same stale count, both pass the check, and the limit is silently violated — with no error anywhere, and no functional test that would ever catch it. `TestEnroll_SeatLimit_NeverExceededUnderConcurrency` is the test that actually proves this: 30 students, 10 seats, real goroutines, real Postgres.

**Lock ordering is a rule, not a habit.** Both `Enroll` and `Withdraw` lock `courses` before touching `enrollments`, per RFC §8.5's audit table. This was actually a bug caught during that audit — `Withdraw` originally decremented the counter via a bare `UPDATE` without the explicit lock — see ADR-0012's addendum.

**`EnrolledAt` is set explicitly in Go, not left to the column's `DEFAULT now()`.** GORM includes every non-pointer struct field in the `INSERT` column list regardless of whether it was "set" — a zero-value `time.Time` sends `0001-01-01`, not "use the database default." This is exactly backwards from `CreatedAt`/`UpdatedAt`, which GORM *does* auto-populate by naming convention. Caught in this package's own test output (the SQL trace of a `-v` run), not by inspection — see the comment in `service.go`'s `Enroll` method before "fixing" this the other way.

## Testing — this module's tests are the point of the module

- **`TestEnroll_SeatLimit_NeverExceededUnderConcurrency`** — 30 goroutines, 10 seats. Asserts successes == limit, rejections == contenders − limit, `courses.enrollment_count` matches actual row count. This is ADR-0012 proven, not documented.
- **`TestEnroll_DuplicateAttempt_ExactlyOneSucceeds`** — 10 concurrent identical enrollment attempts by one student; exactly one succeeds, via the partial unique index, not application logic.
- **`TestWithdraw_ThenReenroll_Allowed`** — proves the partial index (`WHERE status IN ('active','completed')`) permits re-enrollment after withdrawal, which a plain unique constraint would block.
- **`TestEnroll_UnpublishedCourse_Rejected`** — a course with no `published_version_id` can't be enrolled into.

All in `service_integration_test.go` (build tag `integration`), against real Postgres via `test/integration/testenv`. Run: `make test-integration`. Full index: `docs/guides/testing.md`.

## Known scope limits (M1)

`Idempotency-Key` header handling (RFC §15.1) isn't implemented as general middleware yet — duplicate-enrollment protection today comes entirely from the partial unique index (`idx_one_active_enrollment`), which satisfies the FR-1 requirement but doesn't give idempotent-replay-of-the-same-request semantics for other write endpoints. Tracked as a follow-up, not silently dropped.
