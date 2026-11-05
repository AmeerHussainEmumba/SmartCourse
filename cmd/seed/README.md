# cmd/seed

Populates a local database with a realistic dataset — instructors, courses at
every lifecycle state, students with partial progress — so a contributor gets
a working system with data in one command:

```
make seed    # go run ./cmd/seed
```

Requires `make dev-core` (or any profile including Postgres/Redis) already
running, and migrations applied (`make migrate-up`).

## Why it goes through the service layer, not raw SQL

`main.go` calls `identity.Service.Register`, `course.Service.CreateCourse`/
`AddModule`/`AddLesson`/`Publish`, `enrollment.Service.Enroll`, and
`progress.Service.MarkComplete` — the exact same methods the HTTP handlers
call. A raw-SQL seeder would drift from validation rules and transactional
invariants the moment either side changed; this one can't, and it doubles as
an end-to-end smoke test of all four core modules wired together, run on
every `make seed`.

## What it creates

- 1 admin, 3 instructors, 12 students — all registered through the real
  signup flow, then promoted past the student default via
  `UpdateUserRole` where needed (registration itself never accepts a role —
  see `internal/identity/dto.go`).
- 5 courses, deliberately spanning every lifecycle state this codebase
  currently supports: a draft never published, a published course with no
  enrollments yet, a published course filled exactly to its enrollment
  limit (exercising ADR-0012's seat-limit path with real data), and
  published courses with students at every progress stage — not started,
  partway through, and fully complete (the last of which exercises
  `progress.course_completed`'s outbox write, same as a real course
  completion would).

All seeded accounts share one password, printed at the end of the run:
`SeedPass123!`. This data never leaves a developer's own machine (ADR-0018's
local-only compose profiles) — it is not a credential that protects
anything.

## Idempotency

The script checks for `admin@smartcourse.dev` before doing anything else; if
it exists, it logs and exits. Running `make seed` twice is a no-op the
second time, not a pile of duplicate-email errors.
