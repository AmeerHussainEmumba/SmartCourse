# tools/seed

Populates a local database with a realistic dataset — instructors, courses at
every lifecycle state, students with partial progress — so a contributor gets
a working system with data in one command:

```
make seed    # go run ./tools/seed
```

Requires `make dev-workflow` (not just `dev-core`) already running, and
migrations applied (`make migrate-up`) — `Publish` starts a real Temporal
workflow as of M2 (ADR-0025), and `dev-core` alone doesn't bring Temporal
up. Retries the publish call itself a few times on a transient
`temporarily_unavailable` error, since bringing up the whole stack at once
means this script's own first real Temporal RPC can race the server's own
startup — see `publishWithRetry` in `main.go`.

## Host-run, so host addresses

`make seed` runs `go run ./tools/seed` on your host, so it has to use the ports compose publishes on `127.0.0.1`, not the compose-internal names the containers use (`temporal:7233`, `postgres:5432`, `redis:6379`, `minio:9000`). The Makefile target therefore always overrides `TEMPORAL_HOST_PORT=localhost:7233` for the seed process, whatever `.env` contains (`make seed SEED_TEMPORAL_HOST_PORT=host:port` if yours differs), and rewrites `POSTGRES_DSN`, `REDIS_ADDR` and `MINIO_ENDPOINT` to `localhost:5433`, `localhost:6380` and `localhost:9002` only when they still name the compose-internal host (it prints a line when it does; custom host ports and credentials are left alone). Running `go run ./tools/seed` directly bypasses this, so set those values yourself.

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
  see `services/identity/identity/dto.go`).
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
