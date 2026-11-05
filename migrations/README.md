# Migrations

Schema lives here as versioned SQL, applied by [`golang-migrate`](https://github.com/golang-migrate/migrate) — not GORM `AutoMigrate` (ADR §19.15: `AutoMigrate` can't express partial unique indexes, check constraints, or enums, all of which enforce real invariants in this schema, e.g. `idx_one_active_enrollment`, `idx_one_draft_per_course`), and not Liquibase (`docs/rfc/differences-from-requirements.md` DR-010/DR-013 — confirmed not a hard requirement).

## Schema-per-service (ADR-0024)

Four independent migration sets, one per Postgres schema, matching ADR-0023's service boundaries — all still one database, one set of credentials:

| Directory | Schema | Owns |
|---|---|---|
| `platform/` | `platform` | `outbox`, `processed_events`, `idempotency_keys` — shared integration infrastructure every producer writes to, not any one service's domain data. Also installs the `citext`/`pg_trgm` extensions into `public`, since more than one schema needs them. |
| `identity/` | `identity` | `users`, `refresh_tokens`, `password_reset_tokens`, `email_verification_tokens`. |
| `course/` | `course` | `courses`, `course_versions`, `course_prerequisites`, `modules`, `lessons`, `enrollments`, `lesson_progress`, `certificates`, `course_search_index` — course, enrollment, and progress share one schema because enrollment locks the `courses` row in the same transaction it writes an enrollment row in (ADR-0012's seat-limit guarantee); `course_search_index` stays here until `internal/search` actually exists. |
| `analytics/` | `analytics` | Three mirror tables (`user_role_mirror`, `course_mirror`, `enrollment_mirror`, fed from relay events) plus the aggregate read models (`platform_snapshot`, `course_stats`, `daily_enrollments`). |

**No foreign key crosses a schema boundary.** `courses.instructor_id`, `enrollments.student_id`, and `idempotency_keys.user_id` are plain `UUID` columns, not `REFERENCES identity.users(id)` — none of them were ever read across that boundary by application code (callers are authorized via JWT claims), and there's no user-delete feature, so nothing relied on the database catching a dangling reference. See ADR-0024 for the full reasoning, including why `platform` is a deliberate exception (it's integration infrastructure, not domain data).

Each service's connection sets `search_path` (`POSTGRES_SEARCH_PATH`, wired through `internal/platform/config` → `internal/platform/db.Open`) so bare table names in `internal/*/repository.go` resolve to the right schema without every query being hand-qualified — see `docker-compose.yml` and `Makefile`'s `run-*` targets for the exact value each binary uses.

## Running migrations

```
make migrate-up                              # applies all four services, in order (see below)
make migrate-down service=course             # rolls back one migration for one service
make migrate-down-all                        # full teardown, all four, reverse order — local dev reset only
make migrate-create service=course name=add_something
```

`make migrate-up`'s order is fixed and matters: `platform` first (its migration installs `citext`/`pg_trgm`, which `identity` and `course` need), then `identity`, `course`, `analytics` — `analytics` last since nothing else depends on it. Each service is tracked by its own `x-migrations-table` (e.g. `course_schema_migrations`) so the four independent sequences don't collide on a single shared `schema_migrations` row.

Under the hood these call `golang-migrate`'s CLI via `go run`, against `$POSTGRES_DSN` (see `.env.example`). Migrations run as an explicit step, never automatically on service start (RFC §5.8) — an API replica starting mid-rolling-restart must not race another replica to alter a table.

Test code (`test/integration/testenv`) runs the same four migration sets, in the same order, against each test's own ephemeral `testcontainers-go` Postgres — see that package for why tests always apply every service's schema regardless of which module is under test (fixtures commonly span schemas).

## Rules for adding a migration

- Every migration has a tested `.down.sql`. If a table can't be cleanly dropped (e.g. it has dependents), the down migration should say so via a comment, not silently no-op.
- Never edit a migration that has already been applied anywhere outside your own local database. Add a new one.
- Constraints (`CHECK`, partial unique indexes, foreign keys) belong in the migration, not only in application code — see `idx_one_active_enrollment` and `idx_one_draft_per_course` for why: these are invariants the database enforces so a race in application code can't violate them. Foreign keys only within a service's own schema (ADR-0024) — never across one.
- A migration's first statement (after `CREATE SCHEMA IF NOT EXISTS <schema>`, only needed in a service's first-ever migration) should be `SET search_path TO <schema>, public;` — every file in this directory follows this, so table names stay unqualified and match what `internal/*/repository.go` actually queries.
