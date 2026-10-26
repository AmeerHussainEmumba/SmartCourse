# Migrations

Schema lives here as versioned SQL, applied by [`golang-migrate`](https://github.com/golang-migrate/migrate) — not GORM `AutoMigrate` (ADR §19.15: `AutoMigrate` can't express partial unique indexes, check constraints, or enums, all of which enforce real invariants in this schema, e.g. `idx_one_active_enrollment`, `idx_one_draft_per_course`).

## What's here

| File | Owns |
|---|---|
| `000001_extensions` | `citext`, `pg_trgm` |
| `000002_identity` | `users`, `refresh_tokens`, `password_reset_tokens`, `email_verification_tokens` |
| `000003_courses` | `courses`, `course_versions`, `course_prerequisites` |
| `000004_content` | `modules`, `lessons` |
| `000005_enrollment` | `enrollments`, `lesson_progress`, `certificates` |
| `000006_platform` | `outbox`, `processed_events`, `idempotency_keys` |
| `000007_read_models` | `course_search_index`, `analytics_*` |

The full schema is created in one pass (M0), not grown weekly — see `docs/rfc/smartcourse-rfc.md` §21 for why: the outbox, idempotency keys, and `lesson_key` are correctness requirements of write paths built on top of them, not features to bolt on later.

## Running migrations

```
make migrate-up            # apply all pending migrations
make migrate-down          # roll back one migration
make migrate-create name=add_something   # scaffold a new up/down pair
```

Under the hood these call `golang-migrate`'s CLI via `go run`, against `$POSTGRES_DSN` (see `.env.example`). Migrations run as an explicit step, never automatically on service start (RFC §5.8) — an API replica starting mid-rolling-restart must not race another replica to alter a table.

## Rules for adding a migration

- Every migration has a tested `.down.sql`. If a table can't be cleanly dropped (e.g. it has dependents), the down migration should say so via a comment, not silently no-op.
- Never edit a migration that has already been applied anywhere outside your own local database. Add a new one.
- Constraints (`CHECK`, partial unique indexes, foreign keys) belong in the migration, not only in application code — see `idx_one_active_enrollment` and `idx_one_draft_per_course` for why: these are invariants the database enforces so a race in application code can't violate them.
