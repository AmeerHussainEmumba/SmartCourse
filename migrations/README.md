# Migrations

Schema lives here as versioned SQL, applied by [`golang-migrate`](https://github.com/golang-migrate/migrate) — not GORM `AutoMigrate` (ADR §19.15: `AutoMigrate` can't express partial unique indexes, check constraints, or enums, all of which enforce real invariants in this schema, e.g. `idx_one_active_enrollment`, `idx_one_draft_per_course`), and not Liquibase (`docs/rfc/differences-from-requirements.md` DR-010/DR-013 — confirmed not a hard requirement).

## Schema-per-service (ADR-0024)

Six migration sets: one per Postgres schema of the four services that use Postgres, the `platform` set for the extensions, and `cutover` (below). Publishing and audit have no Postgres tables (audit's store is MongoDB). All of it is still one database; each service connects with its own role (ADR-0024, ADR-0033):

| Directory | Schema | Owns |
|---|---|---|
| `platform/` | (none left) | `000001` installs the `citext`/`pg_trgm` extensions into `public` (stays; this is all `platform/` provides now). `000002`/`000003` are history, never edited: on a fresh database they still create `platform.outbox` (with `trace_id`, `span_id`, `actor_id`), `platform.processed_events` and `platform.idempotency_keys`, and `cutover/` then moves their rows and drops them, so after `make migrate-up` **no `platform` schema exists**. |
| `identity/` | `identity` | `users`, `refresh_tokens`, `password_reset_tokens`, `email_verification_tokens`, and (`000002_outbox`) `outbox`. |
| `course/` | `course` | `courses`, `course_versions`, `course_prerequisites`, `modules`, `lessons`, `enrollments`, `lesson_progress`, `certificates`, `course_search_index`, and (`000008_outbox_inbox_idempotency`) `outbox`, `processed_events`, `idempotency_keys` (`000009` adds `response_raw` and `response_headers` for byte-exact replay) — course, enrollment, and progress share one schema because enrollment locks the `courses` row in the same transaction it writes an enrollment row in (ADR-0012's seat-limit guarantee); `course_search_index` lives here because indexing and search are part of the course service's publish activities (ADR-0025). |
| `analytics/` | `analytics` | Three mirror tables (`user_role_mirror`, `course_mirror`, `enrollment_mirror`, fed from Kafka events) plus the aggregate read models (`platform_snapshot`, `course_stats`, `daily_enrollments`), the workflow-issue tables (`publish_failures`, `workflow_issue_stats`, `000004`), and (`000005_processed_events`) `processed_events`. |
| `notification/` | `notification` | `recipients`, `course_titles`, `enrollment_courses`, `deliveries`, and (`000003_processed_events`) `processed_events`. |
| `cutover/` | (none) | Runs LAST. `000001_split_platform` moves the old shared `platform` rows to the owning service and drops `platform` (below). |

## Per-service integration tables and the cutover

Every service that writes events owns an `outbox`, and every service that consumes events owns a `processed_events`, in its own schema, with the shape the shared `platform` tables had (so `libs/outbox`, `libs/relay` and `libs/idempotency` keep using unqualified names and the service's `search_path` picks the table): `identity.outbox`; `course.outbox`, `course.processed_events`, `course.idempotency_keys`; `analytics.processed_events`; `notification.processed_events`. (`docs/plans/microservices-split.md` section 3.1.)

`cutover/000001_split_platform` is one multi-statement migration (it commits or rolls back as a unit) that:

- moves **every** `platform.outbox` row (published and unpublished; the table is pruned, so what is left is the recent history) into `identity.outbox` when `event_type` starts with `identity.` and into `course.outbox` otherwise, copying `id` and `event_id` and inserting in `id` order, then advances each target's id sequence past the highest copied id;
- moves `platform.processed_events` by `consumer_name`: `analytics-aggregator` to analytics, `notification-dispatcher` to notification, `audit-archiver` is **dropped** (the audit service dedups by event id in MongoDB), every other consumer (`course-instructor-mirror`, `certificate-issuer`, `enrollment-course-archival`, anything new) to course. The map comes from the consumer-name constants in `services/*`; a consumer that changes service later needs this map revisited before the cutover runs;
- moves `platform.idempotency_keys` to `course.idempotency_keys`;
- drops the three platform tables and the `platform` schema (`DROP SCHEMA ... RESTRICT`, so an unexpected object in it fails the migration instead of vanishing). `citext` and `pg_trgm` stay in `public`.

It is idempotent (guards on `to_regclass`, `ON CONFLICT DO NOTHING`) and on a fresh database it moves nothing. **Run it when nothing writes to `platform`** (stop the old binaries first): rows written after it ran would have no table. Its `down` recreates an **empty** `platform` schema and tables (including the trace/actor columns) so `platform`'s own down migrations still run; **rows do not move back**. Treat the down as a local-dev reset, not a production rollback. `libs/testenv/cutover_integration_test.go` proves the move, ids, order and event ids, routing, the fresh-database case and the down path.

## Per-service Postgres roles

`scripts/db-roles.sh` (called by `make migrate-up` after the migrations, by `make db-roles`, and by the compose postgres init the first time the volume is created) creates `svc_identity`, `svc_course`, `svc_analytics`, `svc_notification`: `LOGIN`, no superuser, owning nothing, `USAGE` and DML on their own schema's tables and sequences only (and on tables the owner creates later, through default privileges), `EXECUTE` on the `public` functions (`citext`, `pg_trgm`), a `CONNECTION LIMIT` (`SVC_<X>_CONN_LIMIT`), and a role-level `search_path` of `<schema>, public`. `REVOKE ALL ON SCHEMA <service schema> FROM PUBLIC` for every service schema. Passwords come from `SVC_<X>_PASSWORD` in the environment (dev values in `env.example`); no migration carries a password. The roles do not run migrations: the owner (`POSTGRES_DSN`) does, and must be the same login that runs the script so the default privileges apply to the tables it creates. `libs/testenv` (`TestRolesCannotTouchOtherServiceSchemas` and friends) fails if any role can reach another service's schema.

## Warning: the name-collision hazard

`outbox`, `processed_events` and `idempotency_keys` now exist in more than one schema. Postgres resolves an unqualified name to the **first** schema on the `search_path` that has it, silently. A connection with a combined path (the retired shared-worker path `platform,course,analytics,public` is the example) would read and write the wrong service's tables without any error. Every binary and every test connection must have exactly one service schema on its path (`<schema>,public`); owner-side SQL qualifies the name. `libs/testenv` makes `Env.DB` (the owner) use `public` only so unqualified use fails loudly; see its README.

**No foreign key crosses a schema boundary.** `courses.instructor_id`, `enrollments.student_id`, and `idempotency_keys.user_id` are plain `UUID` columns, not `REFERENCES identity.users(id)` — none of them were ever read across that boundary by application code (callers are authorized via JWT claims), and there's no user-delete feature, so nothing relied on the database catching a dangling reference. See ADR-0024 for the full reasoning, including why `platform` is a deliberate exception (it's integration infrastructure, not domain data).

Each service's connection sets `search_path` (`POSTGRES_SEARCH_PATH`, wired through `libs/config` → `libs/db.Open`) so bare table names in `services/*/*/repository.go` resolve to the right schema without every query being hand-qualified — see `docker-compose.yml` and `Makefile`'s `run-*` targets for the exact value each binary uses.

## Running migrations

```
make migrate-up                              # applies all six sets in order, then creates the DB roles (see below)
make db-roles                                # (re)creates the per-service roles and grants only
make migrate-down service=course             # rolls back one migration for one service
make migrate-down-all                        # full teardown, all sets, reverse order (the roles stay) — local dev reset only
make migrate-create service=course name=add_something
```

`make migrate-up`'s order is fixed and matters: `platform` first (its migration installs `citext`/`pg_trgm`, which `identity` and `course` need), then `identity`, `course`, `analytics`, `notification`, and `cutover` last (it needs every service's own tables to exist). Order everywhere: `platform identity course analytics notification cutover`. Each service is tracked by its own `x-migrations-table` (e.g. `course_schema_migrations`) so the independent sequences don't collide on a single shared `schema_migrations` row.

Under the hood these call `golang-migrate`'s CLI via `go run`, against `$POSTGRES_DSN` (see `.env.example`). Migrations run as an explicit step, never automatically on service start (RFC §5.8) — an API replica starting mid-rolling-restart must not race another replica to alter a table.

Test code (`libs/testenv`) runs the same six migration sets (and the roles script), in the same order, against each test's own ephemeral `testcontainers-go` Postgres — see that package for why tests always apply every service's schema regardless of which module is under test (fixtures commonly span schemas).

## Rules for adding a migration

- Every migration has a tested `.down.sql`. If a table can't be cleanly dropped (e.g. it has dependents), the down migration should say so via a comment, not silently no-op.
- Never edit a migration that has already been applied anywhere outside your own local database. Add a new one.
- Constraints (`CHECK`, partial unique indexes, foreign keys) belong in the migration, not only in application code — see `idx_one_active_enrollment` and `idx_one_draft_per_course` for why: these are invariants the database enforces so a race in application code can't violate them. Foreign keys only within a service's own schema (ADR-0024) — never across one.
- A migration's first statement (after `CREATE SCHEMA IF NOT EXISTS <schema>`, only needed in a service's first-ever migration) should be `SET search_path TO <schema>, public;` — every file in this directory follows this, so table names stay unqualified and match what `services/*/*/repository.go` actually queries.
