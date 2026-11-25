# libs/testenv

Real Postgres, Redis, Kafka and MongoDB containers for integration tests (testcontainers-go, Docker required). No mocks for the database: constraint violations, transactions and grants are what the tests check.

## Postgres: `Setup`, `Env.DB`, `Env.ServiceDB`

```go
env := testenv.Setup(t)                  // fresh Postgres + Redis, roles + all migrations applied
svc := env.ServiceDB(t, "course")        // connected as svc_course, search_path "course,public"
env.Owner / env.DB                       // the OWNER connection (the one that runs migrations)
env.ServiceDSN(t, "course")              // the role's DSN, for a binary or your own pool
testenv.Qualify("course", "outbox")      // "course"."outbox", for owner-side SQL
env.OpenWithSearchPath(t, "course,public") // an owner pool with an explicit search_path
```

`Setup` does what `make migrate-up` does: starts `postgres:16-alpine`, runs the real `scripts/db-roles.sh` inside the container (creating `svc_identity`, `svc_course`, `svc_analytics`, `svc_notification`), applies `platform identity course analytics notification cutover` from `migrations/`, runs the roles script again (proving it is idempotent), and returns the clients. Each call gets its own containers.

- **`ServiceDB(t, "identity"|"course"|"analytics"|"notification")`** is what service tests should use. It is the connection a real binary will have: its own role, its own schema first on the path, `permission denied` anywhere else, no DDL. `libs/outbox`, `libs/relay` and `libs/idempotency` use unqualified `outbox`, `processed_events` and `idempotency_keys`, and with this connection those resolve to the service's own tables.
- **`env.DB`** (also `env.Owner`) is the owner. Use it for DDL fixtures and cross-schema assertions, always with qualified names. A test that wants its code under test to run as a service can replace `env.DB` with `ServiceDB(...)` and keep `env.Owner` (the `setupCourse` helpers in `libs/relay`, `libs/outbox` and `libs/idempotency` do this).
- Test role passwords are `pw-<service>`; they exist only in the throwaway container.
- The `platform` schema no longer exists after `Setup` (the `cutover` set drops it). To test against the old layout or the cutover itself, see `cutover_integration_test.go`, which stops before `cutover`, seeds `platform`, and runs it.

## Name-collision hazard (read this)

`outbox` exists in `identity` and `course`; `processed_events` in `course`, `analytics` and `notification`; `idempotency_keys` in `course`. A search path that lists several service schemas resolves such a name to the **first** schema that has it, without an error, so a test (or a worker) could read and write the wrong service's outbox and still go green.

Decision: **`env.DB` has the search path `public` only** (`testenv.OwnerSearchPath`). Every service schema has at least one colliding table name, so no combination of service schemas is safe; with none on the path, an unqualified `outbox`, `processed_events` or `idempotency_keys` (indeed any service table) fails with `relation "..." does not exist` instead of silently using the wrong schema. Owner-side SQL qualifies the name (`course.outbox`, or `testenv.Qualify`). Service pools have exactly `<their schema>,public` and cannot see the others anyway (`TestRolesCannotReachOtherSchemasByName` covers a role whose path is pointed at another schema).

The rule for real binaries is the same: one schema on the path, never a combined one. The old combined worker path `platform,course,analytics,public` is gone with the split.

Existing tests that used `env.DB` with unqualified service tables will fail with `relation does not exist` after this change; move them to `env.ServiceDB(t, "<service>")`.

## Isolation tests (what keeps the split honest)

`isolation_integration_test.go` (build tag `integration`):

- `TestRolesCannotTouchOtherServiceSchemas`: a table of every role against every OTHER service's schema and its tables (including each `outbox` and `processed_events`): `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` and `CREATE TABLE` must all return `permission denied`. One row reads "identity cannot SELECT course.courses".
- `TestRolesCannotReachOtherSchemasByName`: the owner cannot resolve the three colliding names unqualified, and a role with another service's schema on its path still cannot read it.
- `TestEachRoleCanDoItsOwnWork`: each role writes and reads back its own `outbox` / `processed_events` inside a transaction, uses `citext` and `pg_trgm`, runs DML, cannot run DDL, and can use a table the owner creates after the grants (default privileges) while the other roles cannot.

`cutover_integration_test.go`: the `platform` to per-service move (ids, order, event ids, payloads and routing by consumer), the fresh-database case, the cutover's down path and full teardown, and the down path of each new per-service migration.

These tests live here because this is the only module that owns the container, the migrations directory and the roles script together; a separate tests-only module would need its own copy of that wiring and another entry in the module graph.

## Other helpers

`SetupKafka` (`kafka.go`, `apache/kafka` with `Kill`/`Start`), `SetupMongo` (`mongo.go`). See their doc comments.
