# internal/platform/db

The PostgreSQL connection (RFC §5.1 — Postgres is the single system of record). GORM for single-entity CRUD and simple associations; modules needing non-trivial joins or window functions use `db.Raw`/`db.Exec` directly rather than GORM associations (RFC §5.9, `differences-from-requirements.md` DR-004).

## What `Open` does beyond `gorm.Open`

- **Sizes the connection pool against the configured budget** (`MaxOpenConns`/`MaxIdleConns`, RFC §13.2) — pool sizing is a hard rule here, not a tuning afterthought: `MaxOpenConns × replicas` has to stay under Postgres's `max_connections` with headroom for migrations and operators.
- **Applies `search_path` as a DSN connection parameter, not a post-connect `SET`** (`dsnWithSearchPath`, ADR-0024) — a pool opens multiple physical connections, and `SET search_path` only affects the one connection that ran it. Putting it in the DSN makes pgx (the driver underneath `gorm.io/driver/postgres`) apply it as a startup parameter on every connection the pool opens, pooled or not. This is schema-per-service's actual enforcement mechanism: every `cmd/*` binary's `POSTGRES_SEARCH_PATH` env var flows through here.
- **Disables GORM's `AutoMigrate`** (`DisableForeignKeyConstraintWhenMigrating: true`) — the schema is owned entirely by `migrations/`, never inferred from Go structs.
- **Pings on open** with a bounded timeout, so a misconfigured DSN fails at boot with a clear error rather than surfacing as a mysterious failure on the first real query.

`Ping(ctx, gdb)` is the separate, lightweight function `/health/ready` calls on every request (`internal/platform/router`) — never a query against a real table, just a connectivity check.

## Testing

No dedicated test file — `Open`'s behavior (pool sizing, `search_path` injection, AutoMigrate disabled) is exercised indirectly by every integration test in the repo, all of which connect through it via `test/integration/testenv`.
