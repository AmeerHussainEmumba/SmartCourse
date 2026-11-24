# libs/config

Fail-fast environment configuration (RFC §11.8). `Load()` reads every `cmd/*` binary's settings from the environment and validates them before anything else runs.

## Why this exists as its own package

"Fail-fast" means two specific things here, both load-bearing enough to be worth stating explicitly rather than left implicit in the code:

- **No secret has a default.** `JWT_SIGNING_KEY`, `POSTGRES_DSN`, `REDIS_ADDR`, and the rest of the `req()`-wrapped vars have no fallback value — a missing one fails boot, not first use. The alternative (silently falling back to a development constant) is exactly the kind of mistake that reaches production unnoticed.
- **Every error is collected, not just the first.** `Load()` doesn't return on the first missing/malformed var; it appends every validation failure into one slice and returns them together. An operator fixing a broken `.env` sees every problem in one run instead of fixing one var, rerunning, hitting the next one, repeating.

`PostgresConfig.SearchPath` and `TemporalConfig` are the two fields worth knowing about if you're new to this codebase: the former is schema-per-service's connection-level `search_path` (ADR-0024, differs per binary); the latter is all-optional with working defaults, deliberately — see `TemporalConfig`'s own doc comment for why requiring it would be disproportionate (only `course-api`/`worker` ever use it, and only at the moment of an actual publish, not at boot — ADR-0025).

## Argon2 settings

`ARGON2_MEMORY_KIB` (8 to 1048576), `ARGON2_ITERATIONS` (1 to 100) and `ARGON2_PARALLELISM` (1 to 64) set the hash cost. `ARGON2_MAX_CONCURRENT` (1 to 1024; default `clamp(GOMAXPROCS / parallelism, 2, 8)`) bounds how many hashes run at once, so peak hashing memory is about `ARGON2_MAX_CONCURRENT x ARGON2_MEMORY_KIB`; `ARGON2_QUEUE_TIMEOUT` (default 5s, up to 1m) is how long a request waits for a slot before identity answers 503. The gate itself is `libs/auth.ArgonGate`.

## Relay pool sizing

`SMTP_*` (`smtp.go`) points the notification dispatcher at a mail server: `SMTP_HOST` unset means no provider (deliveries stay pending), `SMTP_PORT`, `SMTP_FROM`, optional `SMTP_STARTTLS`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_ALLOW_INSECURE_AUTH` (credentials are refused over a cleartext link unless explicitly allowed) and the connect and send timeouts. The local stack uses Mailpit with no auth. `ASYNQ_*`, `OUTBOX_RETENTION` and `JOBS_*_INTERVAL` configure the worker's Asynq pools, retry policy and scheduled jobs (`asynq.go`; names, defaults and ranges are in `env.example`). Intervals are whole minutes dividing 60 or whole hours dividing 24, because every worker replica must derive the same cron tick (`CronSpec`). `Load` does not check the Asynq pool sizes against `POSTGRES_MAX_OPEN_CONNS`: which handlers use the database is the worker's wiring (RFC §10.2).

`RELAY_WORKERS` (default 4) and `POSTGRES_MAX_OPEN_CONNS` (default 20) are checked together: the pool must be at least twice the worker count, because each relay worker keeps its claim transaction open while its handlers open their own (ADR-0027). A smaller pool is a startup error rather than a warning because it does not merely slow the relay down: claims can hold every connection while the handlers wait, their attempts are burned and events are eventually given up on. Twice the workers is the floor; the worker's Temporal activities share the pool, so leave headroom.

## Testing

`config_test.go` (no build tag — pure env-var logic, no infra) verifies the fail-fast behavior directly: every required var missing individually and in combination (confirming the aggregation, not just that *an* error comes back), `JWT_SIGNING_KEY`'s length check, and that a malformed optional var (an int/bool/duration that fails to parse) is rejected rather than silently falling back to its default.
