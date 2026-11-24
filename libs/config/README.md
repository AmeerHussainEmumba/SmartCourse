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

## Event transport

`EVENT_TRANSPORT` selects how outbox events reach their handlers (ADR-0029): `inprocess` (default; the relay runs the handlers) or `kafka` (the relay publishes to Kafka and consumer groups run them). Any other value is a startup error, so a typo cannot silently leave events off the broker. In `kafka` mode `KAFKA_BROKERS` (comma-separated host:port) and `SCHEMA_REGISTRY_URL` are required and checked for shape; in `inprocess` mode they are ignored. The consumer tuning (`KAFKA_CONSUMER_MAX_IN_FLIGHT`, `..._MAX_ATTEMPTS`, `..._BACKOFF_BASE`, `..._BACKOFF_MAX`, `..._SHUTDOWN_TIMEOUT`) and `KAFKA_PRODUCE_TIMEOUT` have defaults and are range-checked in every mode. `KAFKA_PRODUCE_TIMEOUT` is capped at 25s because the relay gives one row 30s to claim, publish and record the result. `KAFKA_CONSUMER_MAX_IN_FLIGHT` is per consumer group and each running handler holds a Postgres connection, so the worst case is groups x that value; the pool queues the excess rather than failing, and `Load` does not check it against `POSTGRES_MAX_OPEN_CONNS`.
## Telemetry (Phase 7)

All optional; validated at `Load`, never dialled (an unreachable collector must not stop a service).

| Variable | Default | Meaning |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty (tracing off) | OTLP/HTTP collector, `host:port` or an `http(s)` URL without credentials. |
| `OTEL_SDK_DISABLED` | `false` | `true` turns tracing off even with an endpoint. |
| `OTEL_SERVICE_NAME` | the binary's own name | `service.name`; no whitespace. |
| `OTEL_TRACES_SAMPLER_ARG` | `1` | Head-sampling ratio from 0 to 1. |
| `METRICS_ENABLED` | `true` | Off: no `/metrics`, no collectors, no request metrics. |
| `METRICS_HTTP_ADDR` | `:9100` | The worker's own metrics listener (`host:port`); the HTTP services serve `/metrics` on their API port. |
