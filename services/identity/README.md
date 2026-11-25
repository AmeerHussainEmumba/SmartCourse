# services/identity

SmartCourse's identity service: users, credentials, roles and every token type that authenticates a request. One Go module, three binaries, one Postgres schema. It imports only `libs/*` (`docs/plans/microservices-split.md`); no other service imports it.

| Binary | What it is | Image |
|---|---|---|
| `identity-api` (`cmd/identity-api`) | The public HTTP API (`/api/v1/auth/*`, `/me`, `/admin/users/{id}/role`, public health routes). | `Dockerfile.identity-api` |
| `identity-worker` (`cmd/identity-worker`) | The identity outbox relay (publish-only), the token-version re-seeder, housekeeping jobs. | `Dockerfile.identity-worker` |
| `identity-admin` (`cmd/identity-admin`) | CLI that creates the first admin. Built into the identity-api image (`/usr/local/bin/identity-admin`). | `Dockerfile.identity-api` |

Every long-running binary is `libs/runtime.Main`: config, Postgres and Redis, telemetry, signals and graceful shutdown. `/metrics`, `/health/live` and `/health/ready` are on an internal listener (`METRICS_HTTP_ADDR`, default `:9100`); `/metrics` is never on the public API port.

## Data

Schema `identity` (migrations `migrations/identity`): `users`, `refresh_tokens`, `password_reset_tokens`, `email_verification_tokens`, `outbox` (`000002_outbox`). No `processed_events` (identity consumes nothing) and no idempotency table. The binaries connect as the Postgres role `svc_identity` with `POSTGRES_SEARCH_PATH=identity,public`; the role cannot touch any other service's schema, and the unqualified `outbox` resolves to `identity.outbox`. Other services never read these tables: they take the caller from the JWT claims and keep event-fed mirrors.

## What it produces (Kafka, via the outbox)

`identity.user_registered`, `identity.user_role_changed`, `identity.user_profile_updated` (written in the same transaction as the change; published by `identity-worker`). Consumers (course's instructor mirror, for example) are other services' concern.

## identity-worker components

Each is independent: one failing is logged and shown as `component:<name>: failed` in `/health/ready` (status `degraded`) and never stops the others (`libs/runtime`).

- `relay`: `libs/relay` with a publisher and no handlers, over `identity.outbox`. Topics and schemas are prepared with `libs/eventbus` (`Prepare`); the relay waits for Kafka with capped backoff, so the other components do not.
- `token-version-reseeder`: after a Redis flush, re-writes every `token_version` above 1 (`SET NX`, sentinel-guarded, safe on several replicas) so other services stop answering `token_stale` (F-38). It used to run inside `identity-api`.
- `jobs`: the Asynq scheduler and servers (pool sizes from `ASYNQ_*`); `PruneOutbox` only (`JOBS_OUTBOX_PRUNE_INTERVAL`, `OUTBOX_RETENTION`).

Readiness: Postgres and Redis are fatal; Kafka is non-fatal (the outbox buffers an outage). Needs `KAFKA_BROKERS` and `SCHEMA_REGISTRY_URL`.

## identity-admin

```
IDENTITY_ADMIN_EMAIL=admin@example.com IDENTITY_ADMIN_PASSWORD='...' \
  docker compose run --rm -e IDENTITY_ADMIN_EMAIL -e IDENTITY_ADMIN_PASSWORD identity-api identity-admin
```

Creates the first admin through the service layer (`Service.BootstrapAdmin`): the password is validated by the registration rules and hashed with the configured Argon2 settings; one transaction writes the user, `identity.user_registered`, the role change and `identity.user_role_changed`. Idempotent: an existing account with that email is left untouched and the command exits 0. Never prints the password or hash. Exit codes: 0 created or exists, 1 failure, 2 missing or invalid input. `tools/seed` and the E2E harness call it before they use the HTTP API.

## Run and test

```
go run ./services/identity/cmd/identity-api      # needs POSTGRES_DSN (svc_identity), REDIS_ADDR, JWT_SIGNING_KEY, ...
go run ./services/identity/cmd/identity-worker   # also KAFKA_BROKERS, SCHEMA_REGISTRY_URL
go test ./...                                    # unit
go test -race -tags=integration -count=1 ./...   # Docker: Postgres and Redis containers, service-role connections
```

Integration tests use `env.ServiceDB(t, "identity")` (the connection a binary has). The Kafka producer is replaced by a recording publisher in the worker's wiring tests; a live broker is covered by `libs/eventbus` and the container E2E harness, not here. Module docs: `identity/README.md` (the domain), `cmd/*/README.md` (each binary).
