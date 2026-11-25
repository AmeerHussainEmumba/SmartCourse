# The local stack

How `docker-compose.yml` runs the ten-binary topology (`docs/plans/microservices-split.md` sections 2 and 6) on one machine: profiles, ports, per-service Postgres roles, memory and known issues. Everything is published on `127.0.0.1` only and uses local-dev placeholder credentials.

## Profiles

Profiles are cumulative (ADR-0018). Each `make` target maps to one `docker compose --profile <name> up -d`.

| Profile / target | Contains |
|---|---|
| `core` / `make dev-core` | postgres, redis, minio + minio-init, mailpit, kafka + kafka-init, schema-registry + schema-init, mongodb, swagger-ui, and nine binaries: identity-api, identity-worker, course-api, course-worker, analytics-api, analytics-worker, notification-worker, audit-api, audit-worker |
| `workflow` / `make dev-workflow` | `core` + temporal, temporal-ui, publishing-worker |
| `observability` / `make dev-observability` | `workflow` + prometheus, grafana, jaeger (every binary exports OTLP to `jaeger:4318`; the exporter just drops spans when Jaeger is not running) |
| `full` / `make dev-full` | everything |

**Course publishing needs `workflow`.** `publishing-worker` is not in `core`: on plain `core`, `POST /courses/{id}/publish` starts a Temporal workflow that nothing executes (and without Temporal the call answers 503). `make dev-events` is a deprecated alias of `dev-core` (Kafka, the registry and MongoDB moved into `core`). `make dev-down` stops every profile; `docker compose down -v` also deletes the data volumes (`pgdata`, `miniodata`).

`make dev-core UP_FLAGS=--build` rebuilds the images (plain `up -d` only builds the ones that do not exist yet).

## First run

```bash
make stack-up        # postgres + registry, migrate-up (+ roles), register-schemas, every service, then identity-admin
# or by hand:
make dev-workflow && make migrate-up && make register-schemas
docker compose run --rm --no-deps --entrypoint identity-admin \
  -e IDENTITY_ADMIN_EMAIL=admin@smartcourse.local -e IDENTITY_ADMIN_PASSWORD='...' identity-api
```

`make stack-up` creates the admin only when `IDENTITY_ADMIN_EMAIL` and `IDENTITY_ADMIN_PASSWORD` are set (environment or `.env`; the password follows the registration rules, 8 to 128 characters). It uses the published ports with the dev DSN unless `.env` overrides `POSTGRES_DSN`, and is safe to repeat. `STACK_PROFILE=core` limits it to `core`. Migrations never run on container start (RFC 5.8): the workers retry a few times (`restart: on-failure:5`) when started before `migrate-up`, so run it promptly or use `stack-up`, which migrates before starting them.

The `identity-admin` binary ships in the identity-api image; `--entrypoint identity-admin` is needed because the image's entrypoint is `identity-api` (the command in `env.example` omits it).

## Ports (host, 127.0.0.1)

| Port | What | Port | What |
|---|---|---|---|
| 8090 | identity-api | 9101 | identity-api internal listener |
| 8091 | course-api | 9102 | course-api internal listener |
| 8092 | analytics-api | 9103 | analytics-api internal listener |
| 8093 | audit-api | 9104 | audit-api internal listener |
| 8089 | Swagger UI | 9111 | identity-worker internal listener |
| 8088 | Temporal UI | 9112 | course-worker internal listener |
| 8025 / 1025 | Mailpit web+API / SMTP | 9113 | analytics-worker internal listener |
| 8081 | Schema Registry | 9114 | notification-worker internal listener |
| 5433 / 6380 | Postgres / Redis | 9115 | publishing-worker internal listener |
| 9002 / 9003 | MinIO S3 / console | 9116 | audit-worker internal listener |
| 29092 | Kafka (host listener) | 7233 | Temporal |
| 27017 | MongoDB | 9090 / 3001 / 16686 | Prometheus / Grafana / Jaeger UI |

APIs listen on 8080 inside their container. The internal listener (container port 9100 in every binary) serves `/metrics`, `/health/live` and `/health/ready`; `/metrics` is never on a public port (checked: 404 on 8090 to 8093). The containers' healthchecks probe `/health/ready` there, and `depends_on` uses them: services wait for Postgres and Redis (healthy), the workers also for `kafka-init` and `schema-init` (completed), audit for MongoDB, notification-worker for Mailpit, course-api for `minio-init`.

## Per-service Postgres roles

Each binary with Postgres connects with its own role and `POSTGRES_SEARCH_PATH=<schema>,public` (ADR-0024, split plan 3.2). The roles come from `scripts/db-roles.sh`, which the postgres container runs on the first start of an empty volume (so a fresh stack has them before any migration) and `make migrate-up` / `make db-roles` re-run.

| Binaries | Role | Search path |
|---|---|---|
| identity-api, identity-worker, identity-admin | `svc_identity` | `identity,public` |
| course-api, course-worker | `svc_course` | `course,public` |
| analytics-api, analytics-worker | `svc_analytics` | `analytics,public` |
| notification-worker | `svc_notification` | `notification,public` |
| publishing-worker, audit-api, audit-worker | none (no Postgres) | |

The dev passwords default to the values in `env.example` (`svc_<x>_dev`); the compose file reads `SVC_<X>_PASSWORD` from `.env` or the shell if you change them, for the postgres init and the DSNs alike (change all or none). Temporal's own databases use the owner login `smartcourse`.

## Memory

The default Docker Desktop VM is about 8 GB (`docker info` here: 7.75 GiB). Measured with `docker stats --no-stream` on 2026-11-25 (Docker 29.5.3, apple silicon VM, images already built):

| Moment | Containers | Sum of container memory |
|---|---|---|
| `workflow` profile, a minute after the flow below | 20 running | about 2.6 GB |
| `workflow` + Prometheus, Grafana, Jaeger, a few minutes later | 23 | about 2.6 GB (Grafana alone about 620 MiB; the Go services had settled to 15 to 150 MiB each) |

Largest consumers (second snapshot): Kafka about 405 to 490 MiB, Schema Registry about 340 MiB, Temporal about 280 MiB, MongoDB 95 to 285 MiB, Postgres about 200 MiB, MinIO about 140 MiB, identity-api about 150 MiB (argon2 hashing), Grafana about 620 MiB. Every Go binary stayed under 150 MiB.

Tuning in `docker-compose.yml`:

- **Kafka**: `KAFKA_HEAP_OPTS=-Xms256m -Xmx512m` (the image default is a 1 GiB heap). Measured: a default-heap broker idle for 40 s used 340 MiB; the capped broker carrying the whole flow used 405 to 490 MiB, i.e. the cap bounds the heap at 512 MiB while the broker grows with load instead of reserving 1 GiB. Not measured: a sustained load test, so raise the cap in a `docker-compose.override.yml` for that. `kafka-init` gets a 128 MiB heap (it only lists and creates topics).
- **Schema Registry**: `SCHEMA_REGISTRY_HEAP_OPTS=-Xms128m -Xmx256m`; it settled at about 330 to 340 MiB resident (JVM overhead on top of the heap).
- **MongoDB**: `--wiredTigerCacheSizeGB 0.25` (the default cache is half of RAM minus 1 GiB).
- Redis, Postgres, Temporal, MinIO: defaults.

## What was verified (2026-11-25)

A disposable project (`COMPOSE_PROJECT_NAME=sc-split`, fresh volumes, removed with `down -v`; the then-pinned official MinIO images could not be pulled, so a scratch override pointed `minio` and `minio-init` at locally cached `latest` images; since replaced by the `pgsty/minio` fork, see "Object store image" below): `make stack-up` took 46 s with every image and the Go module cache already built and the `migrate` CLI cached. It started the `workflow` profile and all long-running containers reported healthy. The roles existed, the six migration sets applied, 15 schema subjects registered, `identity-admin` created the first admin. All ten internal listeners answered 200 on `/health/ready` and `/metrics`, and the four public API ports answered 404 on `/metrics`. Then over HTTP: admin login, two registrations, role change to instructor, a course with a module and a lesson, publish (202) and `ready` about 1 s later through Temporal and publishing-worker, an enrollment (201). Afterwards: `identity.outbox` 5 of 5 and `course.outbox` 2 of 2 rows published (max attempts 0); topics `identity.events`, `course.events`, `enrollment.events`, `progress.events` and the six `retry.*` topics existed; Mailpit held the welcome mails and "You are enrolled in Proof of Stack"; `GET /analytics/platform` and the course stats showed 1 active enrollment; MongoDB `events` held 7 archived events (identity.user_registered 3, identity.user_role_changed 2, course.course_published, enrollment.student_enrolled) and `audit-api` listed no failed events; Prometheus showed all ten targets UP, all 9 rules loaded, and the dashboard expressions returned data. A second round (three more students enrolling) was run after the Redis change below: 10 mails sent, no `handler not found`.

Not verified: a cold run including image builds and the Go module download; Windows or Linux hosts; Docker VMs smaller than 8 GB; behaviour under load; Kafka or MongoDB outages on this stack; `make stack-up` with `STACK_PROFILE=core`; the e2e harness.

## Object store image

`minio` runs `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` and `minio-init` runs `pgsty/mc:RELEASE.2026-11-12T00-00-00Z` (both pinned by tag and digest in `docker-compose.yml`): a third-party community fork of MinIO, AGPLv3, used because the official `minio/minio` and `minio/mc` images can no longer be pulled on a fresh machine. Same ports (9002 S3, 9003 console), same `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`, same `/minio/health/live`. Verified 2026-11-25 on a disposable project (`sc-minio`): bucket created by `minio-init`, pre-signed PUT, a lesson referencing the asset, publish to `ready`, and a missing or wrong asset failing the publish. Risk and revert steps: ADR-0019 amendment.

## Known issues

- **Asynq queues were shared between workers (fixed: per-service queue namespace).** identity-worker, course-worker, analytics-worker and notification-worker used the same queue names (`critical`, `default`, `low`) in one Redis database, so a worker dequeued other services' tasks and archived them (`handler not found for task "notification:deliver"`, a mail delivery left `pending`). Each worker now prefixes its queues with its service name (`identity:low`, `notification:critical`, ...; `libs/asynqx` `Namespace`), so all services share Redis database 0 again and the compose file has no per-worker `REDIS_DB`. The `queue` label of `asynq_queue_depth` / `asynq_retry_count` carries the prefixed name. Not verified in a compose run after this change.
- `identity-admin` logs a GORM `record not found` line at error level when it creates the first admin (the lookup that precedes the insert); the command succeeds. Cosmetic.
- Starting without the `observability` profile still logs one `opentelemetry error ... no such host` warning a minute per binary (the OTLP endpoint is set to `jaeger:4318`); remove `OTEL_EXPORTER_OTLP_ENDPOINT` from `x-env-base` to silence it.
- Temporal's auto-setup logs `Namespace default is not found` errors while it creates the namespace on first start; they stop once it is created.
- `make seed` is a plain HTTP client of the published ports (`tools/seed/README.md`); it creates the first admin with `identity-admin` first. It was not part of the 2026-11-25 verification run above and has not been re-verified on this stack.
