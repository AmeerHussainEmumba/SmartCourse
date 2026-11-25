# Microservices split: design and work plan

| Field | Value |
|---|---|
| Status | Approved by the project owner on 2026-11-25; in progress |
| Origin | Supervisor feedback on the Week 2 PR: the worker still imports `course` and `analytics` code, so Temporal workflows move into their own service and each service runs its own relay; each service needs its own outbox table instead of `platform.outbox` |
| Decisions confirmed | One Postgres database (confirmed by the supervisor); a separate `-api` and `-worker` binary per service; Kafka in the default dev profile; an independent audit service with its own ADR; the seed tool drives the HTTP APIs |
| Supersedes in part | ADR-0021 (in-process relay), ADR-0024 (the shared `platform` schema), ADR-0026 (worker coupling), ADR-0029 (dual transport), DR-009 (the single worker) |

## 1. Goal and the one rule

No service imports another service's code. They share only `libs/*` (the kernel and the contracts). They talk through Kafka events, and where a service needs another's data it keeps an event-fed mirror. Each service owns its tables, including its outbox and its idempotency tables, and a Postgres role proves it.

**The rule that keeps dual writes out.** The outbox row (producer side) and the `processed_events` marker (consumer side) are written in the same database transaction as the state change or effect they protect, in the owning service's schema. Anything that breaks this rule reintroduces a dual write:

- an outbox in a different database from the business tables;
- a `processed_events` table detached from the effect it guards;
- a service writing events for state another service owns;
- copying outbox or `processed_events` state between tables asynchronously (a mirror-and-sync scheme is itself a dual write).

## 2. Target topology

| Service (Go module) | Binaries | Schema / store | Outbox | `processed_events` | Produces | Consumes |
|---|---|---|---|---|---|---|
| `services/identity` | `identity-api`, `identity-worker`, `identity-admin` (CLI: first admin) | `identity` | `identity.outbox` | none | `identity.*` | none |
| `services/course` | `course-api`, `course-worker` | `course` | `course.outbox` | `course.processed_events` | `course.*`, `enrollment.*`, `progress.*` | identity events (instructor mirror), `course.course_archived` (enrollment archival), `progress.course_completed` (certificate issuer); runs the Temporal activities |
| `services/analytics` | `analytics-api`, `analytics-worker` | `analytics` | none | `analytics.processed_events` | none | the events its mirrors need |
| `services/notification` | `notification-worker` | `notification` | none | `notification.processed_events` | none | the events it mails about |
| `services/publishing` | `publishing-worker` | none | none | none | none | Temporal workflow queue only |
| `services/audit` | `audit-api`, `audit-worker` | MongoDB | none | idempotent by event id in Mongo | none | every topic and its retry topics |

Ten binaries. `services/worker` is deleted. `tools/seed` and `test/e2e` require no service module.

`libs/*` stays the shared kernel. New shared libraries: `libs/eventbus` (the Kafka publisher, consumer runner, codec and schema loader that used to be `services/worker/transport`), `libs/runtime` (process bootstrap shared by every binary: signal handling, config, telemetry, internal listener, graceful shutdown), and `libs/contracts/publishing` (the workflow and activity contract: names, task queues and DTO structs).

## 3. Data

### 3.1 Tables and migrations

Each service's migration set gains its own tables, in the service's schema, with the same columns as today's shared tables (do not redesign them):

| Set | New migration | Content |
|---|---|---|
| `migrations/identity` | `000002_outbox` | `identity.outbox` (the `platform.outbox` shape, including `trace_id`, `span_id`, `actor_id`) |
| `migrations/course` | `000008_outbox_inbox_idempotency` | `course.outbox`, `course.processed_events`, `course.idempotency_keys` |
| `migrations/analytics` | `000005_processed_events` | `analytics.processed_events` |
| `migrations/notification` | `000003_processed_events` | `notification.processed_events` |
| `migrations/cutover` (new set, run last) | `000001_split_platform` | moves every unpublished and recent row from `platform.outbox` to the owning service's outbox in one transaction, preserving `id` ordering and `event_id` (route by `event_type` prefix: `identity.*` to identity, everything else to course); moves `platform.processed_events` rows to the consuming service by `consumer_name`; moves `platform.idempotency_keys` to `course.idempotency_keys`; then drops the platform tables and the `platform` schema. On a fresh database it moves nothing |

Existing migrations are history: do not edit them. `migrations/platform` keeps `000001_extensions` (citext, pg_trgm in `public`); its `000002` and `000003` still run on fresh databases and are removed by the cutover.

Migration order in `make migrate-up` and `libs/testenv`: `platform identity course analytics notification cutover`.

`libs/outbox`, `libs/relay` and `libs/idempotency` keep using unqualified table names, so the service's own `search_path` (`<schema>,public`) selects the right table. The worker's combined search path `platform,course,analytics,public` disappears.

### 3.2 Per-service Postgres roles (enforced independence)

- Roles `svc_identity`, `svc_course`, `svc_analytics`, `svc_notification`, each `LOGIN`, owning nothing, with `USAGE` and DML on its own schema's tables and sequences only. `REVOKE ALL ON SCHEMA ... FROM PUBLIC` for every service schema. A separate owner role (the existing superuser or `smartcourse_owner`) runs migrations. Each role gets the grants it needs on `public` for the extension types and functions (`citext`, `pg_trgm`) and a `CONNECTION LIMIT` sized for its pool.
- No password in a committed migration. A script (`scripts/db-roles.sh`, called by `make migrate-up` and the compose database init) creates the roles from environment variables; dev defaults live in `env.example` only.
- Each service connects with its own role: `POSTGRES_DSN` per binary, `POSTGRES_SEARCH_PATH=<schema>,public`.
- **Enforcement test** (CI): a table-driven integration test over every role against every other service's schema proving `permission denied` for `SELECT`, `INSERT`, `UPDATE`, `DELETE` and `CREATE`. "identity cannot read course" is one row. A second test proves each role can do its own work (the service's own integration suites run as that role).

## 4. Events

- **Kafka is the only transport.** `EVENT_TRANSPORT` and the in-process delivery mode are removed. The relay in each service only publishes: claim a row with a lock, encode (Envelope plus payload, schema registered), produce, and mark published after the broker ack. It never runs another service's handlers.
- **Consumers** run in the owning service's worker through `libs/eventbus`: one consumer group per consumer name (group id = consumer name, as today), the handler runs in a transaction that also writes `processed_events`, then the offset is committed. The `relay.Handler` and `relay.Deliver` contracts stay.
- **Ordering.** Per-aggregate order holds because one service owns each aggregate and the relay's earlier-unpublished-row clause stays per outbox. Order across services is not guaranteed: a consumer must tolerate a dependency not having arrived (retry long enough, or a pending state). The default retry budget is already about a minute (8 attempts, 500 ms to 30 s).
- **"Published" means the broker acked.** How far a consumer is behind is Kafka lag (`kafka_consumer_lag`), not the outbox flag.
- **Pruning is per service:** each worker prunes its own outbox (`outbox.Prune`), its own `processed_events` (new `eventbus` helper, retention configurable) and its own idempotency keys.
- **Topics** are as in `libs/kafka/topics` plus one retry topic per consumer: `retry.<consumer-name>`. `topics.Ensure` and compose's `kafka-init` create them; a test keeps them in step.

### 4.1 Dead letters and replay

- A consumer that exhausts its budget parks the event in MongoDB `failed_events` (as today, one history entry per attempt).
- **Replay is republish, not re-invoke.** `audit-api` replays a parked event by encoding it from the stored payload and schema id and producing it to `retry.<consumer>`; the consuming service's own consumer group handles the retry topic through its normal idempotent path. Order: produce first, then mark replayed. A failure in between leaves the record parked, and a second replay is absorbed by `processed_events`.
- A parked record that cannot be decoded is never replayed (it stays parked with a clear error).
- Replay is audit-logged with the admin's user id.

## 5. Temporal

- `services/publishing` (`publishing-worker`) holds only the deterministic orchestration: `PublishCourseWorkflow`, its retry policies and the activity graph, calling every activity by name on the `course-activities` task queue. It has no database and imports no service code.
- `course-worker` registers the activities on `course-activities` and runs the business logic through the course service (it owns the data, the object store, the search index and the cache).
- The workflow starts on the `publish-workflows` queue. `course.Service.Publish` already starts it by name; only the queue name is checked against the contract.
- `libs/contracts/publishing` defines: the workflow name, the two queue names, the activity names, and the DTOs the activities exchange (`PublishOutline` moves out of `services/course/course`).
- Failure handling (`publish_failed` with a bounded generic reason, stale-publish reconciliation) stays in the course service.

## 6. Processes

- Every binary serves `/metrics`, `/health/live` and `/health/ready` on an **internal** listener (`METRICS_HTTP_ADDR`, default `:9100`). API binaries also serve their public health routes on the API port, but `/metrics` is never on the public port.
- Host ports (all `127.0.0.1`): `identity-api` 8090, `course-api` 8091, `analytics-api` 8092, `audit-api` 8093. Internal listeners published as: `identity-api` 9101, `course-api` 9102, `analytics-api` 9103, `audit-api` 9104, `identity-worker` 9111, `course-worker` 9112, `analytics-worker` 9113, `notification-worker` 9114, `publishing-worker` 9115, `audit-worker` 9116.
- One `Dockerfile.<binary>` per binary. Compose profiles: `core` now contains Postgres, Redis, MinIO, Mailpit, Kafka, Schema Registry and MongoDB plus every service except the publishing worker; `workflow` adds Temporal and `publishing-worker`; `observability` as before.
- Worker concerns by service:
  - `identity-worker`: the identity relay; the token-version re-seeder moves here from `identity-api`.
  - `course-worker`: the course relay, consumers (instructor mirror, enrollment archival, certificate issuer), jobs (stale-publish reconciliation, enrollment counters, idempotency and outbox pruning, `processed_events` pruning), the Temporal activities worker.
  - `analytics-worker`: its consumers and the reconciler job.
  - `notification-worker`: its consumers, the Asynq pools, the sweeper, the SMTP provider.
  - `audit-worker`: the archiver consumer. `audit-api`: failed-event list, get and replay, with the fail-closed admin auth already built.

## 7. Guards and tests

- `scripts/check-module-graph.sh`: **no service may require any other service**; `tools/seed` and `test/e2e` require no service either. Only `libs/*` edges remain.
- `scripts/check-link-graph.sh` and its package budgets are redone per binary; `scripts/check-dockerfile-modules.sh` covers all ten Dockerfiles.
- The cross-service tests that lived in `services/worker/cmd/worker` (events to certificate and mail, one trace across the pipeline, metrics under a Kafka outage) become E2E tests on the container harness, which starts the ten-binary topology. `tools/seed` drives the HTTP APIs after `identity-admin` creates the first admin.
- Every migration has a tested down path where the old ones did; the cutover has a test that seeds `platform` rows and proves they land in the right service with ids and ordering intact.
- Reproducers from the two adversarial passes are re-run on the new topology.

## 8. Work packages

**Wave A (foundation, parallel):**

| Package | Owns |
|---|---|
| A1 data foundation | `migrations/**`, `libs/testenv`, `libs/db` (role-aware helpers), `scripts/db-roles.sh`, the postgres init in compose, `Makefile` migrate targets, `env.example` DSN entries, the isolation tests, the cutover test, `migrations/README.md` |
| A2 shared libraries | `libs/eventbus` (from `services/worker/transport`, generalised so any service uses it), `libs/runtime`, `libs/contracts/publishing` (and the matching edits in `services/course` for `PublishOutline`), `libs/kafka/topics` (retry topics) |

**Wave B (one agent per service, parallel, after A):** B1 identity, B2 course, B3 analytics, B4 notification, B5 publishing, B6 audit. Each owns its service module, its Dockerfiles and its tests, and moves (`git mv`) the code that belongs to it out of `services/worker`.

**Wave C (integration):** compose profiles and ports, CI, the module and link guards, the E2E harness for the new topology and the scenarios on it, the seed tool, observability (dashboards and alerts per service), the Kafka-lag and relay metrics per service.

**Wave D (verify and document):** adversarial re-run, the behaviour fixes from the second pass that survive the move (long Mongo outage, `last_error` pruning, undecodable replay, codec validation), the dependency bump for the govulncheck findings, ADR-0033 (service independence) and ADR-0034 (the audit service), the RFC, every README, the OpenAPI (servers and the moved admin routes), the onboarding documents, and the full suite.
