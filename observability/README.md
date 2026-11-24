# observability/

Provisioning as code for the compose `events` and `observability` profiles
(RFC §12.3, ADR-0018). Nothing here is configured through a UI.

## Files

| Path | What it is |
|---|---|
| `prometheus/prometheus.yml` | Scrape jobs for identity-api, course-api, analytics-api (their API port, 8080 in the container) and worker (`worker:9100`, its own `METRICS_HTTP_ADDR` listener, not the admin port) at `/metrics`, plus rule loading. For host-run binaries use `host.docker.internal:<port>`. |
| `prometheus/rules/smartcourse.yml` | Alert rules (9): dead-letter **rate** (not existence, RFC §6.9), outbox oldest-unpublished age, outbox backlog growth, consumer lag, Asynq retry growth, workflow failures, 5xx ratio, DB pool saturation, scrape target down. **Every threshold is a placeholder** and each rule says why. No Alertmanager is wired; see `/alerts` in Prometheus. |
| `grafana/provisioning/datasources/` | Prometheus (uid `prometheus`, default) and Jaeger (uid `jaeger`). |
| `grafana/provisioning/dashboards/` | File provider that loads every JSON in `grafana/dashboards/` into the "SmartCourse" folder. |
| `grafana/dashboards/smartcourse-overview.json` | One dashboard, 13 panels: RED, outbox, lag, Asynq, dead letters, events processed, workflows, DB pool, scrape targets up. Each panel's description says which service feeds it and which are empty until the Kafka step. |

## Which series have data today

| Series | Fed by | State |
|---|---|---|
| `http_requests_total`, `http_request_duration_seconds` | identity-api, course-api, analytics-api | live (route templates; unmatched requests are `unmatched`) |
| `db_connections_in_use`, `db_connections_max` | all four | live |
| `outbox_backlog`, `outbox_oldest_unpublished_seconds` | identity-api, course-api, worker (one shared table: aggregate with `max()`) | live, refreshed every 15 s |
| `asynq_queue_depth`, `asynq_retry_count` | worker | live (depth = pending + scheduled) |
| `workflow_started/completed/failed_total` | worker | wired; verified with the Temporal test environment only, not against a real Temporal server |
| `dead_letter_total{consumer}` | worker: `countParked` around the dead-letter sink | live once an event is parked (no series before); the relay's own give-up has `consumer="relay"` |
| `event_processed_total{consumer,outcome}` | worker: the Kafka runner (kafka transport) or a relay hook (in-process) | live per consumer once it handles an event |
| `kafka_consumer_lag{group,topic}` | worker: consumer-group lag from the brokers, total per topic, every 15 s | live in kafka mode only; stale (not zero) while the broker is unreachable |

## Exposure of `/metrics`

`/metrics` has **no authentication** (Prometheus scrapes it). It exposes route templates, counters and process statistics, not request data, but it still reveals the API surface and load. Compose publishes the ports on `127.0.0.1` only; the worker serves it on its own listener (`METRICS_HTTP_ADDR`, `:9100`), never on the authenticated admin port. A real deployment must keep `/metrics` on the internal network or filter it at the ingress.

## Tracing

The services export OTLP/HTTP to `jaeger:4318` (`OTEL_EXPORTER_OTLP_ENDPOINT` in compose). Jaeger is in the `observability` profile: without it the endpoint is unreachable, spans are dropped, one WARN per minute is logged and nothing else changes. Probe routes (`/metrics`, `/health/*`) are not traced. Look up a trace at http://localhost:16686 or `GET /api/traces/<trace-id>`; the trace id is in the access log (`trace_id`) and in `platform.outbox.trace_id` for events a request caused.

## Verified, and not

Verified on 2026-11-24 (disposable project `sc-obs`, removed afterwards; images built from the working tree): Prometheus showed `identity-api`, `course-api`, `analytics-api` and `worker` UP (plus itself); `promtool check config` passed for the config and 9 rules; Grafana provisioned the 13-panel dashboard and every panel expression runs against Prometheus (panels for the unfed series return no data, as documented); two requests to course-api each produced a Jaeger trace with the HTTP span and Redis and Postgres spans, one including `INSERT outbox`, and the outbox row carried the same trace id and the actor; with Jaeger down the services started and answered normally.

Not verified: panels rendered in a browser; any alert firing (none was triggered); Alertmanager (not wired); a live trace through Jaeger across the relay, a Kafka consumer and the delivery job on a built image (see ADR-0032's 2026-11-24 Phase 7 events note for what was run); tail sampling; behaviour under load; a real Temporal workflow run.

## Starting

```sh
docker compose --profile events up -d kafka schema-registry mongodb mailpit kafka-init
docker compose --profile observability up -d prometheus grafana jaeger
# or everything: make dev-full
```

Stop with `docker compose stop <service>`. Avoid `down -v`: it deletes the local data volumes.

## URLs (all bound to 127.0.0.1)

| Service | URL |
|---|---|
| Grafana | http://localhost:3001 (local default login admin / admin) |
| Prometheus | http://localhost:9090 (`/alerts`, `/targets`) |
| Jaeger UI | http://localhost:16686 (OTLP: `:4317` gRPC, `:4318` HTTP) |
| Mailpit UI / API | http://localhost:8025 (SMTP on `localhost:1025`) |
| Schema Registry | http://localhost:8081/subjects |
| Kafka (host listener) | `localhost:29092` |

Topics `identity.events`, `course.events`, `enrollment.events`,
`progress.events` are created by the one-shot `kafka-init` service
(idempotent). Override with `KAFKA_TOPIC_PARTITIONS` (default 3) and
`KAFKA_TOPIC_REPLICATION_FACTOR` (default 1). These apply only when a topic
is first created; to grow an existing topic use `kafka-topics.sh --alter`.

## Adding a dashboard

1. Build or edit it in Grafana, then Share > Export > "Export for sharing externally" (or copy the JSON model).
2. Save it as `grafana/dashboards/<name>.json` with a unique `uid` and datasource `{"type":"prometheus","uid":"prometheus"}`.
3. Grafana rescans every 30 s. Dashboards are read-only in the UI (`allowUiUpdates: false`), so changes go through this directory.
4. Use only the fixed metric names from RFC §12.3, and route templates (never raw ids) as label values.
