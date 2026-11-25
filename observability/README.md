# observability/

Provisioning as code for the compose `observability` profile (RFC §12.3, ADR-0018). Nothing here is configured through a UI.

## Files

| Path | What it is |
|---|---|
| `prometheus/prometheus.yml` | One scrape job per binary (ten) at `<service-name>:9100/metrics`, the internal listener (`METRICS_HTTP_ADDR`) of every container, plus Prometheus itself and rule loading. Each target carries the labels `service` (identity, course, analytics, notification, publishing, audit) and `binary` (identity-api, ...); the job name equals the binary. For host-run binaries use `host.docker.internal:<port>`. |
| `prometheus/rules/smartcourse.yml` | Alert rules (9): dead-letter **rate** (not existence, RFC §6.9), outbox oldest-unpublished age, outbox backlog growth, consumer lag, Asynq retry growth, workflow failures, 5xx ratio, DB pool saturation, scrape target down. They group by `service` / `binary`, never by a hard-coded job list. **Every threshold is a placeholder** and each rule says why. No Alertmanager is wired; see `/alerts` in Prometheus. |
| `grafana/provisioning/datasources/` | Prometheus (uid `prometheus`, default) and Jaeger (uid `jaeger`). |
| `grafana/provisioning/dashboards/` | File provider that loads every JSON in `grafana/dashboards/` into the "SmartCourse" folder. |
| `grafana/dashboards/smartcourse-overview.json` | One dashboard, 13 panels: RED per service and route, outbox per service, consumer lag per group, Asynq, dead letters, events processed, workflows, DB pool per binary, scrape targets up. Each panel's description says which binaries feed it. |

## Targets

| Binary | Service | Target (compose network) | Host port (127.0.0.1) |
|---|---|---|---|
| identity-api | identity | `identity-api:9100` | 9101 |
| course-api | course | `course-api:9100` | 9102 |
| analytics-api | analytics | `analytics-api:9100` | 9103 |
| audit-api | audit | `audit-api:9100` | 9104 |
| identity-worker | identity | `identity-worker:9100` | 9111 |
| course-worker | course | `course-worker:9100` | 9112 |
| analytics-worker | analytics | `analytics-worker:9100` | 9113 |
| notification-worker | notification | `notification-worker:9100` | 9114 |
| publishing-worker | publishing | `publishing-worker:9100` | 9115 |
| audit-worker | audit | `audit-worker:9100` | 9116 |

`publishing-worker` only runs under the `workflow` profile (which `observability` includes). If you start only `core` services by name, its target is DOWN and `ScrapeTargetDown` fires for it by design: the target is configured.

## Which series have data

Observed on 2026-11-25 against the live ten-binary stack after one registration / publish / enrollment flow (details in `docs/guides/local-stack.md`): every dashboard expression returned a successful result.

| Series | Fed by | Seen |
|---|---|---|
| `http_requests_total`, `http_request_duration_seconds` | the four APIs | yes: series for identity, course, analytics and audit (route templates; unmatched requests are `unmatched`) |
| `db_connections_in_use`, `db_connections_max` | every binary with Postgres (not publishing, audit) | yes: 7 binaries |
| `outbox_backlog`, `outbox_oldest_unpublished_seconds` | identity and course (each service owns its outbox; api and worker both report it, so the queries use `max by (service)`) | yes: `service="identity"` and `service="course"` |
| `kafka_consumer_lag{group,topic}` | the workers that own a consumer group (group id = consumer name) | yes: 16 series across analytics, course and others |
| `event_processed_total{consumer,outcome}` | the workers' consumers | yes: analytics-aggregator, audit-archiver, course-instructor-mirror, notification-dispatcher |
| `asynq_queue_depth`, `asynq_retry_count` | the workers that run Asynq | yes: course and identity queues seen (queue labels critical, default, low) |
| `workflow_started/completed/failed_total` | the Temporal workers | yes: `service="publishing"` after one publish |
| `dead_letter_total{consumer}` | the consumers' dead-letter sinks | no series until an event is parked (none was) |

## Exposure of `/metrics`

`/metrics` has **no authentication** (Prometheus scrapes it). It exposes route templates, counters and process statistics, not request data, but it still reveals the API surface and load. Every binary serves it on its own internal listener (`METRICS_HTTP_ADDR`, `:9100` in the containers), **never on the public API port** (checked: `GET :8090..8093/metrics` answers 404). Compose publishes the listeners on `127.0.0.1` only. A real deployment must keep them on the internal network or filter them at the ingress.

## Tracing

Every binary exports OTLP/HTTP to `jaeger:4318` (`OTEL_EXPORTER_OTLP_ENDPOINT` in compose). Jaeger is in the `observability` profile: without it the endpoint is unreachable, spans are dropped, one WARN per minute is logged and nothing else changes (observed: the `opentelemetry error` warnings in the logs of a stack started without Jaeger). Probe routes (`/metrics`, `/health/*`) are not traced. Look up a trace at http://localhost:16686 or `GET /api/traces/<trace-id>`; the trace id is in the access log (`trace_id`) and in the owning service's `outbox.trace_id` for events a request caused.

## Verified, and not

Verified on 2026-11-25 (disposable compose project `sc-split`, images built from the working tree, removed with `down -v` afterwards): `promtool check config` passed from the pinned `prom/prometheus:v3.16.0` image (config plus the 9 rules); Prometheus listed all ten application targets UP with their `service` and `binary` labels (plus itself); all 9 rules loaded with health ok (none firing); 18 of the dashboard's 19 expressions returned `success` against Prometheus (the 19th, the p95-by-route panel, was edited afterwards to add `service` to its `sum by`; that edit was not re-run); Grafana provisioned the SmartCourse folder; Jaeger received traces from identity-api, course-worker and analytics-worker (it was started after the flow, so only later requests were traced).

Not verified: panels rendered in a browser; any alert firing (none was triggered); Alertmanager (not wired); a trace across the relay, a Kafka consumer and the delivery job on the split topology; tail sampling; behaviour under load; the `ScrapeTargetDown` case for an absent `publishing-worker`.

## Starting

```sh
make dev-observability          # workflow profile + Prometheus, Grafana, Jaeger
# or everything: make dev-full
# only the three observability services on top of a running stack:
docker compose --profile observability up -d prometheus grafana jaeger
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

Kafka, the Schema Registry and MongoDB are in the `core` profile. Topics (`identity.events`, `course.events`, `enrollment.events`, `progress.events` and one `retry.<consumer>` topic per consumer) are created by the one-shot `kafka-init` service (idempotent). Override with `KAFKA_TOPIC_PARTITIONS` (default 3) and `KAFKA_TOPIC_REPLICATION_FACTOR` (default 1). These apply only when a topic is first created; to grow an existing topic use `kafka-topics.sh --alter`.

## Adding a dashboard

1. Build or edit it in Grafana, then Share > Export > "Export for sharing externally" (or copy the JSON model).
2. Save it as `grafana/dashboards/<name>.json` with a unique `uid` and datasource `{"type":"prometheus","uid":"prometheus"}`.
3. Grafana rescans every 30 s. Dashboards are read-only in the UI (`allowUiUpdates: false`), so changes go through this directory.
4. Use only the fixed metric names from RFC §12.3, route templates (never raw ids) as label values, and group by `service` or `binary`, not by a hard-coded job name.
