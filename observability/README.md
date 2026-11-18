# observability/

Provisioning as code for the compose `events` and `observability` profiles
(RFC §12.3, ADR-0018). Nothing here is configured through a UI.

## Files

| Path | What it is |
|---|---|
| `prometheus/prometheus.yml` | Scrape jobs for identity-api, course-api, analytics-api and worker at `/metrics`, plus rule loading. Targets are compose service names; the worker port (`9100`) is a placeholder until the worker's metrics listener exists. For host-run binaries use `host.docker.internal:<port>`. |
| `prometheus/rules/smartcourse.yml` | Alert rules: dead-letter **rate** (not existence, RFC §6.9), outbox oldest-unpublished age, outbox backlog growth, consumer lag, Asynq retry growth, workflow failures, 5xx ratio, DB pool saturation. No Alertmanager is wired; see `/alerts` in Prometheus. |
| `grafana/provisioning/datasources/` | Prometheus (uid `prometheus`, default) and Jaeger (uid `jaeger`). |
| `grafana/provisioning/dashboards/` | File provider that loads every JSON in `grafana/dashboards/` into the "SmartCourse" folder. |
| `grafana/dashboards/smartcourse-overview.json` | One dashboard with panels for the RFC §12.3 metrics (RED, outbox, lag, Asynq, dead letters, events processed, workflows, DB pool). |

Panels stay empty and the app scrape targets show as down until
`libs/telemetry` lands and the services are running.

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
