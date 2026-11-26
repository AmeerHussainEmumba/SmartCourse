# audit-worker

The audit-archiver: consumer group `audit-archiver` on every event topic and `retry.audit-archiver`. Each event is written to the MongoDB `events` archive (first write wins, keyed by event id) with its schema id and Kafka source (topic, partition, offset, key); audit-relevant types (`identity.user_role_changed`, `course.course_published`, `course.course_archived`, `course.publish_failed`, `progress.certificate_issued`) also get an `audit_log` entry per event id. A failure past `KAFKA_CONSUMER_MAX_ATTEMPTS` is parked in `failed_events` (with schema id and topic, so it can be replayed) and the offset advances. It consumes nothing else and has no HTTP API.

| Variable | Use |
|---|---|
| `MONGO_URI`, `MONGO_DATABASE`, `MONGO_*_TIMEOUT` | the store (required) |
| `KAFKA_BROKERS`, `KAFKA_CONSUMER_*` | consumer tuning (brokers required) |
| `METRICS_HTTP_ADDR` | internal listener: `/metrics`, `/health/live`, `/health/ready` |

Internal listener host port 9116 in compose. Readiness: Kafka fatal, MongoDB non-fatal (`degraded`; events wait in Kafka). Postgres and Redis are not used (no `WithDependencies`; the consumer is `eventbus.NewStreamConsumers`, idempotent by event id in MongoDB). Metrics: `event_processed_total{consumer}`, `dead_letter_total{consumer}`. Built by `Dockerfile.audit-worker`.
