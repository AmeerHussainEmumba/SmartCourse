# Changelog — libs/telemetry

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- README corrected for the split: wiring through `libs/runtime`, per-service outbox gauges, `/metrics` only on the internal listener (`RegisterMetricsRoute` removed), the cross-binary trace test status. No code change.

- README: the internal listener (`libs/runtime`) that serves `/metrics`, `/health/live` and `/health/ready` on `METRICS_HTTP_ADDR` for every binary. No code change.

- Added `TraceparentFromIDs(traceID, spanID)` (builds a traceparent from an outbox row's two id columns, sampled flag set, empty for invalid ids), `StartRelay` (INTERNAL span for the outbox relay delivering a row) and the `TraceparentHeader` constant shared by the Kafka record header and the Asynq task header.

- Wired into identity-api, course-api, analytics-api and the worker through `libs/infra` (`Dependencies.Observe`), Phase 7.
- Added `CheckLabels`: the cardinality guard. It fails when any non-runtime metric has a label outside `route, method, status, queue, group, topic, consumer, outcome` (tests prove it rejects `user_id`, `course_id`, `path`, `url`, `request_id`, `trace_id`, `event_id`, `job_id`, `email`).
- `HTTPTracing` records `smartcourse.request_id` on the server span and does not trace `/metrics`, `/health/live` and `/health/ready`.
- The OpenTelemetry error handler logs a repeated exporter error once a minute (with a suppressed-repeats count) instead of once per failed batch while the collector is down.

- Module created (Week 3 plan, ADR-0026).
- Tracing: `Config`, `Setup` / `SetupWithExporter`, `Provider` (OTLP/HTTP to Jaeger, parent-based ratio sampling, failed spans always exported, disabled mode is a no-op).
- Event-envelope propagation: `InjectTraceContext`, `ContinueFromEvent`, `TraceIDsFromContext`; span helpers `StartProduce`, `StartConsume`, `StartEnqueue`, `StartJob`, `EndSpan`.
- Gin middleware `HTTPTracing` and `HTTPMetrics` (route-template labels, constant `unmatched`).
- Prometheus: `NewRegistry`, `NewMetrics` (the 14 catalogued metrics, idempotent), `Handler` / `GinHandler`, `RegisterDBStats`, `GaugeCollector` with `OutboxGauges`, `ConsumerLagGauges`, `QueueGauges`, `Poll` / `StartPoller`.
- Tests: propagation chain, malformed traceparent, sampling, cardinality guard, catalogue names.
