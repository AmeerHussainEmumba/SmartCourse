# Changelog — libs/telemetry

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

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
