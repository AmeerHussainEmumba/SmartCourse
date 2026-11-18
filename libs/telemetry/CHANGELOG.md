# Changelog — libs/telemetry

All notable changes to this module. Versioning policy: `docs/guides/versioning.md`.

## [Unreleased]

- Module created (Week 3 plan, ADR-0026).
- Tracing: `Config`, `Setup` / `SetupWithExporter`, `Provider` (OTLP/HTTP to Jaeger, parent-based ratio sampling, failed spans always exported, disabled mode is a no-op).
- Event-envelope propagation: `InjectTraceContext`, `ContinueFromEvent`, `TraceIDsFromContext`; span helpers `StartProduce`, `StartConsume`, `StartEnqueue`, `StartJob`, `EndSpan`.
- Gin middleware `HTTPTracing` and `HTTPMetrics` (route-template labels, constant `unmatched`).
- Prometheus: `NewRegistry`, `NewMetrics` (the 14 catalogued metrics, idempotent), `Handler` / `GinHandler`, `RegisterDBStats`, `GaugeCollector` with `OutboxGauges`, `ConsumerLagGauges`, `QueueGauges`, `Poll` / `StartPoller`.
- Tests: propagation chain, malformed traceparent, sampling, cardinality guard, catalogue names.
