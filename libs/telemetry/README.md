# libs/telemetry

OpenTelemetry tracing, W3C trace-context helpers for the event envelope, Gin middleware and Prometheus metrics (ADR-0032, RFC §12). Pure Go, no cgo. Nothing here is wired into a service yet; that waits for the Week 3 module-layout review.

## Tracing

```go
tp, err := telemetry.Setup(ctx, telemetry.Config{
    Enabled:     true,
    ServiceName: "course-api",
    Endpoint:    "jaeger:4318", // OTLP/HTTP; "http://jaeger:4318" also works
    Insecure:    true,
    SampleRatio: 1, // 1 in development (RFC §12.2)
})
if err != nil { /* invalid config */ }
defer tp.Shutdown(shutdownCtx) // flushes buffered spans

r.Use(telemetry.HTTPTracing())
```

- `Config{}` (the zero value) is disabled: `Setup` returns a no-op provider, installs no global, opens no connection and starts no goroutine. Helpers still work and return empty values.
- `Setup` does not contact the collector. A Jaeger outage never blocks boot; failed exports are logged at WARN and spans are dropped.
- Export is OTLP over HTTP (`otlptracehttp`), Jaeger's port 4318.

### Sampling and errors

Head-based, parent-based ratio sampling (`SampleRatio`), with errors always exported. The SDK cannot make a head decision from a later error, so this is implemented as the closest correct thing:

1. A span the sampler would drop is still **recorded** (`RecordOnly`) and its sampled flag stays clear, so the `traceparent` that propagates still says "not sampled".
2. When such a span ends with status Error (`EndSpan(span, err)`, or a 5xx in `HTTPTracing`), a span processor exports it anyway.

Limitations: only the failed span itself is exported for an unsampled trace, not its non-failing siblings or parents (a 5xx HTTP span carries the failure, the earlier DB or Redis spans of that request are lost); the trace appears in Jaeger as a fragment. Because all spans record, the CPU cost of tracing does not fall with the ratio, only the export volume does. 4xx responses are not errors. Tail-based sampling (a collector) would give whole error traces; it is not built.

### Carrying the trace through events

```go
// At the state change (inside the handler's transaction): store on the outbox row.
traceID, spanID, traceparent := telemetry.InjectTraceContext(ctx)

// Relay: continue from the row, publish, put the new traceparent in the Kafka header.
ctx, span := telemetry.StartProduce(telemetry.ContinueFromEvent(ctx, row.Traceparent), topic, eventType, eventID)
_, _, header := telemetry.InjectTraceContext(ctx)
defer telemetry.EndSpan(span, err)

// Consumer: continue from the header, then (if it enqueues a job) carry it in the payload.
ctx, span := telemetry.StartConsume(ctx, header, group, topic, eventType, eventID)
ctx, enq := telemetry.StartEnqueue(ctx, queue, jobType)
_, _, payloadTP := telemetry.InjectTraceContext(ctx)

// Worker:
ctx, span := telemetry.StartJob(ctx, payloadTP, queue, jobType, jobID)
```

`traceparent` is the authoritative carrier; `traceID`/`spanID` are for the envelope columns of RFC §6.4. A missing or malformed `traceparent` is not an error: the next span starts a fresh trace (even when `ctx` holds an unrelated active span). `TraceIDsFromContext` returns the ids for log lines (RFC §12.4).

## HTTP metrics and the metric catalogue

```go
reg := telemetry.NewRegistry()          // Go runtime + process collectors
m, err := telemetry.NewMetrics(reg)     // registers the catalogue below; safe to call twice
r.Use(telemetry.HTTPMetrics(m))
r.GET("/metrics", telemetry.GinHandler(reg)) // internal network only; unauthenticated
_ = telemetry.RegisterDBStats(reg, sqlDB)    // db_connections_*
```

`HTTPMetrics` labels by `c.FullPath()` (the route template); requests that match no route use the constant `unmatched`. Methods outside the standard set are labelled `OTHER`, status is clamped to 3 digits. `TestHTTPMetricsCardinality` fires 20,000 distinct paths, ids and methods and fails if the series count grows.

| Metric | Type | Labels | Fed by |
|---|---|---|---|
| `http_requests_total` | counter | `route`, `method`, `status` | `HTTPMetrics` |
| `http_request_duration_seconds` | histogram | `route`, `method` | `HTTPMetrics` |
| `outbox_backlog` | gauge | | `OutboxGauges` poller |
| `outbox_oldest_unpublished_seconds` | gauge | | `OutboxGauges` poller |
| `kafka_consumer_lag` | gauge | `group`, `topic` | `ConsumerLagGauges` poller |
| `asynq_queue_depth` | gauge | `queue` | `QueueGauges` poller |
| `asynq_retry_count` | gauge | `queue` | `QueueGauges` poller |
| `dead_letter_total` | counter | `consumer` | `m.DeadLettered(consumer)` |
| `workflow_started_total` | counter | | `m.WorkflowStartedTotal.Inc()` |
| `workflow_completed_total` | counter | | `m.WorkflowCompletedTotal.Inc()` |
| `workflow_failed_total` | counter | | `m.WorkflowFailedTotal.Inc()` |
| `db_connections_in_use` | gauge | | `RegisterDBStats` (read at scrape) |
| `db_connections_max` | gauge | | `RegisterDBStats` (0 means unlimited) |
| `event_processed_total` | counter | `consumer`, `outcome` | `m.EventProcessed(consumer, outcome)` |

`outcome` is one of `processed`, `duplicate`, `failed`, `dead_lettered`, `other`; anything else is recorded as `other`. The names are constants (`Metric...`) and are an API: dashboards are provisioned against them.

### Gauge-style collectors

Gauges that come from a query implement `GaugeCollector`. The adapters take a function that runs the query, and `StartPoller` runs them on an interval:

```go
stop := telemetry.StartPoller(ctx, 15*time.Second,
    telemetry.OutboxGauges(m, func(ctx context.Context) (telemetry.OutboxStats, error) { /* SELECT count, age */ }),
    telemetry.ConsumerLagGauges(m, lagQuery),
    telemetry.QueueGauges(m, inspectorQuery),
)
defer stop()
```

A failing query is logged and leaves the previous value (stale, never a false zero). Labelled series that disappear from the source (a deleted queue) are removed.

## Dependencies and licences

Verified against the module proxy on 2026-11-18. Versions are aligned with what the rest of the repo already resolves (`scripts/check-module-hygiene.sh` rejects skew).

| Module | Version | Licence |
|---|---|---|
| `go.opentelemetry.io/otel`, `/sdk`, `/trace`, `/exporters/otlp/otlptrace/otlptracehttp` | v1.44.0 (v1.47.0 is current) | Apache-2.0 |
| `github.com/prometheus/client_golang` | v1.24.1 | Apache-2.0 |
| `github.com/gin-gonic/gin` | v1.12.0 | MIT |
| `go.uber.org/goleak` (tests) | v1.3.0 | MIT |

The OTLP/HTTP exporter links `google.golang.org/grpc` and protobuf (for status codes and the OTLP types), so any binary importing this module links gRPC. Prometheus' `promhttp` adds `klauspost/compress` and `prometheus/common`.

## Limitations

- Errors-always-sampled exports the failed span only, not the whole trace (see Sampling).
- No database, Redis or Temporal instrumentation here: those are separate Phase 7 work on the call sites (`otelgorm`, `redisotel` and the Temporal interceptor would each add a dependency, so they are not pulled in by this module).
- Metrics use no OpenTelemetry metrics SDK; Prometheus is the only metrics path.
- `RegisterDBStats` supports one pool per registry; a second call is a no-op and keeps the first pool.
- Only the W3C `traceparent` is carried; `tracestate` and baggage are not stored on the envelope.
- `/metrics` has no authentication.

## Testing

`GOWORK=off go test -race ./...`. Tracing tests use the SDK's in-memory exporter and prove one trace id across a simulated HTTP, outbox, relay, consumer and Asynq job chain, the fresh-trace behaviour for malformed input, error promotion and the no-op disabled mode. `goleak` guards the whole package.
