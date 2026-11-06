# internal/platform/logging

Structured JSON logging via `log/slog` (RFC §12.4). Every line carries `request_id` (and, eventually, `trace_id`/`span_id` once OpenTelemetry lands in M4) via context — logs are for detail *after* a trace or a request ID has already localised a problem, not the primary diagnostic tool (RFC §12.1).

`New(env, level)` builds the process-wide base logger once at boot (every `cmd/*` binary calls this), JSON-formatted, enriched with `env`. `LevelFromEnv()` reads `LOG_LEVEL` so every binary picks it up the same way rather than each reimplementing the fallback-to-`"info"` logic separately.

`WithContext`/`FromContext` are how a request-scoped logger (already carrying `request_id`) travels through a call without being threaded explicitly through every function signature — `FromContext` never needs a nil check, since it falls back to `slog.Default()` when nothing was attached.

## Testing

No dedicated test file — this package has no business logic to verify beyond "does `slog` get configured the way the code says it does," which is better proven by reading actual JSON log output during development than by a unit test asserting on `slog`'s internal handler configuration.
