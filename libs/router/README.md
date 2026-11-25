# libs/router

Assembles the Gin engine every module registers routes on (`New`), and the two health endpoints every API binary exposes (`RegisterHealth`; the internal listener of `libs/runtime` serves the probes for every binary).

**Used by** the four API binaries: `identity-api`, `course-api`, `analytics-api` and `audit-api` (also `libs/infra`, for the readiness checker type).

## What `New` wires in

Cross-cutting middleware, in order: `httpx.RequestID()`, a structured per-request log line (`slog.Info("request", method, path, status, latency, request_id)` — RFC §12.4), `gin.Recovery()`, then CORS. `allowedOrigins` is **deny-by-default** (RFC §11.9): an empty slice permits no cross-origin requests at all — the safe default until a real client origin is configured, never `"*"`.

## `/health/live` vs `/health/ready` — deliberately different

`RegisterHealth` (RFC §12.5) wires both, and they check different things on purpose:

- **`/health/live`** never checks a dependency. It answers "is this process still running," full stop.
- **`/health/ready`** runs every `Checker` it's given (Postgres, Redis — see `libs/infra.Dependencies.HealthCheckers`) and returns `503` if any fails, so a load balancer stops routing to this instance without the process being killed.

Conflating the two means a brief Redis blip restarts every replica simultaneously — turning a degradation into an outage. This is the specific failure mode the split exists to prevent.

## Testing

No dedicated test file — `New`'s middleware chain and `RegisterHealth`'s two endpoints are exercised indirectly by every module's HTTP-level test (`*_http_test.go`, build tag `integration`), each of which boots a real engine through this package's `New`/route-registration path as its very first assertion.

## Observability slot

`Options.Observe` takes middleware that runs after the request ID and security headers and before the access logger and `Recovery`. `libs/infra.Observability.Middleware()` supplies tracing and RED metrics there, so the span exists when the logger reads it (the access log line then carries `trace_id` and `span_id`) and a recovered panic is recorded as the 500 it answers. `path` in the access log is the route template.

## /metrics

The router never serves `/metrics`: it is on the internal listener of `libs/runtime`, and the API binaries' tests assert a public 404. (The public mounting through `infra.RegisterMetricsRoute` was removed in Wave C5; this package had no route of its own.)
