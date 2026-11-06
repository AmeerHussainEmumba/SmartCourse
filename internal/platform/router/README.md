# internal/platform/router

Assembles the Gin engine every module registers routes on (`New`), and the two health endpoints every binary exposes (`RegisterHealth`).

## What `New` wires in

Cross-cutting middleware, in order: `httpx.RequestID()`, a structured per-request log line (`slog.Info("request", method, path, status, latency, request_id)` — RFC §12.4), `gin.Recovery()`, then CORS. `allowedOrigins` is **deny-by-default** (RFC §11.9): an empty slice permits no cross-origin requests at all — the safe default until a real client origin is configured, never `"*"`.

## `/health/live` vs `/health/ready` — deliberately different

`RegisterHealth` (RFC §12.5) wires both, and they check different things on purpose:

- **`/health/live`** never checks a dependency. It answers "is this process still running," full stop.
- **`/health/ready`** runs every `Checker` it's given (Postgres, Redis — see `internal/platform/bootstrap.Dependencies.HealthCheckers`) and returns `503` if any fails, so a load balancer stops routing to this instance without the process being killed.

Conflating the two means a brief Redis blip restarts every replica simultaneously — turning a degradation into an outage. This is the specific failure mode the split exists to prevent.

## Testing

No dedicated test file — `New`'s middleware chain and `RegisterHealth`'s two endpoints are exercised indirectly by every module's HTTP-level test (`*_http_test.go`, build tag `integration`), each of which boots a real engine through this package's `New`/route-registration path as its very first assertion.
