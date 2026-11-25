# services/identity/cmd/identity-api

SmartCourse's identity HTTP service (ADR-0023). One of identity's three binaries; see `services/identity/README.md`.

```
go run ./services/identity/cmd/identity-api
docker compose --profile core up -d   # or containerized
```

## What's here

`main.go` is one line: `runtime.Main("identity-api", wire, runtime.WithOutbox())`. `wire.go` builds the Gin router (`newRouter`: identity's routes from `services/identity/identity`, the public health routes, tracing and request metrics) and registers the HTTP server as a critical component.

- **`/metrics` is not on the public port.** It is served by the internal listener (`METRICS_HTTP_ADDR`, default `:9100`) together with `/health/live` and `/health/ready`. `TestIdentityAPI_MetricsOnlyOnTheInternalListener` pins it.
- **The token-version re-seeder is not here.** It runs in `identity-worker` (`TestIdentityAPI_DoesNotRunTheTokenVersionReseeder`).
- Connects as `svc_identity` (`POSTGRES_SEARCH_PATH=identity,public`).

## Why this split out cleanly

Nothing outside the service reads its tables; every other module reads the caller from the JWT claims (`auth.FromContext`), never a lookup into `users`. Its events (`identity.user_registered`, `identity.user_role_changed`, `identity.user_profile_updated`) leave through `identity.outbox` and the `identity-worker` relay. See ADR-0023 and ADR-0024.
