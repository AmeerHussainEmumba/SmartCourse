# audit-api

The audit service's admin HTTP surface (`GET/POST /api/v1/admin/failed-events...`, `/health/live`, `/health/ready`). Admin role and a fresh token (fail-closed) required. See `services/audit/README.md` for the replay design.

| Variable | Use |
|---|---|
| `HTTP_PORT` | API port (default 8080; host 8093 in Compose) |
| `METRICS_HTTP_ADDR` | internal listener: `/metrics`, `/health/*` (never the API port) |
| `MONGO_URI`, `MONGO_DATABASE`, `MONGO_*_TIMEOUT` | the store (required) |
| `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB` | token-version check (required) |
| `JWT_SIGNING_KEY`, `JWT_ISSUER` | same key as every service (required) |
| `KAFKA_BROKERS`, `KAFKA_PRODUCE_TIMEOUT` | the replay producer (required); no Schema Registry needed |
| `MAX_BODY_BYTES`, `HTTP_*_TIMEOUT`, `TRUSTED_PROXIES`, `HSTS_ENABLED` | as the other APIs |

`POSTGRES_*`, `MINIO_*`, `SCHEMA_REGISTRY_URL` are not used (placeholders fill them, see `auditconfig`). Built by `Dockerfile.audit-api`. Wiring: `wire.go` (`runtime.Main`, no Postgres).
