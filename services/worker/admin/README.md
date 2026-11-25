# services/worker/admin

The worker's small HTTP surface (ADR-0031, RFC §6.9, §15.8): health probes and the admin-only failed-events routes. It lives in the worker because a replay must re-run the worker's own handlers.

Listens on `ADMIN_HTTP_ADDR` (default `:8093`, validated in `libs/config/admin.go`). Compose publishes it on `127.0.0.1:8093` only. The routes are in `api/openapi.yaml` under `/admin/failed-events`; note that they are served by the worker, not by the three HTTP services.

| Route | Auth | Answers |
|---|---|---|
| `GET /health/live` | none | 200, never checks dependencies |
| `GET /health/ready` | none | 503 if Postgres or Redis fails; 200 `degraded` if only MongoDB fails; 200 `ready`. MongoDB is `ok`, `failed` or `not_configured`. No driver error text is returned |
| `GET /api/v1/admin/failed-events?status=&consumer=&limit=&cursor=` | admin | `{items, next_cursor}`, newest first, keyset-paginated (limit default 20, max 100; opaque cursor), no payloads |
| `GET /api/v1/admin/failed-events/{event_id}` | admin | `{event_id, records}`: one record per consumer, with payload and error history |
| `POST /api/v1/admin/failed-events/{event_id}/replay` | admin | 200 `{event_id, replayed, outcomes}`; `replayed=false` and `already_replayed` on a repeat; 422 `replay_failed` when a consumer failed again |

Errors are `{code, message, details}`: 400 `validation_failed` (bad status, limit, cursor, id), 401 `unauthorized` / `token_stale`, 403 `forbidden`, 404 `not_found`, 422 `replay_failed`, 503 `temporarily_unavailable` while MongoDB is unreachable.

## Security

- `RequireAuth(cfg.JWT)` (the same `JWT_SIGNING_KEY` as every service), `RequireRole("admin")`, `RequireFreshPrivilegeStrict(rdb)`: a demoted admin's old token is refused with `token_stale`. The worker has no `TokenVersionSource` (it cannot read `identity.users`, ADR-0022 to 0024), so a Redis miss on the token-version key **fails closed**: 401 `token_stale`, never "version 1" (E10: after FLUSHALL the old token of an admin demoted from version 1 used to get 200). Cost: after a flush an admin is locked out of these routes until the key is rewritten: identity's re-seeder (15 s tick) rewrites every version above 1 within about one tick; an admin at version 1 is unlocked by logging in, refreshing, or calling an identity `/me` or admin route. Redis unreachable is 503. The instructor routes in course-api keep "miss means version 1" on purpose; see `libs/auth` README.
- Hardening comes from `libs/router`: request id, security headers, recovery, 1 MiB body cap (`MAX_BODY_BYTES`), read/write/idle timeouts, no CORS origins (an operator tool is never called cross-origin from a browser), trusted proxies from config.
- Replay is a privileged write and is audited: each replay attempt appends one `audit_log` record per consumer outcome (`libs/mongostore` `AppendAudit`, no `event_id`, so every attempt is recorded, including a repeat of the same event). Fields: `actor_id` (the admin's user id from the verified token), `actor_role`, `action` `failed_event_replayed`, `resource_type` `failed_event`, `resource_id` (the event id), `after` `{consumer, outcome (replayed | already_replayed | failed), applied, error?}`, `ip`, `user_agent`, `at`. A failure to write the entry never undoes or changes the replay's response; it is logged at error level and counted (`admin.AuditFailures()`; there is no Prometheus series yet). Not audited: requests rejected before a replay runs (401, 403, 400, 404, 503) and the read routes. If MongoDB is down the replay itself already answers 503.
- Bind it to a private interface. Compose uses loopback only; a real deployment needs the same network controls as any admin port.

## Running independently of the worker

`cmd/worker/mongo_wire.go` runs it as its own goroutine with graceful shutdown. A failure to bind or a panic is logged and ends only that goroutine; the relay, Temporal worker and Asynq are unaffected. MongoDB connects in the background with capped backoff: unset or unreachable, the worker still starts, logs loudly, and the failed-events routes answer 503 until it is up.

## Tests

`go test -race -tags=integration ./admin/` (Docker: real MongoDB, Postgres, Redis): 401 / 403 / stale-token matrix on every route, list filters and a keyset walk with no repeat, bad status/limit/cursor, detail 200/404/400, replay applied once with a real instructor-mirror row and a no-op the second time, 404 and 400 on replay, 422 on a failing handler with the record left parked, body cap, security headers, health with MongoDB stopped (degraded) and with a required dependency down.

## Not verified

- Behaviour behind a reverse proxy or TLS terminator; load.
- The admin port was not exercised from the built Docker image or from compose.
