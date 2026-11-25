# services/identity/cmd/identity-worker

Identity's background process (`docs/plans/microservices-split.md` §6). `main.go` is `runtime.Main("identity-worker", wire, runtime.WithOutbox())`; `wire.go` registers three independent components (see `services/identity/README.md`):

| Component | What | Notes |
|---|---|---|
| `relay` | `libs/relay` over `identity.outbox`, publish only | Publisher and no handlers (`TestBuildRelay_IsPublishOnly`). Waits for Kafka with backoff (`eventbus.Prepare`, `NewPublisherFor`); `RELAY_WORKERS` sizes the pool. |
| `token-version-reseeder` | `Service.RunTokenVersionReseeder`, every 15 s | Repairs `user:tv:<id>` after a Redis flush (F-38). Moved here from identity-api. |
| `jobs` | `libs/asynqx` servers and scheduler, `libs/jobs` | `PruneOutbox` only. |

Internal listener `METRICS_HTTP_ADDR` (`:9100`; compose publishes it on 9111): `/metrics`, `/health/live`, `/health/ready`. Postgres and Redis fatal; Kafka non-fatal. A component failing is shown degraded and never stops the others. Connects as `svc_identity`.

Tests: unit wiring guards (`wire_test.go`: publish-only relay, worker count, job set, Asynq pools); `-tags=integration` (`wire_integration_test.go`): real Postgres and Redis through `runtime.Run`, a recording publisher in place of Kafka (relay publishes and marks rows published in `identity.outbox`, re-seeder writes the key, an enqueued prune task deletes an old published row, Kafka down degrades readiness, a panicking relay leaves the others running). Not covered here: a live Kafka broker and schema registry (see `libs/eventbus` and the E2E harness).
