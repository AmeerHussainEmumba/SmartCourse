# internal/platform/temporal

`Connect` builds a Temporal client with `client.NewLazyClient`, not `client.Dial` — it never fails at boot because Temporal is unreachable, only an actual publish call would (ADR-0009: "Temporal down means publishing unavailable — though nothing else is affected").

Its own package, apart from `internal/platform/infra`, so that `cmd/identity-api` and `cmd/analytics-api` don't link the Temporal SDK. Only `cmd/course-api` (to start the publish workflow), `cmd/worker` (to run it) and `cmd/seed` import it. `scripts/check-link-graph.sh` enforces that for the two APIs.

## Testing

No dedicated test file — nothing here beyond constructing the client.
