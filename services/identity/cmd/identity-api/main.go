// Command identity-api serves services/identity/identity's HTTP routes only
// (ADR-0023). It is one of identity's three binaries: identity-worker runs the
// outbox relay, the token-version re-seeder and the housekeeping jobs, and
// identity-admin creates the first admin. The process bootstrap (config,
// Postgres and Redis, telemetry, the internal /metrics and health listener,
// signals, graceful shutdown) is libs/runtime; wire.go holds everything
// identity-specific. /metrics is served on the internal listener only
// (METRICS_HTTP_ADDR), never on the public port.
package main

import "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"

func main() {
	runtime.Main("identity-api", wire, runtime.WithOutbox())
}
