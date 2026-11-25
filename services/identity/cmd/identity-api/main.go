// Command identity-api serves services/identity/identity's HTTP routes only
// (ADR-0023). It is one of identity's three binaries: identity-worker runs the
// outbox relay, the token-version re-seeder and the housekeeping jobs, and
// identity-admin creates the first admin. The process bootstrap (config,
// Postgres and Redis, telemetry, the internal /metrics and health listener,
// signals, graceful shutdown) is libs/runtime; wire.go holds everything
// identity-specific. /metrics is served on the internal listener only
// (METRICS_HTTP_ADDR), never on the public port.
package main

import (
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	runtime.Main("identity-api", wire, options()...)
}

// requirements are the configuration groups this binary needs; it starts without
// the variables of any other group. It serves the HTTP API: it signs and verifies JWTs and hashes passwords (Argon); it neither produces nor consumes Kafka and has no object store.
var requirements = config.Requirements{Postgres: true, Redis: true, JWT: true, Argon: true}

// loadConfig is the runtime's configuration loader for this binary.
func loadConfig() (*config.Config, error) { return config.LoadWith(requirements) }

// options is the runtime configuration of this binary.
func options() []runtime.Option {
	return []runtime.Option{runtime.WithConfig(loadConfig), runtime.WithOutbox()}
}
