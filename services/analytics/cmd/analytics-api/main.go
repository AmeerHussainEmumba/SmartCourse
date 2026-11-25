// Command analytics-api serves services/analytics/analytics's public HTTP
// routes (docs/plans/microservices-split.md §6). It is a thin main over
// libs/runtime: config, Postgres as svc_analytics, Redis, telemetry, the
// internal /metrics and health listener, and graceful shutdown. The wiring is
// in wire.go. The service's consumers and reconciler run in analytics-worker.
package main

import (
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	runtime.Main("analytics-api", wire, options()...)
}

// requirements are the configuration groups this binary needs; it starts without
// the variables of any other group. It serves the HTTP API and verifies JWTs; MONGO_URI stays optional (failed-events metric).
var requirements = config.Requirements{Postgres: true, Redis: true, JWT: true}

// loadConfig is the runtime's configuration loader for this binary.
func loadConfig() (*config.Config, error) { return config.LoadWith(requirements) }

// options is the runtime configuration of this binary.
func options() []runtime.Option {
	return []runtime.Option{runtime.WithDependencies(infra.Connect), runtime.WithConfig(loadConfig)}
}
