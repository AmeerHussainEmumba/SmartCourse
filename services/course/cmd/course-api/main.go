// Command course-api serves services/course/course, services/course/enrollment,
// services/course/progress and services/course/certificate's HTTP routes
// (ADR-0023). The domains stay together, not one binary each, because
// enrollment locks the `courses` row in the SAME database transaction it writes
// an enrollment row in (ADR-0012's seat-limit guarantee). Its background half
// (relay, consumers, jobs, Temporal activities) is cmd/course-worker.
//
// Bootstrap is libs/runtime: config, logger, Postgres (the svc_course role) and
// Redis, telemetry, graceful shutdown. /metrics, /health/live and /health/ready
// are on the INTERNAL listener (METRICS_HTTP_ADDR), never on the public port.
package main

import (
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	runtime.Main("course-api", wire, options()...)
}

// requirements are the configuration groups this binary needs; it starts without
// the variables of any other group. It serves the HTTP API: it verifies JWTs and presigns object-store URLs; it hashes no password and reaches Kafka only through the outbox (course-worker relays it).
var requirements = config.Requirements{Postgres: true, Redis: true, MinIO: true, JWT: true}

// loadConfig is the runtime's configuration loader for this binary.
func loadConfig() (*config.Config, error) { return config.LoadWith(requirements) }

// options is the runtime configuration of this binary.
func options() []runtime.Option {
	return []runtime.Option{runtime.WithConfig(loadConfig), runtime.WithOutbox()}
}
