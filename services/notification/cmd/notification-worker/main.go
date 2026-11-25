// Command notification-worker is the notification service's only binary
// (docs/plans/microservices-split.md §6): the Kafka consumers that turn events
// into delivery rows, the Asynq pools that send them, the pending-delivery
// sweeper, the SMTP provider and the housekeeping jobs. It has no HTTP API; its
// only listener is the internal one (/metrics, /health/live, /health/ready)
// that libs/runtime serves on METRICS_HTTP_ADDR.
package main

import (
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	runtime.Main("notification-worker", wire, options()...)
}

// requirements are the configuration groups this binary needs; it starts without
// the variables of any other group. It consumes Kafka and sends email over Asynq; SMTP and MONGO_URI stay optional.
var requirements = config.Requirements{Postgres: true, Redis: true, Kafka: true}

// loadConfig is the runtime's configuration loader for this binary.
func loadConfig() (*config.Config, error) { return config.LoadWith(requirements) }

// options is the runtime configuration of this binary.
func options() []runtime.Option {
	return []runtime.Option{runtime.WithDependencies(infra.Connect), runtime.WithConfig(loadConfig)}
}
