// Command analytics-worker runs the analytics service's background work: the
// Kafka consumers that feed its mirrors and aggregates (analytics.Handlers,
// consumer group "analytics-aggregator", with its retry topic), the scheduled
// reconciler and the processed_events pruning
// (docs/plans/microservices-split.md §6). Analytics produces no events, so
// there is no outbox and no relay here. A thin main over libs/runtime; the
// wiring is in wire.go.
package main

import (
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	runtime.Main("analytics-worker", wire, options()...)
}

// requirements are the configuration groups this binary needs; it starts without
// the variables of any other group. It consumes Kafka and runs the Asynq reconciler; MONGO_URI stays optional (dead-letter parking).
var requirements = config.Requirements{Postgres: true, Redis: true, Kafka: true}

// loadConfig is the runtime's configuration loader for this binary.
func loadConfig() (*config.Config, error) { return config.LoadWith(requirements) }

// options is the runtime configuration of this binary.
func options() []runtime.Option {
	return []runtime.Option{runtime.WithDependencies(infra.Connect), runtime.WithConfig(loadConfig)}
}
