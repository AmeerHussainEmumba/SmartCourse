// Command identity-worker is identity's background process (design:
// docs/plans/microservices-split.md §6). It runs three independent components
// side by side, built on libs/runtime so a failure in one never stops the
// others:
//
//   - relay: publishes identity.outbox to Kafka (libs/relay with a publisher
//     and no handlers: identity consumes nothing);
//   - token-version-reseeder: repairs the `user:tv:<id>` Redis keys after a
//     Redis flush (F-38); it used to run inside identity-api;
//   - jobs: the Asynq scheduler and servers running housekeeping (PruneOutbox).
//
// /metrics, /health/live and /health/ready are on the internal listener
// (METRICS_HTTP_ADDR). Postgres and Redis are fatal readiness checks, Kafka is
// non-fatal (the outbox buffers an outage).
package main

import (
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	runtime.Main("identity-worker", wire, options()...)
}

// requirements are the configuration groups this binary needs; it starts without
// the variables of any other group. It publishes identity.outbox to Kafka and runs the token-version re-seeder and jobs; it signs no token and hashes no password.
var requirements = config.Requirements{Postgres: true, Redis: true, Kafka: true}

// loadConfig is the runtime's configuration loader for this binary.
func loadConfig() (*config.Config, error) { return config.LoadWith(requirements) }

// options is the runtime configuration of this binary.
func options() []runtime.Option {
	return []runtime.Option{runtime.WithDependencies(infra.Connect), runtime.WithConfig(loadConfig), runtime.WithOutbox()}
}
