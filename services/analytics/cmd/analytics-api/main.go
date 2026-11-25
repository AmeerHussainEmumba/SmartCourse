// Command analytics-api serves services/analytics/analytics's public HTTP
// routes (docs/plans/microservices-split.md §6). It is a thin main over
// libs/runtime: config, Postgres as svc_analytics, Redis, telemetry, the
// internal /metrics and health listener, and graceful shutdown. The wiring is
// in wire.go. The service's consumers and reconciler run in analytics-worker.
package main

import "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"

func main() {
	runtime.Main("analytics-api", wire)
}
