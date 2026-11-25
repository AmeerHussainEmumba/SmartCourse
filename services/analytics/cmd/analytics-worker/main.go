// Command analytics-worker runs the analytics service's background work: the
// Kafka consumers that feed its mirrors and aggregates (analytics.Handlers,
// consumer group "analytics-aggregator", with its retry topic), the scheduled
// reconciler and the processed_events pruning
// (docs/plans/microservices-split.md §6). Analytics produces no events, so
// there is no outbox and no relay here. A thin main over libs/runtime; the
// wiring is in wire.go.
package main

import "github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
import "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"

func main() {
	runtime.Main("analytics-worker", wire, runtime.WithDependencies(infra.Connect))
}
