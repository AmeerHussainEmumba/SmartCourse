// Command notification-worker is the notification service's only binary
// (docs/plans/microservices-split.md §6): the Kafka consumers that turn events
// into delivery rows, the Asynq pools that send them, the pending-delivery
// sweeper, the SMTP provider and the housekeeping jobs. It has no HTTP API; its
// only listener is the internal one (/metrics, /health/live, /health/ready)
// that libs/runtime serves on METRICS_HTTP_ADDR.
package main

import "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"

func main() {
	runtime.Main("notification-worker", wire)
}
