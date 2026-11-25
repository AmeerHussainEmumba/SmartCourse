// Command audit-worker is the audit service's consumer: the audit-archiver,
// the one consumer of every event topic. It writes each event to the MongoDB
// archive (idempotent by event id) and the audit-relevant ones to audit_log,
// and parks its own failures in failed_events. It has no Postgres and no HTTP
// API (audit-api serves the failed-events admin surface); /metrics and the
// probes are on the internal listener (METRICS_HTTP_ADDR).
package main

import (
	rt "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	rt.Main("audit-worker", wire, rt.WithConfig(loadConfig))
}
