// Command course-worker is the background half of the course service
// (docs/plans/microservices-split.md §6). It runs four independent components
// beside each other; a failure in one never stops another:
//
//   - relay: publish-only over course.outbox (libs/relay + libs/eventbus);
//   - consumers: one Kafka consumer set for the instructor mirror, enrollment
//     archival and the certificate issuer (libs/eventbus);
//   - jobs: the Asynq scheduler and pool for the stale-publish and
//     enrollment-count reconciliations and the outbox, processed_events and
//     idempotency-key pruning (libs/jobs, libs/asynqx);
//   - activities: the Temporal worker for the publish workflow's activities on
//     the activity queue. The workflow itself runs in publishing-worker.
//
// Bootstrap is libs/runtime. It connects as the svc_course role (search_path
// course,public), so every unqualified outbox, processed_events and
// idempotency_keys here is the course schema's own.
package main

import "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"

// The workflow_* counters (runtime.WithWorkflows) belong to the process that
// runs the workflow worker, which is publishing-worker, not this one.
func main() {
	runtime.Main("course-worker", wire, runtime.WithOutbox())
}
