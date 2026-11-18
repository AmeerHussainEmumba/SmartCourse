//go:build integration

package e2e_test

import "testing"

// Failure-mode scenarios: E4 to E7. See the header of
// scenarios_pipeline_test.go for the rules every spec follows.

// TestE2E_E04_PoisonMessage is E4, matrix row E04 (Phase 4).
//
// Steps:
//  1. Stack as E01 with a small retry budget for consumers (for example
//     3 attempts, 50 ms backoff) so the test is fast.
//  2. Produce a record to enrollment.events that decodes (valid envelope and
//     schema id) but makes the analytics consumer fail every time (for
//     example a StudentEnrolled for a course id that does not exist and that
//     the handler treats as an error). Then enroll a real student normally,
//     so a healthy event sits behind the poison one on the same partition.
//  3. Wait for the retry budget to be exhausted.
//
// Assertions:
//   - failed_events in MongoDB has exactly one document for the poison event
//     id with the payload, the consumer name, attempts equal to the budget
//     and one error entry per attempt.
//   - The healthy enrollment behind it was processed (analytics counters,
//     Mailpit mail): the partition did not stall, the offset advanced.
//   - The failed-events admin endpoint (admin token) lists the document;
//     dead_letter_total increased by 1.
//   - POST replay for that event id (admin) re-applies it through the normal
//     idempotent path: after fixing the cause (create the missing course) the
//     effect appears once. A second replay of the same id is a 200/no-op with
//     no second effect.
//
// Helper needed: Kafka producer, Mongo client, admin endpoint client (Phase 4).
func TestE2E_E04_PoisonMessage(t *testing.T) {
	t.Skip("pending: Phase 4 - dead-letter parking in failed_events, admin list and replay endpoints")
}

// TestE2E_E05_KafkaDownThenUp is E5, matrix row E05 (Phase 3; metrics from
// Phase 7).
//
// Steps:
//  1. Stack as E01. Record baseline analytics counters.
//  2. Stop the Kafka container.
//  3. Over 30 seconds, enroll 20 different students (each registered
//     beforehand) in 20 courses with seats, and call read endpoints
//     (GET /courses, GET /enrollments) throughout.
//  4. Assert while Kafka is down. Start Kafka again.
//  5. Wait for the outbox to drain.
//
// Assertions:
//   - While down: every enroll is 201 and every read is 200 (the API does not
//     depend on Kafka; /health/ready for course-api stays 200 because Kafka is
//     a non-fatal check). outbox rows for the 20 events are unpublished and
//     outbox_backlog on /metrics rises to at least 20;
//     outbox_oldest_unpublished_seconds grows.
//   - After Kafka returns: outbox_backlog returns to 0 within a bounded time;
//     every event is on its topic exactly once (the same outbox id seen once
//     per topic, key preserved, per-aggregate order preserved); analytics
//     counters moved by exactly 20, not 40 (idempotency absorbed any
//     redelivery); nothing lost.
//
// Helper needed: container stop/start control, /metrics scraper (Phases 3, 7).
func TestE2E_E05_KafkaDownThenUp(t *testing.T) {
	t.Skip("pending: Phase 3 - Kafka producer in the relay; outbox_backlog metric from Phase 7")
}

// TestE2E_E06_ConsumerKilledMidFlow is E6, matrix row E06 (Phase 3).
//
// Steps:
//  1. Run the worker (relay, consumers, Asynq server) as a separate OS
//     process, so it can be killed with SIGKILL (kill -9), not a graceful
//     stop. A test-only fault hook (for example an environment variable that
//     makes the handler block after its database write and before the
//     transaction commits) lets the test kill the process at that exact point.
//  2. Enroll a student; wait until the worker is inside the hook.
//  3. kill -9 the worker. Start a new worker process without the hook.
//
// Assertions:
//   - The enrollment event is eventually consumed by every consumer: the
//     analytics counters moved by exactly 1, one Mailpit mail, one archive
//     document: no lost effect.
//   - processed_events has exactly one row per (consumer, event id): no
//     double effect.
//   - The uncommitted transaction left nothing behind (the effect and its
//     processed_events row are atomic, RFC 6.6).
//   - Repeat with the kill after the commit and before the offset commit: the
//     redelivery is a no-op.
//
// Helper needed: worker as a subprocess with a fault hook (Phase 3).
func TestE2E_E06_ConsumerKilledMidFlow(t *testing.T) {
	t.Skip("pending: Phase 3 - Kafka consumer runner; worker runnable as a killable subprocess with a fault hook")
}

// TestE2E_E07_SMTPProviderDown is E7, matrix row E07 (Phase 5).
//
// Steps:
//  1. Stack as E01 with Mailpit as the SMTP sink and short Asynq retry
//     backoff (for example 1 s base) configured for the test.
//  2. Stop Mailpit (or make the provider return a temporary failure). Enroll
//     three students.
//  3. Observe for several retry intervals. Start Mailpit again.
//
// Assertions:
//   - The enroll requests are all 201 and the Kafka offsets for the
//     notification consumer advance (the consumer enqueues and commits at
//     once, ADR-0014): delivery trouble does not block event processing.
//   - While down: asynq_queue_depth (or the Asynq inspector) shows the three
//     jobs pending or retrying; asynq_retry_count increases; attempts are
//     spaced with growing delay (record the timestamps from job logs or
//     inspector and assert each gap is at least the previous one).
//   - After Mailpit returns: all three mails arrive; the number of mails per
//     student is exactly 1 (a delivery record prevents a repeat send); queue
//     depth returns to 0.
//
// Helper needed: Mailpit client and start/stop control, Asynq inspector (Phase 5).
func TestE2E_E07_SMTPProviderDown(t *testing.T) {
	t.Skip("pending: Phase 5 - Asynq notification delivery with provider interface and Mailpit")
}
