//go:build integration

package e2e_test

import "testing"

// Correctness and contract scenarios: E8 to E11. See the header of
// scenarios_pipeline_test.go for the rules every spec follows.

// TestE2E_E08_CounterDrift is E8, matrix row E08 (Phase 5 job, Phase 6 metrics).
//
// Steps:
//  1. Stack as E01 with the analytics reconciliation interval set to 5 s for
//     the test (production is 60 s, ADR-0017).
//  2. Create real data: two students, one published course, one enrollment.
//     Wait until analytics.platform_snapshot shows total_students 2 (plus any
//     baseline) through GET /analytics/platform.
//  3. Corrupt state by hand with SQL against analytics.platform_snapshot:
//     set total_students to 9999 and avg_courses_per_student to 42. Also
//     corrupt analytics.course_stats for the course (enrollment totals to 500).
//  4. Wait up to two reconciliation intervals.
//
// Assertions:
//   - Immediately after step 3 the API shows the corrupted values (proves the
//     test really changed what the API serves).
//   - Within two intervals GET /analytics/platform and the course stats
//     return the values recomputed from the source tables, not the corrupted
//     ones, and the values equal a direct SQL recompute from the mirrors.
//   - The reconciliation job ran as a scheduled Asynq task (visible in the
//     scheduler entries) and logged a drift correction.
//
// Helper needed: Asynq scheduler configurable interval, SQL access to the
// analytics schema in the test container (Phases 5 and 6).
func TestE2E_E08_CounterDrift(t *testing.T) {
	t.Skip("pending: Phase 5/6 - scheduled analytics reconciliation job and recompute from mirrors")
}

// TestE2E_E09_EnrollmentUnderLoad is E9, matrix row E09 (Phase 3).
//
// Steps:
//  1. Stack as E01. Instructor publishes a course with enrollment_limit 25.
//  2. Register and log in 150 students in advance (outside the timed part).
//  3. Release 150 goroutines together (a start barrier) each doing
//     POST /enrollments for the course. Bound the client concurrency and
//     record each status code and latency.
//
// Assertions:
//   - Exactly 25 responses are 201; the other 125 are the seat-limit refusal
//     (the documented status and code); none is a 5xx and none times out.
//   - course.enrollments has exactly 25 active rows for the course, one per
//     student (the partial unique index), and enrollment_count is 25.
//   - After the consumers drain, analytics shows 25 enrollments for the course
//     (not 150, not 24), the archive has exactly 25 StudentEnrolled events,
//     and Mailpit has exactly 25 enrollment mails.
//   - Backpressure is visible: while the burst is processed, the notification
//     queue depth or consumer lag rose above 0 and returned to 0 (assert on
//     the metrics, not on timing).
//   - Run the same test with the in-process transport to show both transports
//     give the same counts (week-3.md D2).
//
// Helper needed: 150 pre-registered users (argon2 parameters low in the test
// config), metrics scraper (Phases 3 and 7).
func TestE2E_E09_EnrollmentUnderLoad(t *testing.T) {
	t.Skip("pending: Phase 3 - Kafka path for the enrollment flow; lag and queue metrics from Phase 7")
}

// TestE2E_E10_RedisFlushWithNewServices is E10, matrix row E10 (Phase 3).
//
// The existing test TestE2E_InstructorAfterRedisRestart_LoginReseedsAndCourseAPIWorks
// (redis_flush_e2e_test.go) stays as it is and keeps running. This spec re-runs
// its scenario with the Week 3 services in the stack.
//
// Steps:
//  1. Stack as E01 (Kafka, workers, Asynq all running, using the same Redis).
//  2. Repeat the existing test's steps: promote a user to instructor, create a
//     course, FLUSHALL on Redis, observe the token_stale refusal, log in again,
//     create a course, FLUSHALL, refresh, demote, FLUSHALL, log in, observe
//     the old token refused.
//  3. While doing so, enroll a student and complete a lesson so the consumers
//     and Asynq are active across the flushes.
//
// Assertions:
//   - Every assertion of the existing test holds unchanged.
//   - The Asynq server recovers after the flush (its queues are Redis keys):
//     a delivery job enqueued after the flush is processed, and the worker
//     process did not exit.
//   - The idempotency and rate-limit state loss does not cause a 5xx anywhere.
//   - No event is lost or applied twice across the flushes (counters match the
//     number of enrollments made).
//
// Helper needed: the stack harness of E01 and the helpers the existing test
// defines (newStack, login, setRole, createCourse in redis_flush_e2e_test.go,
// reuse them, do not copy them).
func TestE2E_E10_RedisFlushWithNewServices(t *testing.T) {
	t.Skip("pending: Phase 3 - the Week 3 services must exist in the stack; asynq recovery from Phase 5")
}

// TestE2E_E11_SchemaIncompatibility is E11, matrix row E11 (Phase 2).
//
// Steps:
//  1. Start Schema Registry (and Kafka) in testcontainers; run the registry
//     bootstrap script (idempotent) and assert it registers every subject.
//  2. Take the current schema of the StudentEnrolled subject from the registry.
//  3. Build a breaking variant (rename a field and renumber another; or
//     remove a required field) and POST it to /compatibility/subjects/<subject>/versions/latest.
//  4. Attempt to register the variant as a new version.
//  5. Run the CI compatibility check (the same command CI runs, for example
//     the script under scripts/ added in Phase 2) against the working tree
//     with the breaking .proto edit applied in a temporary copy.
//
// Assertions:
//   - Step 1: every subject exists and its compatibility level is BACKWARD.
//   - Step 3: is_compatible is false.
//   - Step 4: the registry answers 409 (incompatible schema) and the subject
//     still has the same latest version number.
//   - Step 5: the CI check exits non-zero and names the subject and the
//     breaking change; with no edit it exits 0.
//   - A backward-compatible change (adding an optional field) is accepted:
//     is_compatible true and the version increments.
//   - Old-version payloads still decode with the new schema (round-trip using
//     the codec in libs/events).
//
// Helper needed: Schema Registry client and the Phase 2 bootstrap and check
// scripts. Do not import libs/events until it exists; call the script instead.
func TestE2E_E11_SchemaIncompatibility(t *testing.T) {
	t.Skip("pending: Phase 2 - proto contracts, registry bootstrap script and CI compatibility check")
}
