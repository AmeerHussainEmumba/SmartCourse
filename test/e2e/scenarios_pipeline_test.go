//go:build integration

package e2e_test

import "testing"

// The tests in the scenarios_*_test.go files are the executable form of
// docs/plans/week-3.md section 5 (E1 to E14). Each is a skipped spec today:
// its doc comment is the exact procedure, so the engineer who builds the
// phase it names can implement it without guessing. Rules for all of them:
//
//   - Black box. HTTP in; assertions on HTTP responses, Kafka topics, MongoDB,
//     Redis, Mailpit, Jaeger and /metrics out. No calls into service packages.
//   - Real Postgres, Redis, Kafka, Schema Registry, MongoDB and Temporal via
//     testcontainers, with the services run from their real wiring. Helpers
//     that do not exist yet (a Kafka/Mongo/Mailpit/Jaeger harness, a way to
//     start the worker and consumers as separate processes, a Kafka reader)
//     come from the phase named in the skip message and are described in each
//     comment as "helper needed".
//   - No sleeps: wait on a condition with a deadline (poll, then fail with the
//     last observed value).
//   - Mutation check once implemented: break the behaviour the test guards,
//     confirm the test fails, restore it, and record both runs in the
//     acceptance matrix (docs/plans/week-3-acceptance-matrix.md, Evidence).
//
// Tracking: grep -rn 't.Skip("pending:' test/e2e lists every spec still
// pending, with its phase. A spec is done when its Skip line is gone and the
// matrix row names the passing run. See test/README.md.

// TestE2E_E01_HappyPath is E1, matrix row E01 (delivered by Phase 8, needs
// Phases 2 to 7).
//
// Steps:
//  1. Start identity-api, course-api, analytics-api and the worker (relay with
//     EVENT_TRANSPORT=kafka, all consumers, Asynq server) against fresh
//     containers; Mailpit is the SMTP sink.
//  2. Register an admin-promoted instructor and two students through the API
//     (register 202, login 200, admin PATCH /admin/users/{id}/role, re-login).
//  3. Instructor creates a course with one module and two lessons and
//     POST /courses/{id}/publish (202); poll publish-status until "ready".
//  4. Student A POST /enrollments (201), then PUT the completion of both
//     lessons (200 each).
//  5. Student B enrolls then DELETE /enrollments/{id} (withdraw).
//
// Assertions (each polled with a deadline):
//   - publish-status is "ready"; course.published_version_id is set.
//   - course.enrollments, course.progress rows exist for A; a certificate row
//     exists for A exactly once (RFC 8.4) after the final lesson.
//   - Kafka: one message per event on identity.events, course.events,
//     enrollment.events (StudentEnrolled, StudentWithdrawn) and
//     progress.events (LessonCompleted x2, CourseCompleted, CertificateIssued),
//     each keyed as RFC 6.5 says, each carrying a schema id.
//   - analytics: all nine metrics from the analytics API reflect the above
//     (total students 2 more than before, completion rate 1 of 2 enrolled and
//     so on; the baseline of each metric is read first and the deltas asserted).
//   - MongoDB events collection contains every event id once.
//   - Mailpit holds the enrollment mail for A and B and a completion mail for A.
//
// Helper needed: stack harness that starts the real wiring (Phase 8), Kafka
// topic reader, Mongo and Mailpit clients (Phases 3, 4, 5).
func TestE2E_E01_HappyPath(t *testing.T) {
	t.Skip("pending: Phase 8 (needs Phases 2-7) - full pipeline harness, Kafka/Mongo/Mailpit readers, nine metrics, certificates")
}

// TestE2E_E02_OneTraceAcrossPipeline is E2, matrix row E02 (Phase 7, ADR-0032).
//
// Steps:
//  1. Same stack as E01 plus Jaeger (OTLP) with tracing enabled and sampling 1.0.
//  2. Student enrolls with a request carrying a known W3C traceparent header
//     (trace id T chosen by the test).
//  3. Wait until the enrollment notification is delivered to Mailpit.
//
// Assertions:
//   - The outbox row written by the request has trace_id = T and a non-empty
//     span_id (new Phase 2 migration).
//   - GET Jaeger /api/traces/T returns one trace whose spans include, by
//     operation name: the HTTP server span for POST /enrollments, the outbox
//     write, the relay publish, the Kafka consume for each consumer that
//     handled StudentEnrolled (analytics, notification, audit) and the Asynq
//     delivery job. All spans share T; parent/child links form one tree (no
//     orphan root other than the HTTP span).
//   - An errored span is exported even when sampling is lowered (errors always
//     sampled): force a consumer error and assert its span is present.
//
// Helper needed: Jaeger query client; tracing-enabled stack (Phase 7).
func TestE2E_E02_OneTraceAcrossPipeline(t *testing.T) {
	t.Skip("pending: Phase 7 - OpenTelemetry through the event envelope, Jaeger harness")
}

// TestE2E_E03_DuplicateDelivery is E3, matrix row E03 (Phase 3).
//
// Steps:
//  1. Stack as E01. Student enrolls; wait until the event is consumed
//     (processed_events has one row per consumer; Mailpit has one mail).
//  2. Read the current analytics counters and the mail count.
//  3. Re-produce the exact same Kafka record (same event id, same key, same
//     bytes) to enrollment.events twice more, bypassing the relay (a test-only
//     producer), simulating at-least-once redelivery.
//  4. Wait until every consumer group's lag is 0.
//
// Assertions:
//   - Analytics counters are unchanged from step 2 (changed exactly once in
//     total relative to before the enrollment).
//   - Mailpit still holds exactly one enrollment mail for the student.
//   - processed_events still has exactly one row per (consumer, event id).
//   - No row in failed_events.
//
// Helper needed: test-only Kafka producer and consumer-lag reader (Phase 3).
func TestE2E_E03_DuplicateDelivery(t *testing.T) {
	t.Skip("pending: Phase 3 - Kafka transport and consumer runner with processed_events idempotency")
}
