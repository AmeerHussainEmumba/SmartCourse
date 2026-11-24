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

// E1 (TestE2E_E01_HappyPath) and E3 (TestE2E_E03_DuplicateDelivery) are implemented in
// scenarios_pipeline_e2e_test.go (build tag e2e).

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
