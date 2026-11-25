//go:build integration

package e2e_test

// The tests in the scenarios_*_e2e_test.go files are the executable form of
// docs/plans/week-3.md section 5 (E1 to E14). They began as skipped specs whose
// doc comment is the exact procedure; E01 and E03 to E07 are implemented on the
// ten-binary harness, E02 and E08 to E14 are pending (Wave C3b), and the comment
// stays above each test. Rules for all of them:
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

// E2 (TestE2E_E02_OneTraceAcrossPipeline) is pending on the ten-binary harness
// (Wave C3b): a skipped stub in scenarios_pending_e2e_test.go, the spec and the
// old body in legacy/scenarios_trace_e2e_test.go.
