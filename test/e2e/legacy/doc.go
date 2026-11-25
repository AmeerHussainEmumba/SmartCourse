// Package legacy is a holding directory, not a test package that runs.
//
// The files here are the scenarios written for the old four-binary harness
// (one worker, one database connection) that Wave C3a has not ported to the
// ten-binary harness yet: E02 (trace), E08 to E14 and the adversarial
// reproducers. They are behind the build tag e2e_legacy, which nothing sets,
// so they are never compiled or run, and they do not compile against the new
// harness (KillWorker, WorkerMetrics, WorkerAdminURL, EventTransport,
// platform.outbox and the single database search path are gone).
//
// Why they are kept: each file's doc comment is the exact procedure of its
// scenario, and the body is the starting point of the port. Wave C3b moves a
// file back to test/e2e (retag it `e2e`, port the body to the new harness,
// delete the skip stub of the same scenario in
// test/e2e/scenarios_pending_e2e_test.go and the line in the allowlist of
// scripts/check-pending-e2e.sh) and deletes it from here. The helpers file
// (helpers_e2e_test.go) goes back with the first scenario that needs it.
package legacy
