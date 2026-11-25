//go:build e2e

package e2e_test

import "testing"

// Scenarios not yet ported to the ten-binary harness (Wave C3a ported E01 and
// E03 to E07, the F-38 test and the data-independence test; Wave C3b ports
// these). Each is a skipped spec, not a missing one: `make test-e2e` lists it
// as SKIP, scripts/check-pending-e2e.sh allowlists it with its owning phase
// and fails the day it is ported and the allowlist line is still there.
//
// The spec (the exact procedure) and the body written for the old four-binary
// harness are in test/e2e/legacy/. To port one: move its file back here,
// retag it `e2e`, rewrite the body on the new harness (see test/README.md,
// "How to extend the harness"), delete its stub below and its line in the
// allowlist of scripts/check-pending-e2e.sh, and update its matrix row.

// E10: Redis flush with the new services (legacy/scenarios_redis_e2e_test.go).
func TestE2E_E10_RedisFlushWithNewServices(t *testing.T) {
	t.Skip("pending: Wave C3b - port to the ten-binary harness; spec and old body in test/e2e/legacy/scenarios_redis_e2e_test.go")
}

// E11: schema incompatibility (legacy/scenarios_schema_e2e_test.go).
func TestE2E_E11_SchemaIncompatibility(t *testing.T) {
	t.Skip("pending: Wave C3b - port to the ten-binary harness; spec and old body in test/e2e/legacy/scenarios_schema_e2e_test.go")
}

// E12: publish under fault (legacy/scenarios_publish_e2e_test.go).
func TestE2E_E12_PublishUnderFault(t *testing.T) {
	t.Skip("pending: Wave C3b - port to the ten-binary harness; spec and old body in test/e2e/legacy/scenarios_publish_e2e_test.go")
}

// E13: archive and audit (legacy/scenarios_archive_e2e_test.go).
func TestE2E_E13_ArchiveAndAudit(t *testing.T) {
	t.Skip("pending: Wave C3b - port to the ten-binary harness; spec and old body in test/e2e/legacy/scenarios_archive_e2e_test.go")
}

// E14: auth and access on the new surface (legacy/scenarios_access_e2e_test.go).
func TestE2E_E14_AuthAndAccessOnNewSurface(t *testing.T) {
	t.Skip("pending: Wave C3b - port to the ten-binary harness; spec and old body in test/e2e/legacy/scenarios_access_e2e_test.go")
}
