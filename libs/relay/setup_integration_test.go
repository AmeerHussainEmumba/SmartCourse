//go:build integration

package relay_test

import (
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// setupCourse starts the test containers and returns an Env whose DB is the
// course service's own role connection (search_path "course,public"), so the
// unqualified outbox, processed_events and idempotency_keys these tests use
// resolve to course's tables exactly as they do in the course binaries. The
// owner connection is not needed here (and would not resolve those names).
func setupCourse(t *testing.T) *testenv.Env {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	return env
}
