//go:build integration

package idempotency_test

import (
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// setupCourse starts the test containers and returns an Env whose DB is the
// course service's own role connection (search_path "course,public"), so the
// unqualified idempotency_keys these tests use resolves to
// course.idempotency_keys exactly as it does in the course binaries.
func setupCourse(t *testing.T) *testenv.Env {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	return env
}
