//go:build integration

package outbox_test

import (
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// setupCourse starts the test containers and returns an Env whose DB is the
// course service's own role connection (search_path "course,public"), so the
// unqualified outbox these tests use resolves to course.outbox exactly as it
// does in the course binaries. The owner connection would not resolve it.
func setupCourse(t *testing.T) *testenv.Env {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	return env
}
