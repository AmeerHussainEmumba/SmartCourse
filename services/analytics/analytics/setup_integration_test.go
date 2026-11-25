//go:build integration

package analytics_test

import (
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// setup starts the containers and returns an Env whose DB is the analytics
// service's own connection (role svc_analytics, search_path "analytics,public"),
// the one a real binary has. The owner connection stays in env.Owner for
// qualified, cross-schema assertions. The owner's search_path is "public" only,
// so an unqualified analytics table on it would fail (libs/testenv README).
func setup(t *testing.T) *testenv.Env {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "analytics")
	return env
}
