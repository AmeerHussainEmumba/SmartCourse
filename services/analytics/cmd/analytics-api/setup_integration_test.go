//go:build integration

package main

import (
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// setup starts the containers and returns an Env whose DB is the analytics
// service's own connection (role svc_analytics, search_path "analytics,public"),
// the one a real binary has. The owner connection stays in env.Owner.
func setup(t *testing.T) *testenv.Env {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "analytics")
	return env
}
