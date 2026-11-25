//go:build integration

package main

import (
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// setup starts Postgres and Redis and returns an Env whose DB is the analytics
// service's own connection (svc_analytics, search_path "analytics,public"). The
// owner stays in env.Owner for qualified DDL-class fixtures.
func setup(t *testing.T) *testenv.Env {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "analytics")
	return env
}
