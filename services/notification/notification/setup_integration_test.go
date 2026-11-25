//go:build integration

package notification

import (
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// setup starts the real containers and returns an Env whose DB is the
// notification service's own connection (role svc_notification, search_path
// "notification,public"), the one the binary has: unqualified processed_events
// is notification.processed_events, and any other service's schema is
// "permission denied". env.Owner is still the owner connection, for DDL and
// cross-schema assertions (always schema-qualified).
func setup(t *testing.T) *testenv.Env {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "notification")
	return env
}
