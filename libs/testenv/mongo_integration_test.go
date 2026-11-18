//go:build integration

package testenv_test

import (
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestSetupMongoStartsAndStops(t *testing.T) {
	env := testenv.SetupMongo(t)

	u, err := url.Parse(env.URI)
	if err != nil || u.Scheme != "mongodb" {
		t.Fatalf("unexpected URI %q (err %v)", env.URI, err)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	if err != nil {
		t.Fatalf("mongodb not reachable at %s: %v", u.Host, err)
	}
	_ = conn.Close()

	env.Stop(t)
	if conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second); err == nil {
		_ = conn.Close()
		t.Fatalf("mongodb still reachable after Stop")
	}
}
