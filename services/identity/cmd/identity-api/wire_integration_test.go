//go:build integration

package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// startAPI runs the real binary wiring (runtime.Run + wire) against real
// Postgres (as svc_identity) and Redis, on free ports, and returns the public
// and internal base URLs. It stops the process at cleanup.
func startAPI(t *testing.T, env *testenv.Env) (public, internal string) {
	t.Helper()
	pubAddr, intAddr := freeAddr(t), freeAddr(t)
	_, port, _ := net.SplitHostPort(pubAddr)
	t.Setenv("POSTGRES_DSN", env.ServiceDSN(t, "identity"))
	t.Setenv("POSTGRES_SEARCH_PATH", "identity,public")
	t.Setenv("REDIS_ADDR", env.RDB.Options().Addr)
	t.Setenv("MINIO_ENDPOINT", "unused:9000")
	t.Setenv("MINIO_ACCESS_KEY_ID", "unused")
	t.Setenv("MINIO_SECRET_ACCESS_KEY", "unused")
	t.Setenv("JWT_SIGNING_KEY", "integration-test-signing-key-32-bytes!!")
	t.Setenv("ARGON2_MEMORY_KIB", "8192")
	t.Setenv("ARGON2_ITERATIONS", "1")
	t.Setenv("ARGON2_PARALLELISM", "1")
	t.Setenv("HTTP_PORT", port)
	t.Setenv("METRICS_HTTP_ADDR", intAddr)
	t.Setenv("OTEL_SDK_DISABLED", "true")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, "identity-api", wire, runtime.WithOutbox()) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("identity-api did not stop")
		}
	})
	public, internal = "http://"+pubAddr, "http://"+intAddr
	deadline := time.Now().Add(30 * time.Second)
	for {
		if code, _ := get(t, internal+"/health/ready"); code == 200 {
			if code, _ := get(t, public+"/health/live"); code == 200 {
				return public, internal
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("identity-api never became ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The public port serves the API and its health routes but never /metrics; the
// internal listener serves /metrics and readiness with Postgres and Redis.
func TestIdentityAPI_MetricsOnlyOnTheInternalListener(t *testing.T) {
	env := testenv.Setup(t)
	public, internal := startAPI(t, env)

	if code, _ := get(t, public+"/metrics"); code != http.StatusNotFound {
		t.Errorf("public /metrics = %d, want 404", code)
	}
	if code, body := get(t, internal+"/metrics"); code != 200 || !strings.Contains(body, "db_connections_in_use") {
		t.Errorf("internal /metrics = %d, body lacks db_connections_in_use", code)
	}
	code, body := get(t, internal+"/health/ready")
	var ready struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal([]byte(body), &ready); err != nil || code != 200 || ready.Checks["postgres"] != "ok" || ready.Checks["redis"] != "ok" {
		t.Errorf("internal ready = %d %s", code, body)
	}
	// The internal listener serves nothing of the API, and the public port keeps its routes.
	if code, _ := get(t, internal+"/api/v1/auth/login"); code != http.StatusNotFound {
		t.Errorf("internal listener served an API route: %d", code)
	}
	resp, err := http.Post(public+"/api/v1/auth/register", "application/json",
		strings.NewReader(`{"email":"wire@example.com","password":"correct horse battery","full_name":"Wire"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("public register = %d, want 202", resp.StatusCode)
	}
}

// The token-version re-seeder moved to identity-worker: after a Redis flush the
// API process must NOT repair the keys (F-38 is the worker's job now).
func TestIdentityAPI_DoesNotRunTheTokenVersionReseeder(t *testing.T) {
	env := testenv.Setup(t)
	db := env.ServiceDB(t, "identity")
	id := uuid.New()
	if err := db.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status, token_version)
		VALUES (?, 'tv@example.com', 'x', 'TV', 'instructor', 'active', 3)`, id).Error; err != nil {
		t.Fatal(err)
	}
	key := auth.TokenVersionRedisKey(id.String())
	startAPI(t, env)

	// The re-seeder, were it here, writes at start (and its tick is 15 s): give it ample time.
	time.Sleep(3 * time.Second)
	if n, err := env.RDB.Exists(context.Background(), key).Result(); err != nil || n != 0 {
		t.Errorf("token version key written by identity-api (exists=%d, err=%v): the re-seeder must run in identity-worker only", n, err)
	}
}
