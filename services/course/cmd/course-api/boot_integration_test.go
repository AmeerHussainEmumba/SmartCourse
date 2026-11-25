//go:build integration

// What this file checks: course-api boots through libs/runtime against real
// Postgres (as the svc_course role) and Redis; the public port serves the API
// routes and health but NEVER /metrics, while the internal listener serves
// /metrics and readiness; shutdown on cancel is clean.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
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

func bootConfig(t *testing.T, env *testenv.Env, publicAddr, internalAddr string) *config.Config {
	t.Helper()
	_, port, _ := net.SplitHostPort(publicAddr)
	var p int
	fmt.Sscan(port, &p)
	return &config.Config{
		Env:      "test",
		HTTPPort: p,
		Postgres: config.PostgresConfig{DSN: env.ServiceDSN(t, "course"), MaxOpenConns: 5, MaxIdleConns: 2, SearchPath: "course,public"},
		Redis:    config.RedisConfig{Addr: env.RDB.Options().Addr},
		JWT: config.JWTConfig{
			SigningKey: "test-signing-key-at-least-32-bytes-long!!", Issuer: "smartcourse-test", AccessTokenTTL: 15 * time.Minute,
		},
		Temporal:  config.TemporalConfig{HostPort: "127.0.0.1:1", Namespace: "default", TaskQueue: "publish-workflows", ActivityQueue: "course-activities"},
		Telemetry: config.TelemetryConfig{MetricsAddr: internalAddr, MetricsEnabled: true, SampleRatio: 1},
	}
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

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestBoot_PublicPortServesTheAPIButNotMetrics_InternalListenerDoes(t *testing.T) {
	env := testenv.Setup(t)
	public, internal := freeAddr(t), freeAddr(t)
	cfg := bootConfig(t, env, public, internal)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, "course-api", wire,
			runtime.WithConfig(func() (*config.Config, error) { return cfg, nil }), runtime.WithOutbox(),
			runtime.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	}()
	t.Cleanup(cancel)

	eventually(t, "internal readiness", func() bool {
		code, body := get(t, "http://"+internal+"/health/ready")
		return code == 200 && strings.Contains(body, `"postgres":"ok"`) && strings.Contains(body, `"redis":"ok"`)
	})
	eventually(t, "the public listener", func() bool {
		code, _ := get(t, "http://"+public+"/api/v1/certificates")
		return code == http.StatusUnauthorized // a real route, guarded
	})

	// Public port: no /metrics, whatever the metrics setting.
	if code, body := get(t, "http://"+public+"/metrics"); code != http.StatusNotFound {
		t.Fatalf("public GET /metrics = %d, want 404 (body %.200q)", code, body)
	}
	// Internal listener: /metrics with the request series and the outbox gauges.
	eventually(t, "metrics on the internal listener", func() bool {
		code, body := get(t, "http://"+internal+"/metrics")
		return code == 200 && strings.Contains(body, "http_requests_total") && strings.Contains(body, "outbox_backlog")
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("course-api did not shut down")
	}
}

// The router itself carries no /metrics route even with metrics enabled, so
// the guarantee does not depend on a listener being up.
func TestNewRouter_HasNoMetricsRoute(t *testing.T) {
	env := testenv.Setup(t)
	cfg := bootConfig(t, env, "127.0.0.1:0", "127.0.0.1:0")
	deps := &infra.Dependencies{DB: env.ServiceDB(t, "course"), RDB: env.RDB}
	obs := deps.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "course-api", Outbox: true})
	t.Cleanup(func() { obs.Shutdown(context.Background()) })
	if obs.MetricsHandler() == nil {
		t.Fatal("test setup: metrics are not enabled, the check below would be vacuous")
	}

	r, err := newRouter(&runtime.Process{Config: cfg, Deps: deps, Obs: obs}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, ri := range r.Routes() {
		if strings.Contains(ri.Path, "metrics") {
			t.Errorf("public router has %s %s", ri.Method, ri.Path)
		}
	}
}
