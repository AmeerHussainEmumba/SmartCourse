//go:build integration

// analytics-api's telemetry and listeners as deployed (design §6): /metrics is
// served only on the internal listener (METRICS_HTTP_ADDR), never on the public
// API port, and carries RED by route template and the DB pool but no outbox
// gauges (analytics has no outbox).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
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

func httpGet(t *testing.T, url, bearer string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// The public router must not serve /metrics (it was mounted there before the
// split). Mutation check: put obs.RegisterMetricsRoute(r) back into buildRouter
// and this test fails.
func TestAnalyticsAPI_PublicRouterHasNoMetrics(t *testing.T) {
	env := setup(t)
	deps := &infra.Dependencies{DB: env.DB, RDB: env.RDB}
	cfg := cfgFor("")
	cfg.Env = "test"
	cfg.Telemetry = config.TelemetryConfig{SampleRatio: 1, MetricsEnabled: true}
	obs := deps.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "analytics-api"})
	t.Cleanup(func() { obs.Shutdown(context.Background()) })

	r, closeFn, err := buildRouter(cfg, deps, obs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeFn(context.Background()) })
	for _, path := range []string{"/metrics", "/api/v1/metrics"} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("public GET %s: want 404, got %d", path, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("public /health/live: want 200, got %d", rec.Code)
	}
}

// The whole binary under libs/runtime: public port serves the API and no
// /metrics, the internal listener serves /metrics (RED by route template, DB
// pool, no outbox gauges) and readiness with Postgres and Redis.
func TestAnalyticsAPI_RunsUnderRuntime_MetricsOnInternalListenerOnly(t *testing.T) {
	env := setup(t)
	pub, internal := freeAddr(t), freeAddr(t)
	_, port, _ := net.SplitHostPort(pub)
	var portNum int
	if _, err := fmt.Sscanf(port, "%d", &portNum); err != nil {
		t.Fatal(err)
	}
	cfg := cfgFor("")
	cfg.Env = "test"
	cfg.HTTPPort = portNum
	cfg.Postgres = config.PostgresConfig{DSN: env.ServiceDSN(t, "analytics"), MaxOpenConns: 5, MaxIdleConns: 2, SearchPath: "analytics,public"}
	cfg.Redis = config.RedisConfig{Addr: env.RDB.Options().Addr}
	cfg.Telemetry = config.TelemetryConfig{SampleRatio: 1, MetricsEnabled: true, MetricsAddr: internal}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, "analytics-api", wire, runtime.WithConfig(func() (*config.Config, error) { return cfg, nil }))
	}()
	var runErr error
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case runErr = <-done:
			case <-time.After(20 * time.Second):
				runErr = errors.New("analytics-api did not shut down")
			}
		})
	}
	t.Cleanup(stop)

	ready := false
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if code, _ := httpGet(t, "http://"+internal+"/health/ready", ""); code == 200 {
			if code, _ = httpGet(t, "http://"+pub+"/health/live", ""); code == 200 {
				ready = true
				break
			}
		}
	}
	if !ready {
		t.Fatal("analytics-api did not become ready on both listeners")
	}

	if code, _ := httpGet(t, "http://"+pub+"/metrics", ""); code != http.StatusNotFound {
		t.Errorf("public port /metrics: want 404, got %d", code)
	}

	tok, err := auth.IssueAccessToken(cfg.JWT, uuid.New(), "admin", 1)
	if err != nil {
		t.Fatal(err)
	}
	httpGet(t, "http://"+pub+issuesPath, tok)
	courseID := uuid.NewString()
	httpGet(t, "http://"+pub+"/api/v1/analytics/courses/"+courseID+"/summary", "")

	code, body := httpGet(t, "http://"+internal+"/metrics", "")
	if code != 200 {
		t.Fatalf("internal /metrics: %d", code)
	}
	if !strings.Contains(body, `route="/api/v1/analytics/issues"`) {
		t.Errorf("RED series lack the route template for /analytics/issues:\n%.800s", body)
	}
	for _, want := range []string{"http_requests_total", "http_request_duration_seconds_bucket", "db_connections_in_use", "go_goroutines"} {
		if !strings.Contains(body, want) {
			t.Errorf("analytics-api /metrics lacks %q", want)
		}
	}
	if strings.Contains(body, "# HELP outbox_backlog") {
		t.Error("analytics-api exposes outbox_backlog although it has no outbox")
	}
	if strings.Contains(body, courseID) {
		t.Error("a raw course id appears in the metrics")
	}
	_, ready2 := httpGet(t, "http://"+internal+"/health/ready", "")
	if !strings.Contains(ready2, "postgres") || !strings.Contains(ready2, "redis") {
		t.Errorf("readiness lacks postgres/redis: %s", ready2)
	}

	stop()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
}
