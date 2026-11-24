//go:build integration

// analytics-api with telemetry wired the way main.go does it. It has no outbox
// access (POSTGRES_SEARCH_PATH is analytics,public), so it must not expose the
// outbox gauges, but it does expose RED by route template and the DB pool.
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestAnalyticsAPI_MetricsByRouteTemplate_NoOutboxGauges(t *testing.T) {
	env := testenv.Setup(t)
	deps := &infra.Dependencies{DB: env.DB, RDB: env.RDB}
	cfg := cfgFor("")
	cfg.Env = "test"
	cfg.Telemetry = config.TelemetryConfig{SampleRatio: 1, MetricsEnabled: true}

	obs := deps.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "analytics-api"}) // Outbox false, as in main.go
	t.Cleanup(func() { obs.Shutdown(context.Background()) })
	r, err := router.NewWithOptions(router.Options{Env: "test", Observe: obs.Middleware()})
	if err != nil {
		t.Fatal(err)
	}
	obs.RegisterMetricsRoute(r)
	closeFn := registerRoutes(r, deps, cfg)
	t.Cleanup(func() { closeFn(context.Background()) })

	tok, err := auth.IssueAccessToken(cfg.JWT, uuid.New(), "admin", 1)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analytics/issues", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	r.ServeHTTP(httptest.NewRecorder(), req)
	courseID := uuid.NewString()
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/analytics/courses/"+courseID+"/summary", nil))

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	b, _ := io.ReadAll(rec.Body)
	body := string(b)

	if !strings.Contains(body, `route="/api/v1/analytics/issues"`) {
		t.Errorf("RED series lack the route template for /analytics/issues:\n%s", body)
	}
	for _, want := range []string{"http_requests_total", "http_request_duration_seconds_bucket", "db_connections_in_use", "go_goroutines"} {
		if !strings.Contains(body, want) {
			t.Errorf("analytics-api /metrics lacks %q", want)
		}
	}
	if strings.Contains(body, "# HELP outbox_backlog") {
		t.Error("analytics-api exposes outbox_backlog although it cannot read the outbox")
	}
	if strings.Contains(body, courseID) {
		t.Error("a raw course id appears in the metrics")
	}
	if err := telemetry.CheckLabels(obs.Gatherer()); err != nil {
		t.Error(err)
	}
}
