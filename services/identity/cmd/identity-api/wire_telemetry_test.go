//go:build integration

// identity-api with telemetry wired the way wire.go does it: a registration
// writes an outbox row carrying the request's trace, and /metrics exposes the
// route-template RED series and the outbox and DB gauges.
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestIdentityAPI_RegisterTracesToOutboxAndExposesMetrics(t *testing.T) {
	env := testenv.Setup(t)
	db := env.ServiceDB(t, "identity")
	deps := &infra.Dependencies{DB: db, RDB: env.RDB}
	cfg := &config.Config{
		Env:   "test",
		JWT:   config.JWTConfig{SigningKey: "integration-test-signing-key-32-bytes!!", AccessTokenTTL: 15 * time.Minute, RefreshTokenTTL: time.Hour, Issuer: "smartcourse-test"},
		Argon: config.Argon2Config{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32},
		Telemetry: config.TelemetryConfig{
			OTLPEndpoint: "jaeger:4318", SampleRatio: 1, MetricsEnabled: true,
		},
	}
	exp := tracetest.NewInMemoryExporter()
	obs := deps.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "identity-api", Outbox: true, OutboxPollInterval: 100 * time.Millisecond, SpanExporter: exp})
	t.Cleanup(func() { obs.Shutdown(context.Background()) })

	r, err := newRouter(cfg, deps, obs)
	if err != nil {
		t.Fatal(err)
	}
	// /metrics is the internal listener's (libs/runtime), not the public router's.
	health := runtime.NewHealth()
	health.Started()
	internal := runtime.NewInternalServer("127.0.0.1:0", obs.MetricsHandler(), health).Handler

	const traceID = "0af7651916cd43dd8448eb211c80319c"
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register",
		strings.NewReader(`{"email":"trace@example.com","password":"correct horse battery","full_name":"Trace Test"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", "00-"+traceID+"-b7ad6b7169203331-01")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("register returned %d: %s", rec.Code, rec.Body)
	}

	var row outbox.Row
	if err := db.Where("event_type = ?", "identity.user_registered").First(&row).Error; err != nil {
		t.Fatalf("no outbox row: %v", err)
	}
	if row.TraceID == nil || *row.TraceID != traceID || row.SpanID == nil {
		t.Errorf("outbox trace/span = %v/%v, want trace %s", row.TraceID, row.SpanID, traceID)
	}

	var body string
	deadline := time.Now().Add(10 * time.Second)
	for {
		m := httptest.NewRecorder()
		internal.ServeHTTP(m, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		b, _ := io.ReadAll(m.Body)
		body = string(b)
		if strings.Contains(body, "outbox_backlog 1") || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, want := range []string{
		`http_requests_total{method="POST",route="/api/v1/auth/register",status="202"} 1`,
		"http_request_duration_seconds_bucket", "outbox_backlog 1", "outbox_oldest_unpublished_seconds",
		"db_connections_in_use", "db_connections_max",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("identity-api internal /metrics lacks %q", want)
		}
	}
	if strings.Contains(body, "trace@example.com") {
		t.Error("an email leaked into /metrics")
	}
	if err := telemetry.CheckLabels(obs.Gatherer()); err != nil {
		t.Error(err)
	}
}
