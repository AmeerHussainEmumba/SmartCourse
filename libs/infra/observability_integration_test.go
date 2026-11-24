//go:build integration

package infra_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func depsOf(env *testenv.Env) *infra.Dependencies {
	return &infra.Dependencies{DB: env.DB, RDB: env.RDB}
}

func telemetryCfg(endpoint string, metrics bool) *config.Config {
	return &config.Config{Env: "test", Telemetry: config.TelemetryConfig{
		OTLPEndpoint: endpoint, SampleRatio: 1, MetricsEnabled: metrics, MetricsAddr: ":9100",
	}}
}

// serve builds an engine the way the services do, with one route that touches
// Postgres and Redis through the request context.
func serve(t *testing.T, deps *infra.Dependencies, obs *infra.Observability) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r, err := router.NewWithOptions(router.Options{Env: "test", Observe: obs.Middleware()})
	if err != nil {
		t.Fatal(err)
	}
	obs.RegisterMetricsRoute(r)
	r.GET("/things/:id", func(c *gin.Context) {
		ctx := c.Request.Context()
		var n int
		if err := deps.DB.WithContext(ctx).Raw("SELECT 1").Row().Scan(&n); err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		_ = deps.RDB.Get(ctx, "missing-key").Err() // redis.Nil is a miss, not a failure
		c.JSON(http.StatusOK, gin.H{"n": n})
	})
	return r
}

func get(r http.Handler, path string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestObserve_RequestProducesHTTPDBAndRedisSpansInOneTrace(t *testing.T) {
	env := testenv.Setup(t)
	deps := depsOf(env)
	exp := tracetest.NewInMemoryExporter()
	obs := deps.Observe(context.Background(), telemetryCfg("jaeger:4318", true), infra.ObserveOptions{Service: "test-api", SpanExporter: exp})
	t.Cleanup(func() { obs.Shutdown(context.Background()) })
	if !obs.TracingEnabled() {
		t.Fatal("tracing should be enabled")
	}
	r := serve(t, deps, obs)

	const parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if rec := get(r, "/things/"+uuid.NewString(), "traceparent", parent); rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if err := obs.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}

	var server, dbSpan, redisSpan bool
	for _, s := range exp.GetSpans() {
		if s.SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Errorf("span %q is in trace %s, not the caller's", s.Name, s.SpanContext.TraceID())
		}
		for _, a := range s.Attributes {
			switch {
			case a.Key == "http.route" && a.Value.AsString() == "/things/:id":
				server = true
			case a.Key == "db.system.name" && a.Value.AsString() == "postgresql":
				dbSpan = true
			case a.Key == "db.system.name" && a.Value.AsString() == "redis":
				redisSpan = true
			}
		}
		if strings.Contains(s.Name, "missing-key") {
			t.Errorf("redis key leaked into span name %q", s.Name)
		}
	}
	if !server || !dbSpan || !redisSpan {
		t.Errorf("want HTTP, DB and Redis spans; got server=%v db=%v redis=%v (%d spans)", server, dbSpan, redisSpan, len(exp.GetSpans()))
	}
}

func TestObserve_MetricsUseRouteTemplatesAndExposeCatalogue(t *testing.T) {
	env := testenv.Setup(t)
	deps := depsOf(env)
	if err := env.DB.Exec(`INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, occurred_at)
		VALUES (?, 'x', ?, 'x.y', 1, '{}', now() - interval '90 seconds')`, uuid.New(), uuid.New()).Error; err != nil {
		t.Fatal(err)
	}
	obs := deps.Observe(context.Background(), telemetryCfg("", true), infra.ObserveOptions{Service: "test-api", Outbox: true})
	t.Cleanup(func() { obs.Shutdown(context.Background()) })
	r := serve(t, deps, obs)

	id := uuid.NewString()
	get(r, "/things/"+id)
	get(r, "/nope/"+id)

	var body string
	deadline := time.Now().Add(10 * time.Second)
	for {
		rec := get(r, "/metrics")
		b, _ := io.ReadAll(rec.Body)
		body = string(b)
		if strings.Contains(body, "outbox_backlog 1") || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, want := range []string{
		`http_requests_total{method="GET",route="/things/:id",status="200"} 1`,
		`http_requests_total{method="GET",route="unmatched",status="404"} 1`,
		"http_request_duration_seconds_bucket",
		"db_connections_in_use",
		"db_connections_max",
		"outbox_backlog 1",
		"outbox_oldest_unpublished_seconds",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if strings.Contains(body, id) {
		t.Error("a raw request id leaked into a metric label")
	}
}

func TestObserve_DisabledIsInert(t *testing.T) {
	env := testenv.Setup(t)
	deps := depsOf(env)
	cfg := telemetryCfg("", false) // no endpoint, metrics off
	obs := deps.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "test-api"})
	if obs.TracingEnabled() || obs.Metrics != nil || len(obs.Middleware()) != 0 || obs.MetricsHandler() != nil {
		t.Fatalf("disabled telemetry must be inert: %+v", obs)
	}
	if env.DB.Callback().Query().Get("smartcourse:trace_before_query") != nil {
		t.Error("the GORM tracing hook is registered although tracing is off")
	}
	r := serve(t, deps, obs)
	if rec := get(r, "/things/1"); rec.Code != http.StatusOK {
		t.Errorf("request failed with telemetry off: %d", rec.Code)
	}
	if rec := get(r, "/metrics"); rec.Code != http.StatusNotFound {
		t.Errorf("/metrics answered %d with metrics disabled, want 404", rec.Code)
	}
	obs.Shutdown(context.Background()) // must not panic or block
}

// An unreachable collector must neither block start-up nor requests, and the
// flush at exit is bounded by the caller's deadline.
func TestObserve_UnreachableCollectorNeverBlocks(t *testing.T) {
	env := testenv.Setup(t)
	deps := depsOf(env)

	start := time.Now()
	obs := deps.Observe(context.Background(), telemetryCfg("127.0.0.1:1", true), infra.ObserveOptions{Service: "test-api"})
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Observe took %s with the collector down", d)
	}
	if !obs.TracingEnabled() {
		t.Fatal("tracing should be on (the endpoint is only dialled at export time)")
	}
	r := serve(t, deps, obs)
	start = time.Now()
	for i := 0; i < 20; i++ {
		if rec := get(r, "/things/1"); rec.Code != http.StatusOK {
			t.Fatalf("request %d failed: %d", i, rec.Code)
		}
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("20 requests took %s with the collector down", d)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start = time.Now()
	obs.Shutdown(ctx)
	if d := time.Since(start); d > 4*time.Second {
		t.Errorf("Shutdown took %s with a 2s deadline", d)
	}
}
