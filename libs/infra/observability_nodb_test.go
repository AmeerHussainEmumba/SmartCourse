package infra_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
)

// A binary with no database (libs/runtime WithoutDependencies, the publishing
// worker) still gets telemetry: Observe on a nil or empty Dependencies must not
// touch a database, exports the Go runtime and RED series, and no outbox or db_*
// series even when asked for the outbox gauges.
func TestObserve_WithoutADatabase(t *testing.T) {
	cfg := &config.Config{Env: "test", Telemetry: config.TelemetryConfig{SampleRatio: 1, MetricsEnabled: true, MetricsAddr: ":9100", OTLPEndpoint: "collector:4318"}}
	for name, d := range map[string]*infra.Dependencies{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			exp := tracetest.NewInMemoryExporter()
			obs := d.Observe(context.Background(), cfg, infra.ObserveOptions{Service: "publishing-worker", Outbox: true, SpanExporter: exp})
			defer obs.Shutdown(context.Background())
			if !obs.TracingEnabled() || obs.Metrics == nil {
				t.Fatalf("tracing=%v metrics=%v, want both on", obs.TracingEnabled(), obs.Metrics != nil)
			}
			rec := httptest.NewRecorder()
			obs.MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
			b, _ := io.ReadAll(rec.Body)
			body := string(b)
			if !strings.Contains(body, "go_goroutines") {
				t.Error("no Go runtime metrics")
			}
			for _, absent := range []string{"outbox_backlog", "db_connections_in_use"} {
				if strings.Contains(body, "# HELP "+absent+" ") {
					t.Errorf("%s exported by a binary with no database", absent)
				}
			}
		})
	}
}
