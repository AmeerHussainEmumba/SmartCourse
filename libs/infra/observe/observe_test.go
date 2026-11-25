package observe_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra/observe"
)

func cfg(endpoint string, metrics bool) *config.Config {
	return &config.Config{Env: "test", Telemetry: config.TelemetryConfig{
		OTLPEndpoint: endpoint, SampleRatio: 1, MetricsEnabled: metrics, MetricsAddr: ":9100",
	}}
}

// OnTracing is the binary's hook installer: it runs only when tracing is on, so
// disabled tracing registers nothing on the query path.
func TestNew_OnTracingRunsOnlyWhenTracingIsOn(t *testing.T) {
	for name, tc := range map[string]struct {
		endpoint string
		want     int
	}{"tracing on": {"collector:4318", 1}, "tracing off": {"", 0}} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			obs := observe.New(context.Background(), cfg(tc.endpoint, false), observe.Options{
				Service: "svc", SpanExporter: tracetest.NewInMemoryExporter(), OnTracing: func() { calls++ },
			})
			defer obs.Shutdown(context.Background())
			if calls != tc.want {
				t.Errorf("OnTracing ran %d times, want %d", calls, tc.want)
			}
		})
	}
}

// With no SQLDB the registry carries no db_* series and no outbox gauges even
// when the outbox is asked for (the with-database series are covered by the infra
// integration tests).
func TestNew_DatabaseSeriesComeFromSQLDB(t *testing.T) {
	scrape := func(o observe.Options) string {
		obs := observe.New(context.Background(), cfg("", true), o)
		defer obs.Shutdown(context.Background())
		rec := httptest.NewRecorder()
		obs.MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		b, _ := io.ReadAll(rec.Body)
		return string(b)
	}

	body := scrape(observe.Options{Service: "svc", Outbox: true})
	if !strings.Contains(body, "go_goroutines") {
		t.Error("no Go runtime metrics")
	}
	for _, absent := range []string{"db_connections_in_use", "outbox_backlog"} {
		if strings.Contains(body, "# HELP "+absent+" ") {
			t.Errorf("%s exported without a database", absent)
		}
	}

}

// The inert value is safe to use: no middleware, no handler, nothing to flush.
func TestObservability_ZeroValueIsInert(t *testing.T) {
	var obs *observe.Observability
	if obs.Middleware() != nil || obs.MetricsHandler() != nil || obs.TracingEnabled() {
		t.Error("a nil Observability is not inert")
	}
	obs.Shutdown(context.Background())
}
