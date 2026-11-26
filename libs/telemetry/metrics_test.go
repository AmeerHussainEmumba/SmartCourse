package telemetry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

func newMetrics(t *testing.T) (*prometheus.Registry, *Metrics) {
	t.Helper()
	reg := NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	return reg, m
}

// seriesByFamily counts series per metric family name.
func seriesByFamily(t *testing.T, reg prometheus.Gatherer) map[string]int {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, f := range fams {
		out[f.GetName()] = len(f.GetMetric())
	}
	return out
}

func labelValues(t *testing.T, reg prometheus.Gatherer, family, label string) []string {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range fams {
		if f.GetName() != family {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label {
					out = append(out, l.GetValue())
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

type fakeDB struct{ stats sql.DBStats }

func (f fakeDB) Stats() sql.DBStats { return f.stats }

// TestMetricCatalogue pins the exact names dashboards are built on.
func TestMetricCatalogue(t *testing.T) {
	reg, m := newMetrics(t)
	if err := RegisterDBStats(reg, fakeDB{sql.DBStats{InUse: 3, MaxOpenConnections: 10}}); err != nil {
		t.Fatal(err)
	}
	// Touch every labelled vector so it exposes a series.
	m.HTTPRequests.WithLabelValues("/r", "GET", "200").Inc()
	m.HTTPDuration.WithLabelValues("/r", "GET").Observe(0.01)
	m.KafkaConsumerLag.WithLabelValues("g", "t").Set(1)
	m.AsynqQueueDepth.WithLabelValues("q").Set(1)
	m.AsynqRetryCount.WithLabelValues("q").Set(1)
	m.DeadLettered("c")
	m.EventProcessed("c", OutcomeProcessed)
	m.WorkflowStartedTotal.Inc()
	m.HeldDelta("c", 1)
	m.ParkFailed("c")

	got := seriesByFamily(t, reg)
	want := []string{
		"http_requests_total", "http_request_duration_seconds", "outbox_backlog",
		"outbox_oldest_unpublished_seconds", "kafka_consumer_lag", "asynq_queue_depth",
		"asynq_retry_count", "dead_letter_total", "workflow_started_total",
		"workflow_completed_total", "workflow_failed_total", "db_connections_in_use",
		"db_connections_max", "event_processed_total",
		"dead_letter_held_records", "dead_letter_park_failures_total",
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("metric %s missing", name)
		}
	}
	// Nothing else in our namespace: only the 16 plus runtime/process families.
	for name := range got {
		if strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_") {
			continue
		}
		found := false
		for _, w := range want {
			found = found || w == name
		}
		if !found {
			t.Errorf("unexpected metric %s", name)
		}
	}
}

func TestNewMetricsTwiceDoesNotPanic(t *testing.T) {
	reg := NewRegistry()
	a, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	a.WorkflowStartedTotal.Inc()
	b.WorkflowStartedTotal.Inc() // same underlying collector
	a.HTTPRequests.WithLabelValues("/r", "GET", "200").Inc()
	b.HTTPRequests.WithLabelValues("/r", "GET", "200").Inc()
	if n := seriesByFamily(t, reg)["http_requests_total"]; n != 1 {
		t.Fatalf("series = %d, want 1 shared series", n)
	}
	db := fakeDB{}
	if err := RegisterDBStats(reg, db); err != nil {
		t.Fatal(err)
	}
	if err := RegisterDBStats(reg, db); err != nil {
		t.Fatalf("second RegisterDBStats: %v", err)
	}
}

func TestDBStats(t *testing.T) {
	reg := NewRegistry()
	db := &fakeDB{sql.DBStats{InUse: 4, MaxOpenConnections: 25}}
	if err := RegisterDBStats(reg, db); err != nil {
		t.Fatal(err)
	}
	fams, _ := reg.Gather()
	vals := map[string]float64{}
	for _, f := range fams {
		if strings.HasPrefix(f.GetName(), "db_") {
			vals[f.GetName()] = f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	if vals["db_connections_in_use"] != 4 || vals["db_connections_max"] != 25 {
		t.Fatalf("got %v", vals)
	}
}

func TestEventProcessedBoundsOutcome(t *testing.T) {
	reg, m := newMetrics(t)
	for i := 0; i < 500; i++ {
		m.EventProcessed("c", Outcome(fmt.Sprintf("weird-%d", i)))
	}
	if got := labelValues(t, reg, "event_processed_total", "outcome"); len(got) != 1 || got[0] != "other" {
		t.Fatalf("outcomes = %v", got)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg, m := newMetrics(t)
	r := gin.New()
	r.Use(HTTPMetrics(m))
	r.GET("/metrics", GinHandler(reg))
	r.GET("/ping", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ping", nil))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{
		`http_requests_total{method="GET",route="/ping",status="204"} 1`,
		"http_request_duration_seconds_bucket",
		"outbox_backlog 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics body lacks %q", want)
		}
	}
}

// TestHTTPMetricsCardinality is the guard of ADR-0032: however many distinct
// raw paths, ids and methods arrive, the series count depends only on the
// registered routes. A middleware labelling by raw path fails this test.
func TestHTTPMetricsCardinality(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg, m := newMetrics(t)
	r := gin.New()
	r.Use(HTTPMetrics(m))
	r.GET("/courses/:id", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/courses/:id/lessons/:lesson", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.POST("/enrollments", func(c *gin.Context) { c.Status(http.StatusAccepted) })

	const n = 5000
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i*7919)
		do(r, http.MethodGet, "/courses/"+id)
		do(r, http.MethodGet, fmt.Sprintf("/courses/%s/lessons/%d", id, i))
		do(r, http.MethodGet, fmt.Sprintf("/scanner/probe-%d/.env", i)) // unmatched
		do(r, "BREW"+fmt.Sprint(i%50), "/courses/"+id)                  // unmatched, odd method
	}
	do(r, http.MethodPost, "/enrollments")

	series := seriesByFamily(t, reg)
	for _, fam := range []string{"http_requests_total", "http_request_duration_seconds"} {
		if got := series[fam]; got > 12 {
			t.Errorf("%s has %d series after %d distinct paths; want a small constant", fam, got, 4*n)
		}
	}
	for _, route := range labelValues(t, reg, "http_requests_total", "route") {
		switch route {
		case "/courses/:id", "/courses/:id/lessons/:lesson", "/enrollments", UnmatchedRoute:
		default:
			t.Errorf("unexpected route label %q", route)
		}
	}
	for _, method := range labelValues(t, reg, "http_requests_total", "method") {
		if strings.HasPrefix(method, "BREW") {
			t.Errorf("raw method %q leaked into labels", method)
		}
	}
}

func do(h http.Handler, method, path string) {
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
}

func TestOutboxGauges(t *testing.T) {
	_, m := newMetrics(t)
	c := OutboxGauges(m, func(context.Context) (OutboxStats, error) {
		return OutboxStats{Backlog: 7, OldestUnpublished: 90 * time.Second}, nil
	})
	if err := c.Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v := gaugeValue(t, m.OutboxBacklog); v != 7 {
		t.Errorf("backlog %v", v)
	}
	if v := gaugeValue(t, m.OutboxOldestUnpublish); v != 90 {
		t.Errorf("oldest %v", v)
	}
	// A failing query must leave the last value, not zero it.
	fail := OutboxGauges(m, func(context.Context) (OutboxStats, error) { return OutboxStats{}, errors.New("db down") })
	if err := fail.Collect(context.Background()); err == nil {
		t.Fatal("error swallowed")
	}
	if v := gaugeValue(t, m.OutboxBacklog); v != 7 {
		t.Errorf("failed query changed backlog to %v", v)
	}
}

func TestVectorGaugesDropVanishedSeries(t *testing.T) {
	reg, m := newMetrics(t)
	queues := []QueueStats{{"default", 5, 1}, {"notifications", 2, 0}}
	q := QueueGauges(m, func(context.Context) ([]QueueStats, error) { return queues, nil })
	lag := []ConsumerLag{{"g1", "t1", 10}, {"g1", "t2", 3}}
	l := ConsumerLagGauges(m, func(context.Context) ([]ConsumerLag, error) { return lag, nil })

	ctx := context.Background()
	if err := q.Collect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Collect(ctx); err != nil {
		t.Fatal(err)
	}
	if n := seriesByFamily(t, reg)["asynq_queue_depth"]; n != 2 {
		t.Fatalf("queue series %d", n)
	}
	queues, lag = queues[:1], lag[:1]
	_ = q.Collect(ctx)
	_ = l.Collect(ctx)
	got := seriesByFamily(t, reg)
	if got["asynq_queue_depth"] != 1 || got["asynq_retry_count"] != 1 || got["kafka_consumer_lag"] != 1 {
		t.Fatalf("stale series remain: %v", got)
	}
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(g)
	fams, err := reg.Gather()
	if err != nil || len(fams) != 1 {
		t.Fatalf("gather: %v", err)
	}
	return fams[0].GetMetric()[0].GetGauge().GetValue()
}

func TestPollerRunsAndStops(t *testing.T) {
	var calls atomic.Int32
	stop := StartPoller(context.Background(), 5*time.Millisecond, GaugeCollectorFunc(func(context.Context) error {
		calls.Add(1)
		return errors.New("logged, not fatal")
	}))
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	if calls.Load() < 3 {
		t.Fatalf("collector ran %d times", calls.Load())
	}
	n := calls.Load()
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != n {
		t.Fatal("poller kept running after stop")
	}
}
