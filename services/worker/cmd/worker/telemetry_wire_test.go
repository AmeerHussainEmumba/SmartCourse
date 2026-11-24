package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

type fakeParkStore struct{ err error }

func (f fakeParkStore) ParkFailedEvent(context.Context, mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	return &mongostore.FailedEvent{}, f.err
}

func TestCountParked_CountsOnlySuccessfulParks(t *testing.T) {
	m, err := telemetry.NewMetrics(telemetry.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	ok := countParked(fakeParkStore{}, m)
	for i := 0; i < 3; i++ {
		if _, err := ok.ParkFailedEvent(context.Background(), mongostore.ParkInput{Consumer: "analytics"}); err != nil {
			t.Fatal(err)
		}
	}
	bad := countParked(fakeParkStore{err: errors.New("mongo down")}, m)
	if _, err := bad.ParkFailedEvent(context.Background(), mongostore.ParkInput{Consumer: "analytics"}); err == nil {
		t.Fatal("the store's error was swallowed")
	}
	if got := testutil.ToFloat64(m.DeadLetterTotal.WithLabelValues("analytics")); got != 3 {
		t.Errorf("dead_letter_total{consumer=analytics} = %v, want 3 (a failed park is not a dead letter)", got)
	}
}

func TestCountParked_NilMetricsReturnsStoreUnchanged(t *testing.T) {
	inner := fakeParkStore{}
	if got := countParked(inner, nil); got != any(inner) {
		t.Errorf("with metrics disabled the store must not be wrapped, got %T", got)
	}
}

func TestMetricsServer_ServesOnlyMetrics(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("up 1\n")) })
	srv := newMetricsServer(":0", h)
	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()

	for path, want := range map[string]int{"/metrics": 200, "/": 404, "/health/live": 404, "/api/v1/admin/failed-events": 404} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
	}
	if srv.ReadHeaderTimeout == 0 || srv.WriteTimeout == 0 {
		t.Error("metrics server has no timeouts")
	}
}

// A port that is already taken must not stop the worker: serveMetrics logs and
// returns.
func TestServeMetrics_BindFailureReturnsWithoutPanic(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	srv := newMetricsServer(l.Addr().String(), http.NotFoundHandler())

	done := make(chan struct{})
	go func() {
		serveMetrics(context.Background(), slog.Default(), srv)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveMetrics did not return after a bind failure")
	}
}

func TestServeMetrics_StopsWhenContextIsCancelled(t *testing.T) {
	srv := newMetricsServer("127.0.0.1:0", http.NotFoundHandler())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		serveMetrics(ctx, slog.Default(), srv)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveMetrics did not stop after cancel")
	}
}
