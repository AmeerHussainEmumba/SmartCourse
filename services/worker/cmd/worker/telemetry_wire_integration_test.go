//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func metricsTestAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func scrapeMetrics(t *testing.T, addr string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		return 0, ""
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// The worker's own /metrics listener: reachable without credentials on
// METRICS_HTTP_ADDR, carrying the outbox, Asynq and DB series, and gone after
// shutdown.
func TestStartTelemetry_WorkerServesMetricsOnItsOwnListener(t *testing.T) {
	env := testenv.Setup(t)
	addr := metricsTestAddr(t)
	cfg := asynqTestConfig(env.RDB.Options().Addr)
	cfg.Env = "test"
	cfg.Telemetry = config.TelemetryConfig{SampleRatio: 1, MetricsEnabled: true, MetricsAddr: addr}

	ctx, cancel := context.WithCancel(context.Background())
	shutdown := startTelemetry(ctx, slog.Default(), &infra.Dependencies{DB: env.DB, RDB: env.RDB}, cfg)

	var body string
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, b := scrapeMetrics(t, addr)
		body = b
		if code == http.StatusOK && strings.Contains(body, "asynq_queue_depth") && strings.Contains(body, "outbox_backlog") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker /metrics never exposed the series (last status %d):\n%s", code, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, want := range []string{
		`asynq_queue_depth{queue="critical"}`, `asynq_queue_depth{queue="default"}`, `asynq_queue_depth{queue="low"}`,
		`asynq_retry_count{queue="critical"}`, "outbox_backlog 0", "outbox_oldest_unpublished_seconds 0",
		"db_connections_in_use", "go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("worker /metrics lacks %q", want)
		}
	}
	// workflow_* and dead_letter_total are registered (HELP lines), fed by
	// Temporal interceptors and the dead-letter wrapper when they run.
	for _, name := range []string{"workflow_started_total", "workflow_completed_total", "workflow_failed_total"} {
		if !strings.Contains(body, "# HELP "+name) {
			t.Errorf("worker /metrics lacks %s", name)
		}
	}

	cancel()
	shutdown()
	if code, _ := scrapeMetrics(t, addr); code != 0 {
		t.Errorf("the metrics listener is still up after shutdown (status %d)", code)
	}
}

// With METRICS_ENABLED=false the worker exposes nothing and starts nothing.
func TestStartTelemetry_DisabledOpensNoListener(t *testing.T) {
	env := testenv.Setup(t)
	addr := metricsTestAddr(t)
	cfg := asynqTestConfig(env.RDB.Options().Addr)
	cfg.Telemetry = config.TelemetryConfig{MetricsEnabled: false, MetricsAddr: addr}

	ctx, cancel := context.WithCancel(context.Background())
	shutdown := startTelemetry(ctx, slog.Default(), &infra.Dependencies{DB: env.DB, RDB: env.RDB}, cfg)
	time.Sleep(300 * time.Millisecond)
	if code, _ := scrapeMetrics(t, addr); code != 0 {
		t.Errorf("a listener is up on %s with metrics disabled (status %d)", addr, code)
	}
	cancel()
	shutdown()
}
