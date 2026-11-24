//go:build integration

// Observability of the Asynq side against a real Redis: the queue gauges come
// from the Inspector, and a running task gets a span and a job_id logger.
package asynqx_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestInspectorGauges_ReportPendingScheduledAndRetry(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)
	c := newClient(t, redis)
	m, err := telemetry.NewMetrics(telemetry.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}

	mustEnqueue(t, c, asynqx.Job{Type: "t", ID: "a", Queue: asynqx.QueueCritical})
	mustEnqueue(t, c, asynqx.Job{Type: "t", ID: "b", Queue: asynqx.QueueCritical})
	mustEnqueue(t, c, asynqx.Job{Type: "t", ID: "c", Queue: asynqx.QueueLow, ProcessIn: time.Hour})

	if err := newInspector(t, redis).Gauges(m).Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for q, want := range map[string]float64{asynqx.QueueCritical: 2, asynqx.QueueDefault: 0, asynqx.QueueLow: 1} {
		if got := testutil.ToFloat64(m.AsynqQueueDepth.WithLabelValues(q)); got != want {
			t.Errorf("asynq_queue_depth{queue=%s} = %v, want %v", q, got, want)
		}
		if got := testutil.ToFloat64(m.AsynqRetryCount.WithLabelValues(q)); got != 0 {
			t.Errorf("asynq_retry_count{queue=%s} = %v, want 0", q, got)
		}
	}

	// A task that fails once waits in the retry set.
	env2 := newServers(t, asynqx.ServersConfig{
		Redis: redis, Pools: map[string]int{asynqx.QueueDefault: 1},
		Backoff: asynqx.BackoffConfig{Base: time.Hour, Max: time.Hour}, DelayedTaskCheckInterval: time.Hour,
	})
	failed := make(chan struct{}, 1)
	mustRegister(t, env2, "flaky", asynqx.QueueDefault, func(context.Context, []byte) error {
		select {
		case failed <- struct{}{}:
		default:
		}
		return context.DeadlineExceeded
	})
	runServers(t, env2)
	mustEnqueue(t, c, asynqx.Job{Type: "flaky", ID: "f", Queue: asynqx.QueueDefault, MaxRetry: 3})
	<-failed
	deadline := time.Now().Add(5 * time.Second)
	for {
		_ = newInspector(t, redis).Gauges(m).Collect(context.Background())
		if testutil.ToFloat64(m.AsynqRetryCount.WithLabelValues(asynqx.QueueDefault)) == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := testutil.ToFloat64(m.AsynqRetryCount.WithLabelValues(asynqx.QueueDefault)); got != 1 {
		t.Errorf("asynq_retry_count{queue=default} = %v, want 1", got)
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func TestServers_TaskGetsSpanAndJobIDLogger(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)
	exp := tracetest.NewInMemoryExporter()
	tp, err := telemetry.SetupWithExporter(telemetry.Config{Enabled: true, ServiceName: "test", SampleRatio: 1}, exp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	var logs syncBuf
	ss := newServers(t, asynqx.ServersConfig{
		Redis: redis, Pools: map[string]int{asynqx.QueueDefault: 1},
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	done := make(chan struct{})
	mustRegister(t, ss, "greet", asynqx.QueueDefault, func(ctx context.Context, _ []byte) error {
		logging.Correlated(ctx).Info("handler ran")
		close(done)
		return nil
	})
	runServers(t, ss)
	mustEnqueue(t, newClient(t, redis), asynqx.Job{Type: "greet", ID: "job-42", Queue: asynqx.QueueDefault})
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("task did not run")
	}
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, s := range exp.GetSpans() {
		if s.Name != "greet process" {
			continue
		}
		for _, a := range s.Attributes {
			if a.Key == "smartcourse.job.id" && a.Value.AsString() == "job-42" {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("no span for the task with job id job-42; spans: %d", len(exp.GetSpans()))
	}
	out := logs.String()
	if !strings.Contains(out, `"job_id":"job-42"`) || !strings.Contains(out, `"trace_id"`) {
		t.Errorf("handler log line lacks job_id and trace_id: %s", out)
	}
}
