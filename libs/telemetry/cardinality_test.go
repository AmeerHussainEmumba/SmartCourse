package telemetry

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// populate touches every labelled vector so it appears in a gather.
func populate(m *Metrics) {
	m.HTTPRequests.WithLabelValues("/a/:id", "GET", "200").Inc()
	m.HTTPDuration.WithLabelValues("/a/:id", "GET").Observe(0.01)
	m.KafkaConsumerLag.WithLabelValues("g", "t").Set(1)
	m.AsynqQueueDepth.WithLabelValues("default").Set(1)
	m.AsynqRetryCount.WithLabelValues("default").Set(1)
	m.DeadLettered("c")
	m.EventProcessed("c", OutcomeProcessed)
}

func TestMetricLabelsAreBounded(t *testing.T) {
	reg := NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	populate(m)
	if err := RegisterDBStats(reg, fakeDB{}); err != nil {
		t.Fatal(err)
	}
	if err := CheckLabels(reg); err != nil {
		t.Fatalf("the shipped catalogue uses an unbounded label: %v", err)
	}
}

// The guard has to fail on the mistakes it exists to catch.
func TestCheckLabelsRejectsUnboundedLabels(t *testing.T) {
	for _, label := range []string{"user_id", "course_id", "path", "url", "request_id", "trace_id", "event_id", "job_id", "email"} {
		t.Run(label, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			bad := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bad_total", Help: "x"}, []string{"route", label})
			reg.MustRegister(bad)
			bad.WithLabelValues("/a", "v").Inc()
			err := CheckLabels(reg)
			if err == nil || !strings.Contains(err.Error(), label) || !strings.Contains(err.Error(), "bad_total") {
				t.Fatalf("CheckLabels accepted label %q: %v", label, err)
			}
		})
	}
}

// Gauges fed by collectors must not let a source widen the label set either.
func TestCollectorSeriesStayWithinBoundedLabels(t *testing.T) {
	reg := NewRegistry()
	m, _ := NewMetrics(reg)
	if err := QueueGauges(m, func(context.Context) ([]QueueStats, error) {
		return []QueueStats{{Queue: "critical", Pending: 1}}, nil
	}).Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := ConsumerLagGauges(m, func(context.Context) ([]ConsumerLag, error) {
		return []ConsumerLag{{Group: "g", Topic: "t", Lag: 3}}, nil
	}).Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := CheckLabels(reg); err != nil {
		t.Fatal(err)
	}
}
