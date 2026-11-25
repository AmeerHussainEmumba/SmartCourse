package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/temporal"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
)

// queueGaugeInterval is how often the Asynq queue gauges read Redis, so the
// number of Redis calls does not depend on how often Prometheus scrapes.
const queueGaugeInterval = 15 * time.Second

// startTelemetry wires tracing and metrics into the worker (ADR-0032) and
// returns the Observability (the event transport and the dead-letter sink take
// their tracing flag and Metrics from it; it is never nil) and the function
// main defers to stop everything and flush spans. It never fails the process: a metrics port that cannot be bound, a collector that is
// down or an invalid tracing setup are logged and the worker runs without.
//
//   - Tracing and the DB/Redis hooks: deps.Observe (OTEL_EXPORTER_OTLP_ENDPOINT).
//   - Temporal: the tracing interceptor and the workflow_* counters, applied by
//     temporal.Connect to every client built after this call (the publish
//     worker's and the jobs'), so it must run before they are built.
//   - Asynq: asynq_queue_depth and asynq_retry_count from the Inspector.
//   - The internal listener (libs/runtime, shared with every service binary):
//     /metrics, /health/live and /health/ready on METRICS_HTTP_ADDR (default
//     :9100), not the authenticated admin port; Prometheus and the probes carry
//     no credentials. Like before, it is only opened when metrics are enabled.
//
// health is the readiness state the caller keeps adding checks to (main adds the
// non-fatal Kafka check once the transport exists, and marks it started); nil
// means a state that is ready as soon as the checks pass.
func startTelemetry(ctx context.Context, logger *slog.Logger, deps *infra.Dependencies, cfg *config.Config, health *runtime.Health) (o *infra.Observability, shutdown func()) {
	if health == nil {
		health = runtime.NewHealth()
		health.Started()
	}
	o = deps.Observe(ctx, cfg, infra.ObserveOptions{Service: "worker", Outbox: true, Workflows: true})

	temporal.Observe(temporal.Observability{Tracing: o.TracingEnabled(), Metrics: o.Metrics})

	var wg sync.WaitGroup
	stopGauges := func() {}
	if o.Metrics != nil {
		inspector := asynqx.NewInspector(asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB))
		stopPoll := telemetry.StartPoller(ctx, queueGaugeInterval, inspector.Gauges(o.Metrics))
		stopGauges = func() {
			stopPoll()
			_ = inspector.Close()
		}
		for name, check := range deps.HealthCheckers() {
			health.Add(name, runtime.Check(check))
		}
		srv := runtime.NewInternalServer(cfg.Telemetry.MetricsAddr, o.MetricsHandler(), health)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.ServeInternal(ctx, logger, srv)
		}()
	}

	return o, func() {
		wg.Wait() // ServeInternal returns once ctx is cancelled
		stopGauges()
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		o.Shutdown(flushCtx)
	}
}

// parkStore is what deadletter.NewSink needs (deadletter.ParkStore).
type parkStore interface {
	ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error)
}

// countingParkStore counts dead_letter_total{consumer} for every event that was
// really parked (a failed ParkFailedEvent is not a dead letter: the caller
// keeps the event). main wraps the store the Sink is built over, with the
// Metrics startTelemetry returned (mongoWiring.Sink does it):
//
//	deadletter.NewSink(countParked(mg, o.Metrics))
//
// Both dead-letter paths go through that one Sink: the relay giving up on a row
// (in-process handlers, or a publish that kept failing) and a Kafka consumer
// whose handler used its budget.
type countingParkStore struct {
	inner parkStore
	m     *telemetry.Metrics
}

var _ deadletter.ParkStore = countingParkStore{}

// countParked returns store unchanged when metrics are disabled (m is nil).
func countParked(store parkStore, m *telemetry.Metrics) deadletter.ParkStore {
	if m == nil {
		return store
	}
	return countingParkStore{inner: store, m: m}
}

func (c countingParkStore) ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	fe, err := c.inner.ParkFailedEvent(ctx, in)
	if err == nil {
		c.m.DeadLettered(in.Consumer)
	}
	return fe, err
}
