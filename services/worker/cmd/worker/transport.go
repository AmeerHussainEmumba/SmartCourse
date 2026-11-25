package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

// transportMode reports the selected event transport, config.TransportInProcess
// or config.TransportKafka (EVENT_TRANSPORT, ADR-0029). Wiring that must exist
// in only one mode asks this, for example the audit archiver, which is a
// consumer of the topics and so is registered only when
// transportMode(cfg) == config.TransportKafka:
//
//	if transportMode(cfg) == config.TransportKafka {
//		rl.Register(auditArchiverHandler)
//	}
//
// Every handler registered on the relay, whichever mode, is what Run starts a
// consumer group for in kafka mode (the group id is its ConsumerName), so a
// kafka-only handler needs nothing else.
func transportMode(cfg *config.Config) string { return cfg.Kafka.Transport }

// eventTransport is the worker's event transport for the selected mode. In
// in-process mode it is inert: it adds only the optional dead-letter sink to the
// relay. In kafka mode it owns the producer the relay publishes through and the
// consumer groups that run the relay's handlers.
type eventTransport struct {
	cfg       *config.Config
	logger    *slog.Logger
	sink      relay.DeadLetterSink
	publisher *eventbus.Publisher
	// obs carries the tracing flag and the metrics the relay observer, the
	// publisher, the consumers and the lag gauges use; an inert value means none.
	obs *infra.Observability
	// lagInterval is how often kafka_consumer_lag asks the brokers.
	lagInterval time.Duration
}

// lagGaugeInterval is how often the consumer lag gauges query the brokers, so
// the broker load does not depend on how often Prometheus scrapes.
const lagGaugeInterval = 15 * time.Second

// newEventTransport prepares the transport. In kafka mode it provisions the
// topics, checks the schema registry (fails when it is unreachable or a subject
// is missing) and builds the producer, so a misconfigured worker stops at start
// instead of failing row by row. sink is the optional dead-letter sink (nil
// keeps log-and-drop, the documented gap); the MongoDB wiring passes its
// failed_events sink here. obs is startTelemetry's Observability (nil is the
// same as an inert one): it switches on the relay and produce/consume spans and
// the event_processed_total and kafka_consumer_lag series.
func newEventTransport(ctx context.Context, logger *slog.Logger, cfg *config.Config, sink relay.DeadLetterSink, obs *infra.Observability) (*eventTransport, error) {
	t := &eventTransport{cfg: cfg, logger: logger, sink: sink, obs: obs, lagInterval: lagGaugeInterval}
	if transportMode(cfg) != config.TransportKafka {
		return t, nil
	}
	k := cfg.Kafka
	schemas, err := eventbus.Prepare(ctx, k.Brokers, k.SchemaRegistryURL)
	if err != nil {
		return nil, fmt.Errorf("kafka transport: %w", err)
	}
	t.publisher, err = eventbus.NewPublisher(eventbus.PublisherConfig{
		Brokers: k.Brokers, ProduceTimeout: k.ProduceTimeout, Schemas: schemas, Tracing: obs.TracingEnabled(),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka transport: %w", err)
	}
	logger.Info("event transport: kafka", slog.Any("brokers", k.Brokers), slog.String("schema_registry", k.SchemaRegistryURL))
	return t, nil
}

// RelayOptions are the options the relay is built with: the publisher in kafka
// mode, the dead-letter sink when there is one, the trace observer when tracing
// is on and the event_processed_total hook when metrics are (the hook only fires
// in-process; kafka consumers count their own).
func (t *eventTransport) RelayOptions() []relay.Option {
	var opts []relay.Option
	if t.obs.TracingEnabled() {
		opts = append(opts, relay.WithObserver(eventbus.RelayObserver()))
	}
	if m := t.metrics(); m != nil {
		opts = append(opts, relay.WithHandlerHook(eventbus.HandlerCounter(m)))
	}
	if t.publisher != nil {
		opts = append(opts, relay.WithPublisher(t.publisher))
	}
	if t.sink != nil {
		opts = append(opts, relay.WithDeadLetterSink(t.sink))
	}
	return opts
}

// Run starts the consumer groups for rl's registered handlers and blocks until
// ctx is cancelled and they have drained. It returns at once in in-process mode.
// Call it after every handler is registered on rl.
func (t *eventTransport) Run(ctx context.Context, rl *relay.Relay, db *gorm.DB) {
	if t.publisher == nil {
		return
	}
	k := t.cfg.Kafka
	runner, err := eventbus.NewConsumers(k, rl.Handlers(), db,
		eventbus.WithSink(t.sink), eventbus.WithLogger(t.logger),
		eventbus.WithTracing(t.obs.TracingEnabled()), eventbus.WithMetrics(t.metrics()))
	if err != nil {
		t.logger.Error("kafka consumers not started", slog.Any("error", err))
		return
	}
	if m := t.metrics(); m != nil {
		defer t.startLagGauges(ctx, runner, m)()
	}
	runner.Run(ctx)
	t.logger.Info("kafka consumers stopped")
}

func (t *eventTransport) metrics() *telemetry.Metrics {
	if t.obs == nil {
		return nil
	}
	return t.obs.Metrics
}

// startLagGauges runs the kafka_consumer_lag poller for runner's groups and
// returns the function that stops it. A reader that cannot be built is logged:
// the consumers run without the gauge.
func (t *eventTransport) startLagGauges(ctx context.Context, runner *eventbus.Consumers, m *telemetry.Metrics) (stop func()) {
	collector, closeReader, err := runner.LagGauges(m)
	if err != nil {
		t.logger.Error("kafka_consumer_lag not exported", slog.Any("error", err))
		return func() {}
	}
	stopPoll := telemetry.StartPoller(ctx, t.lagInterval, collector)
	return func() {
		stopPoll()
		closeReader()
	}
}

// HealthCheck is the non-fatal Kafka readiness check (RFC §6.2: the outbox
// buffers events, so an unreachable broker degrades readiness, it does not fail
// it). It is nil in in-process mode. The worker has no HTTP surface yet, so
// nothing calls it today; a service that exposes /health/ready registers it as
// a non-fatal checker.
func (t *eventTransport) HealthCheck() func(context.Context) error {
	if t.publisher == nil {
		return nil
	}
	return kafka.HealthCheck(t.cfg.Kafka.Brokers)
}

// Close releases the producer. Call it after the relay has stopped.
func (t *eventTransport) Close() {
	if t.publisher != nil {
		t.publisher.Close()
	}
}
