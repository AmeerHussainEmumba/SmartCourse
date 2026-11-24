package main

import (
	"context"
	"fmt"
	"log/slog"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport"
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
	publisher *transport.Publisher
}

// newEventTransport prepares the transport. In kafka mode it provisions the
// topics, checks the schema registry (fails when it is unreachable or a subject
// is missing) and builds the producer, so a misconfigured worker stops at start
// instead of failing row by row. sink is the optional dead-letter sink (nil
// keeps log-and-drop, the documented gap); the MongoDB wiring passes its
// failed_events sink here.
func newEventTransport(ctx context.Context, logger *slog.Logger, cfg *config.Config, sink relay.DeadLetterSink) (*eventTransport, error) {
	t := &eventTransport{cfg: cfg, logger: logger, sink: sink}
	if transportMode(cfg) != config.TransportKafka {
		return t, nil
	}
	k := cfg.Kafka
	schemas, err := transport.Prepare(ctx, k.Brokers, k.SchemaRegistryURL)
	if err != nil {
		return nil, fmt.Errorf("kafka transport: %w", err)
	}
	t.publisher, err = transport.NewPublisher(transport.PublisherConfig{
		Brokers: k.Brokers, ProduceTimeout: k.ProduceTimeout, Schemas: schemas,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka transport: %w", err)
	}
	logger.Info("event transport: kafka", slog.Any("brokers", k.Brokers), slog.String("schema_registry", k.SchemaRegistryURL))
	return t, nil
}

// RelayOptions are the options the relay is built with: the publisher in kafka
// mode, and the dead-letter sink when there is one.
func (t *eventTransport) RelayOptions() []relay.Option {
	var opts []relay.Option
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
	runner, err := transport.NewRunner(transport.RunnerConfig{
		Brokers:         k.Brokers,
		MaxInFlight:     k.ConsumerMaxInFlight,
		MaxAttempts:     k.ConsumerMaxAttempts,
		BackoffBase:     k.ConsumerBackoffBase,
		BackoffMax:      k.ConsumerBackoffMax,
		ShutdownTimeout: k.ConsumerShutdownTimeout,
		Sink:            t.sink,
		Logger:          t.logger,
	}, db, rl.Handlers())
	if err != nil {
		t.logger.Error("kafka consumers not started", slog.Any("error", err))
		return
	}
	runner.Run(ctx)
	t.logger.Info("kafka consumers stopped")
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
