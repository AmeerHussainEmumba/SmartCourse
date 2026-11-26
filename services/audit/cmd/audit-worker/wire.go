package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	rt "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/archive"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/deadletter"
	"github.com/AmeerHussainEmumba/SmartCourse/services/audit/store"
)

// topicsTimeout bounds creating the topics at start-up; a failure is not fatal
// (the broker may be down: the consumer retries until it is up).
const topicsTimeout = 30 * time.Second

// wire registers the archiver. Components and readiness:
//
//   - "mongo": connects in the background and keeps retrying; never stops the
//     process (the archiver errors, retries and parks while it is down).
//   - "archiver" (critical): the consumer group audit-archiver. It returns only
//     at shutdown, or at once on a configuration error, which then exits 1.
//   - readiness: Kafka is FATAL (without the broker the service does nothing),
//     MongoDB is NON-FATAL ("degraded": the events stay in Kafka and the
//     archiver catches up when it is back).
//
// Postgres is not used (no runtime.WithDependencies): the archive is
// idempotent by event id in MongoDB, and the consumer is eventbus.NewStreamConsumers.
func wire(p *rt.Process) error {
	cfg := p.Config
	mongo := store.New(cfg.Mongo)
	p.Go("mongo", func(ctx context.Context) error { return mongo.Run(ctx, p.Logger) })
	p.Ready("mongodb", mongo.Ping, rt.NonFatal())
	p.Ready("kafka", kafka.HealthCheck(cfg.Kafka.Brokers))

	sink := deadletter.ForRelay(deadletter.NewSink(countParked(mongo, p.Obs.Metrics)))
	handlers := archive.Handlers(mongo)
	// The stream mode: no Postgres transaction and no processed_events marker. The
	// archiver is idempotent by event id in MongoDB (first write wins), which is
	// what makes the at-least-once delivery and a replay safe.
	runner, err := eventbus.NewStreamConsumers(cfg.Kafka, handlers,
		eventbus.WithSink(sink), eventbus.WithLogger(p.Logger), eventbus.WithTracing(p.Obs.TracingEnabled()), eventbus.WithMetrics(p.Obs.Metrics))
	if err != nil {
		return fmt.Errorf("archiver: %w", err)
	}
	p.Go("archiver", func(ctx context.Context) error {
		tctx, cancel := context.WithTimeout(ctx, topicsTimeout)
		if err := topics.Ensure(tctx, cfg.Kafka.Brokers); err != nil {
			p.Logger.Warn("could not ensure the event topics; the consumer retries on its own", slog.Any("error", err))
		}
		cancel()
		runner.Run(ctx)
		if ctx.Err() == nil {
			return errors.New("the archiver consumer stopped")
		}
		return nil
	}, rt.Critical())
	return nil
}

// countParkStore counts every event the sink parks in dead_letter_total{consumer}.
type countParkStore struct {
	inner deadletter.ParkStore
	m     *telemetry.Metrics
}

// countParked returns s unchanged when metrics are disabled (m is nil).
func countParked(s deadletter.ParkStore, m *telemetry.Metrics) deadletter.ParkStore {
	if m == nil {
		return s
	}
	return countParkStore{inner: s, m: m}
}

func (c countParkStore) ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	fe, err := c.inner.ParkFailedEvent(ctx, in)
	if err == nil {
		c.m.DeadLettered(in.Consumer)
	}
	return fe, err
}

// loadConfig is audit-worker's configuration: MongoDB and Kafka only (no Redis, no JWT key). It needs no POSTGRES_* or MINIO_*
// (config.Requirements asks only for what is listed).
func loadConfig() (*config.Config, error) {
	return config.LoadWith(config.Requirements{KafkaBrokers: true, Mongo: true})
}
