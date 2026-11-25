package main

import (
	"context"
	"fmt"
	"os"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	libtemporal "github.com/AmeerHussainEmumba/SmartCourse/libs/temporal"
)

// wire registers course-worker's components (see main.go). Every component is
// non-critical except the activities worker (see runActivities): a failure is
// logged, shown in /health/ready as degraded and leaves the others running.
// Readiness: Postgres and Redis are fatal (the runtime adds them), Kafka is
// non-fatal because the outbox buffers an outage (RFC §6.2).
//
// Start-up fails (and nothing runs) when the topics cannot be provisioned or a
// schema subject is missing from the registry: a misconfigured worker stops at
// start instead of failing row by row.
func wire(p *runtime.Process) error {
	cfg, logger, deps, obs := p.Config, p.Logger, p.Deps, p.Obs
	k := cfg.Kafka

	// Temporal clients (the jobs' and the activities') get the tracing
	// interceptor only when built after this call. Tracing only: the
	// workflow_* counters belong to the process that runs the workflow worker.
	libtemporal.Observe(libtemporal.Observability{Tracing: obs.TracingEnabled()})

	// Dead letters: parked in MongoDB only when it is configured. Without a
	// store a sink would keep every poison event unpublished forever; with none
	// the transport logs the event in full and drops it (the documented gap).
	var sink relay.DeadLetterSink
	if cfg.Mongo.Configured() {
		mc := &mongoConn{cfg: cfg.Mongo}
		sink = newParkSink(mc, obs.Metrics)
		p.Go("mongo", func(ctx context.Context) error { return mc.run(ctx, logger) })
	} else {
		logger.Warn("MONGO_URI is not set: dead-letter parking is disabled; an event whose retries are spent is logged and dropped")
	}

	// Kafka: topics, schema ids, producer.
	schemas, err := eventbus.Prepare(p.Context(), k.Brokers, k.SchemaRegistryURL)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	pub, err := eventbus.NewPublisherFor(k, schemas, obs.TracingEnabled())
	if err != nil {
		return fmt.Errorf("kafka publisher: %w", err)
	}
	p.OnShutdown("kafka-producer", func(context.Context) error { pub.Close(); return nil })
	p.Ready("kafka", kafka.HealthCheck(k.Brokers), runtime.NonFatal())

	// (a) the relay: publish-only over course.outbox.
	rl := buildRelay(deps.DB, cfg, pub, sink, obs.TracingEnabled())
	p.Go("relay", func(ctx context.Context) error {
		logger.Info("outbox relay starting (publish-only)")
		rl.Run(ctx) // blocks until ctx is cancelled, draining in-flight work first
		return nil
	})

	// (b) the consumers: one set for every handler this service owns.
	consumers, err := buildConsumers(k, deps.DB, sink, logger, obs)
	if err != nil {
		return fmt.Errorf("kafka consumers: %w", err)
	}
	p.Go("consumers", func(ctx context.Context) error { return runConsumers(ctx, logger, consumers, obs.Metrics) })

	// (c) the jobs. The stale-publish reconciliation asks Temporal about
	// workflows, so the jobs get their own lazy client: a Temporal outage
	// fails that job's tick, never the process.
	retention, err := processedEventsRetention(os.Getenv)
	if err != nil {
		return err
	}
	tc, err := libtemporal.Connect(cfg)
	if err != nil {
		return fmt.Errorf("build temporal client for the reconciliation job: %w", err)
	}
	p.OnShutdown("temporal-jobs-client", func(context.Context) error { tc.Close(); return nil })
	js, err := buildJobs(deps.DB, cfg, newCourseService(deps, cfg, tc), retention)
	if err != nil {
		return fmt.Errorf("jobs: %w", err)
	}
	servers, scheduler, err := buildAsynq(cfg, logger, js)
	if err != nil {
		return fmt.Errorf("asynq: %w", err)
	}
	p.Go("jobs", func(ctx context.Context) error {
		return runJobs(ctx, logger, cfg, servers, scheduler, len(js), obs.Metrics)
	})

	// (d) the Temporal activities worker.
	p.Go("activities", func(ctx context.Context) error {
		return runActivities(ctx, logger, deps, cfg)
	}, runtime.Critical())
	return nil
}
