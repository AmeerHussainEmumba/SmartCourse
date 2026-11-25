package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/jobs"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

// asynqNamespace prefixes every Asynq queue this worker uses (identity:critical,
// identity:default, identity:low). Workers of different services share one Redis database
// and the same logical queue names; without the prefix they consume and archive
// each other's tasks ("handler not found"). Servers, scheduler, client and
// queue gauges must all use it.
const asynqNamespace = "identity"

const (
	// reseedInterval is how often the re-seeder checks its sentinel key (one
	// Redis SET NX per tick while the sentinel exists).
	reseedInterval = 15 * time.Second
	// queueGaugeInterval is how often the Asynq queue gauges read Redis.
	queueGaugeInterval = 15 * time.Second
	// publisherRetryMax caps the wait between attempts to reach Kafka at start.
	publisherRetryMax = 30 * time.Second
)

// publisherFactory builds the relay's publisher (topics, schemas, producer) and
// the function that releases it. Tests replace it; production uses
// kafkaPublisher.
type publisherFactory func(ctx context.Context) (relay.Publisher, func(), error)

// wire registers the worker's components and readiness checks.
func wire(p *runtime.Process) error {
	return wireWith(p, kafkaPublisher(p))
}

func wireWith(p *runtime.Process, newPublisher publisherFactory) error {
	cfg := p.Config
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS is required: identity-worker publishes identity.outbox to Kafka")
	}
	// The outbox buffers a Kafka outage, so Kafka degrades readiness and never
	// fails it (RFC §6.2).
	p.Ready("kafka", kafka.HealthCheck(cfg.Kafka.Brokers), runtime.NonFatal())

	// Independent components: one failing is logged and shown degraded in
	// /health/ready, the others keep running (libs/runtime).
	p.Go("relay", func(ctx context.Context) error {
		return runRelay(ctx, p.Logger, infra.Of(p.Deps).DB, cfg, p.Obs.TracingEnabled(), newPublisher)
	})
	p.Go("token-version-reseeder", func(ctx context.Context) error {
		newService(infra.Of(p.Deps), cfg).RunTokenVersionReseeder(ctx, reseedInterval)
		return nil
	})
	p.Go("jobs", func(ctx context.Context) error { return runJobs(ctx, p.Logger, infra.Of(p.Deps), cfg, p.Obs) })
	return nil
}

// newService is the identity service the re-seeder runs on. It only needs the
// database and Redis (ReseedTokenVersions touches nothing else), but is built
// the same way identity-api builds it.
func newService(deps *infra.Dependencies, cfg *config.Config) *identity.Service {
	return identity.NewService(identity.NewRepository(deps.DB), deps.RDB, cfg.JWT, cfg.Argon, identity.LoggingEmailSender{})
}

// kafkaPublisher is the production publisherFactory: it provisions the topics,
// reads the schema ids from the registry and builds the producer.
func kafkaPublisher(p *runtime.Process) publisherFactory {
	return func(ctx context.Context) (relay.Publisher, func(), error) {
		k := p.Config.Kafka
		schemas, err := eventbus.Prepare(ctx, k.Brokers, k.SchemaRegistryURL)
		if err != nil {
			return nil, nil, err
		}
		pub, err := eventbus.NewPublisherFor(k, schemas, p.Obs.TracingEnabled())
		if err != nil {
			return nil, nil, err
		}
		return pub, pub.Close, nil
	}
}

// buildRelay builds the identity relay: PUBLISH ONLY. It has a publisher and no
// handler (identity consumes no events), so it never runs anything but "encode,
// produce, mark published after the broker ack". It runs over the unqualified
// outbox table, which the svc_identity connection's search_path resolves to
// identity.outbox.
func buildRelay(db *gorm.DB, cfg *config.Config, pub relay.Publisher, tracing bool, extra ...relay.Option) *relay.Relay {
	opts := []relay.Option{relay.WithWorkerCount(cfg.Relay.Workers)}
	if tracing {
		opts = append(opts, relay.WithObserver(eventbus.RelayObserver()))
	}
	return relay.New(db, pub, append(opts, extra...)...)
}

// runRelay waits for Kafka (retrying with capped backoff: the worker's other
// components must not wait on it, and an outbox row just stays unpublished
// meanwhile), then runs the relay until ctx is cancelled.
func runRelay(ctx context.Context, logger *slog.Logger, db *gorm.DB, cfg *config.Config, tracing bool, newPublisher publisherFactory) error {
	backoff := time.Second
	for {
		pub, closePub, err := newPublisher(ctx)
		if err == nil {
			defer closePub()
			logger.Info("outbox relay starting", slog.String("transport", "kafka"))
			buildRelay(db, cfg, pub, tracing).Run(ctx) // blocks until ctx is cancelled, draining in-flight work first
			return nil
		}
		logger.Error("kafka publisher not ready; the outbox buffers events meanwhile", slog.Any("error", err), slog.Duration("retry_in", backoff))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > publisherRetryMax {
			backoff = publisherRetryMax
		}
	}
}

// buildJobs assembles identity's scheduled jobs from cfg: PruneOutbox only
// (identity has no consumers, so no processed_events, and no idempotency table).
func buildJobs(db *gorm.DB, cfg *config.Config) ([]jobs.Job, error) {
	spec, err := config.CronSpec(cfg.Jobs.OutboxPruneInterval)
	if err != nil {
		return nil, fmt.Errorf("outbox job interval: %w", err)
	}
	pruneOutbox, err := jobs.PruneOutbox(db, cfg.Jobs.OutboxRetention, spec)
	if err != nil {
		return nil, err
	}
	return []jobs.Job{pruneOutbox}, nil
}

// buildAsynq builds the Asynq side: one server per queue with RFC §10.2's pool
// sizes from cfg, and the scheduler that enqueues js on their ticks. Nothing is
// started here.
func buildAsynq(cfg *config.Config, logger *slog.Logger, js []jobs.Job) (*asynqx.Servers, *asynqx.Scheduler, error) {
	redis := asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	a := cfg.Asynq
	servers, err := asynqx.NewServers(asynqx.ServersConfig{
		Redis:     redis,
		Namespace: asynqNamespace,
		Pools: map[string]int{
			asynqx.QueueCritical: a.CriticalConcurrency,
			asynqx.QueueDefault:  a.DefaultConcurrency,
			asynqx.QueueLow:      a.LowConcurrency,
		},
		ShutdownTimeout: a.ShutdownTimeout,
		Backoff:         asynqx.BackoffConfig{Base: a.BackoffBase, Max: a.BackoffMax, Jitter: asynqx.DefaultBackoff.Jitter},
		OnExhausted: func(_ context.Context, f asynqx.FailedTask) {
			logger.Error("asynq task exhausted its retries and is archived",
				slog.String("id", f.ID), slog.String("type", f.Type), slog.String("queue", f.Queue), slog.Any("error", f.Err))
		},
		Logger: logger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("build asynq servers: %w", err)
	}
	scheduler, err := asynqx.NewScheduler(asynqx.SchedulerConfig{Redis: redis, Namespace: asynqNamespace, Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("build scheduler: %w", err)
	}
	if err := jobs.Register(scheduler, servers, a.MaxRetry, js...); err != nil {
		scheduler.Shutdown()
		return nil, nil, err
	}
	return servers, scheduler, nil
}

// runJobs runs the Asynq servers and the scheduler until ctx is cancelled, then
// stops the scheduler first (no new ticks) and drains the pools. Start does not
// fail while Redis is down: Asynq keeps trying and picks up when it returns.
func runJobs(ctx context.Context, logger *slog.Logger, deps *infra.Dependencies, cfg *config.Config, obs *infra.Observability) error {
	js, err := buildJobs(deps.DB, cfg)
	if err != nil {
		return fmt.Errorf("jobs not started: %w", err)
	}
	servers, scheduler, err := buildAsynq(cfg, logger, js)
	if err != nil {
		return fmt.Errorf("asynq not started: %w", err)
	}
	if obs.Metrics != nil {
		inspector := asynqx.NewInspector(asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB), asynqx.WithNamespace(asynqNamespace))
		stopPoll := telemetry.StartPoller(ctx, queueGaugeInterval, inspector.Gauges(obs.Metrics))
		defer func() {
			stopPoll()
			_ = inspector.Close()
		}()
	}
	if err := servers.Start(); err != nil {
		scheduler.Shutdown()
		return fmt.Errorf("asynq servers failed to start: %w", err)
	}
	if err := scheduler.Start(); err != nil {
		servers.Shutdown()
		scheduler.Shutdown()
		return fmt.Errorf("scheduler failed to start: %w", err)
	}
	logger.Info("asynq started", slog.Any("queues", servers.Queues()), slog.Int("scheduled_jobs", len(js)))
	<-ctx.Done()
	scheduler.Shutdown()
	servers.Shutdown()
	logger.Info("asynq stopped")
	return nil
}
