package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/jobs"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

const (
	// TypeReconcile is the reconciler's task type and scheduler entry name: the
	// same string on every replica (it was jobs.TypeAnalyticsReconcile).
	TypeReconcile = "maintenance:analytics-reconcile"

	// ProcessedRetentionEnv is read here with os.Getenv: libs/config has no
	// field for it yet.
	ProcessedRetentionEnv = "PROCESSED_EVENTS_RETENTION"
	// DefaultProcessedRetention is the duplicate-absorption window. It must
	// exceed the Kafka topic retention and the longest park-to-replay delay
	// (eventbus.PruneProcessed).
	DefaultProcessedRetention = 14 * 24 * time.Hour

	// lagGaugeInterval is how often kafka_consumer_lag asks the brokers.
	lagGaugeInterval = 15 * time.Second
)

// prepareFunc provisions the topics and checks the registry before the
// consumers subscribe (eventbus.Prepare). Tests replace it, there is no Schema
// Registry container.
type prepareFunc func(ctx context.Context, k config.KafkaConfig) error

func prepareKafka(ctx context.Context, k config.KafkaConfig) error {
	_, err := eventbus.Prepare(ctx, k.Brokers, k.SchemaRegistryURL)
	return err
}

// wire is the production wiring.
func wire(p *runtime.Process) error { return wireWith(p, prepareKafka) }

// wireWith registers the worker's components. There is no outbox, no relay and
// no producer: analytics emits nothing.
//
//   - mongo (only with MONGO_URI): the background MongoDB connection for
//     dead-letter parking. Non-fatal readiness "mongodb".
//   - consumers: eventbus.NewConsumers over analytics.Handlers(). Non-fatal
//     readiness "kafka".
//   - asynq: the scheduler and the low-queue servers that run the jobs.
//
// Postgres and Redis are the fatal readiness checks libs/runtime adds.
func wireWith(p *runtime.Process, prepare prepareFunc) error {
	cfg := p.Config
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS is required: Kafka is the only event transport")
	}

	var sink relay.DeadLetterSink
	if cfg.Mongo.Configured() {
		dl := newDeadLetters(cfg.Mongo, p.Obs.Metrics)
		sink = dl
		p.Go("mongo", func(ctx context.Context) error { return dl.run(ctx, p.Logger) })
		p.Ready("mongodb", dl.Ping, runtime.NonFatal())
	} else {
		p.Logger.Warn("MONGO_URI is not set: events that exhaust their retries are logged and dropped, not parked")
	}

	copts := []eventbus.ConsumerOption{
		eventbus.WithLogger(p.Logger),
		eventbus.WithTracing(p.Obs.TracingEnabled()),
		eventbus.WithMetrics(p.Obs.Metrics),
	}
	if sink != nil {
		copts = append(copts, eventbus.WithSink(sink))
	}
	consumers, err := eventbus.NewConsumers(cfg.Kafka, analytics.Handlers(), p.Deps.DB, copts...)
	if err != nil {
		return fmt.Errorf("build consumers: %w", err)
	}
	p.Go("consumers", func(ctx context.Context) error {
		return runConsumers(ctx, p.Logger, cfg.Kafka, consumers, p.Obs.Metrics, prepare)
	})
	p.Ready("kafka", kafka.HealthCheck(cfg.Kafka.Brokers), runtime.NonFatal())

	js, err := buildJobs(p.Deps.DB, p.Deps.RDB, cfg)
	if err != nil {
		return err
	}
	servers, scheduler, err := buildAsynq(cfg, p.Logger, js)
	if err != nil {
		return err
	}
	p.Go("asynq", func(ctx context.Context) error {
		// Start does not fail while Redis is down: Asynq keeps trying and the
		// pools and the scheduler pick up when it returns.
		if err := servers.Start(); err != nil {
			scheduler.Shutdown()
			return fmt.Errorf("asynq servers: %w", err)
		}
		if err := scheduler.Start(); err != nil {
			servers.Shutdown()
			scheduler.Shutdown()
			return fmt.Errorf("scheduler: %w", err)
		}
		p.Logger.Info("asynq started", slog.Any("queues", servers.Queues()), slog.Int("scheduled_jobs", len(js)))
		<-ctx.Done()
		scheduler.Shutdown() // no new ticks, then drain the pools
		servers.Shutdown()
		return nil
	})
	return nil
}

// runConsumers provisions the topics (retrying while Kafka is down, so a broker
// outage at start degrades readiness instead of crash-looping the pod), then
// runs the consumer groups until ctx is cancelled.
func runConsumers(ctx context.Context, logger *slog.Logger, k config.KafkaConfig, c *eventbus.Consumers, m *telemetry.Metrics, prepare prepareFunc) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		err := prepare(ctx, k)
		if err == nil {
			break
		}
		logger.Error("kafka not ready; consumers wait", slog.Any("error", err), slog.Duration("retry_in", backoff))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	if m != nil {
		if collector, closeReader, err := c.LagGauges(m); err != nil {
			logger.Error("kafka_consumer_lag not exported", slog.Any("error", err))
		} else {
			stop := telemetry.StartPoller(ctx, lagGaugeInterval, collector)
			defer func() { stop(); closeReader() }()
		}
	}
	logger.Info("kafka consumers starting", slog.Any("groups", c.Groups()))
	c.Run(ctx)
	logger.Info("kafka consumers stopped")
	return nil
}

// processedRetention reads PROCESSED_EVENTS_RETENTION (a Go duration such as
// 336h; default 14 days).
func processedRetention(getenv func(string) string) (time.Duration, error) {
	v := strings.TrimSpace(getenv(ProcessedRetentionEnv))
	if v == "" {
		return DefaultProcessedRetention, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as 336h, got %q", ProcessedRetentionEnv, v)
	}
	return d, nil
}

// buildJobs assembles the scheduled jobs: the analytics reconciler (RFC §9.1,
// every JOBS_ANALYTICS_RECONCILE_INTERVAL, default 1 minute) and the pruning of
// analytics.processed_events.
func buildJobs(db *gorm.DB, rdb *redis.Client, cfg *config.Config) ([]jobs.Job, error) {
	reconcileSpec, err := config.CronSpec(cfg.Jobs.AnalyticsReconcileInterval)
	if err != nil {
		return nil, fmt.Errorf("analytics reconcile interval: %w", err)
	}
	pruneSpec, err := config.CronSpec(time.Hour)
	if err != nil {
		return nil, fmt.Errorf("prune interval: %w", err)
	}
	retention, err := processedRetention(os.Getenv)
	if err != nil {
		return nil, err
	}
	prune, err := jobs.PruneProcessedEvents(db, retention, pruneSpec)
	if err != nil {
		return nil, err
	}
	return []jobs.Job{
		jobs.RunnerJob(TypeReconcile, reconcileSpec, jobs.EveryMinuteTimeout, analytics.NewReconciler(db, rdb)),
		prune,
	}, nil
}

// buildAsynq builds the Asynq side: one server per queue with RFC §10.2's pool
// sizes from cfg, and the scheduler that enqueues js on their ticks. Nothing is
// started here.
func buildAsynq(cfg *config.Config, logger *slog.Logger, js []jobs.Job) (*asynqx.Servers, *asynqx.Scheduler, error) {
	rc := asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	a := cfg.Asynq
	servers, err := asynqx.NewServers(asynqx.ServersConfig{
		Redis: rc,
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
	scheduler, err := asynqx.NewScheduler(asynqx.SchedulerConfig{Redis: rc, Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("build scheduler: %w", err)
	}
	if err := jobs.Register(scheduler, servers, a.MaxRetry, js...); err != nil {
		scheduler.Shutdown()
		return nil, nil, err
	}
	return servers, scheduler, nil
}
