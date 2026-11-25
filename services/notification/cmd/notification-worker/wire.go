package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/jobs"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/services/notification/notification"
)

// lagGaugeInterval is how often the consumer lag gauges query the brokers, so
// the broker load does not depend on how often Prometheus scrapes.
const lagGaugeInterval = 15 * time.Second

// wire registers the notification worker's components on p: the Kafka
// consumers (notification-dispatcher plus its retry topic), the Asynq pools and
// scheduler, the pending-delivery sweeper and, optionally, the Mongo-backed
// dead-letter sink. Postgres and Redis readiness are fatal (libs/runtime); Kafka,
// MongoDB and SMTP are non-fatal.
func wire(p *runtime.Process) error {
	cfg, logger, db := p.Config, p.Logger, p.Deps.DB
	ctx := p.Context()

	// Dead-letter sink: only with MONGO_URI. Without it an event that exhausts
	// its budget is logged in full and dropped (the documented gap).
	var sink relay.DeadLetterSink
	if cfg.Mongo.Configured() {
		ms := newMongoSink(cfg.Mongo, p.Obs.Metrics)
		sink = ms
		p.Go("mongodb", func(ctx context.Context) error { return ms.run(ctx, logger) })
		p.Ready("mongodb", ms.Ping, runtime.NonFatal())
	} else {
		logger.Warn("MONGO_URI is not set: events that exhaust their retries are logged and dropped, not parked")
	}

	// Consumers. Prepare provisions the topics (retry topics included) and
	// checks the registry, so a misconfigured worker stops here, not per event.
	k := cfg.Kafka
	if _, err := eventbus.Prepare(ctx, k.Brokers, k.SchemaRegistryURL); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	consumers, err := eventbus.NewConsumers(k, notification.Handlers(), db,
		eventbus.WithSink(sink), eventbus.WithLogger(logger),
		eventbus.WithTracing(p.Obs.TracingEnabled()), eventbus.WithMetrics(p.Obs.Metrics))
	if err != nil {
		return fmt.Errorf("kafka consumers: %w", err)
	}
	p.Go("consumers", func(ctx context.Context) error {
		if m := p.Obs.Metrics; m != nil {
			defer startLagGauges(ctx, logger, consumers, m)()
		}
		consumers.Run(ctx)
		return nil
	})
	p.Ready("kafka", kafka.HealthCheck(k.Brokers), runtime.NonFatal()) // the broker is not on the delivery path

	// Asynq: pools, housekeeping jobs and the delivery task.
	js, err := buildJobs(db, cfg)
	if err != nil {
		return err
	}
	servers, scheduler, err := buildAsynq(cfg, logger, js)
	if err != nil {
		return err
	}
	provider := newProvider(logger, cfg)
	registered, err := registerDelivery(logger, servers, db, provider)
	if err != nil {
		scheduler.Shutdown()
		return err
	}
	p.Go("asynq", servers.Run)
	p.Go("scheduler", scheduler.Run)

	if registered {
		client := asynqx.NewClient(asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB))
		p.OnShutdown("asynq-client", func(context.Context) error { return client.Close() })
		p.Go("sweeper", func(ctx context.Context) error {
			notification.RunSweeper(ctx, db, notificationEnqueuer{client}, sweepInterval, notification.SweepOptions{})
			return nil
		})
		p.Ready("smtp", smtpCheck(cfg.SMTP), runtime.NonFatal())
	}
	return nil
}

// buildJobs assembles the housekeeping jobs: today the processed_events prune.
// The notification service has no outbox, no idempotency keys and no
// reconciliation to run.
func buildJobs(db *gorm.DB, cfg *config.Config) ([]jobs.Job, error) {
	spec, err := config.CronSpec(cfg.Jobs.OutboxPruneInterval) // the shared prune cadence (JOBS_OUTBOX_PRUNE_INTERVAL, default 1h)
	if err != nil {
		return nil, fmt.Errorf("prune job interval: %w", err)
	}
	prune, err := jobs.PruneProcessedEvents(db, cfg.Jobs.ProcessedEventsRetention, spec)
	if err != nil {
		return nil, err
	}
	return []jobs.Job{prune}, nil
}

// buildAsynq builds the Asynq side: one server per queue with the pool sizes
// from cfg, and the scheduler that enqueues js on their ticks. Nothing is started.
func buildAsynq(cfg *config.Config, logger *slog.Logger, js []jobs.Job) (*asynqx.Servers, *asynqx.Scheduler, error) {
	redis := asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	a := cfg.Asynq
	servers, err := asynqx.NewServers(asynqx.ServersConfig{
		Redis: redis,
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
	scheduler, err := asynqx.NewScheduler(asynqx.SchedulerConfig{Redis: redis, Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("build scheduler: %w", err)
	}
	if err := jobs.Register(scheduler, servers, a.MaxRetry, js...); err != nil {
		scheduler.Shutdown()
		return nil, nil, err
	}
	return servers, scheduler, nil
}

// startLagGauges runs the kafka_consumer_lag poller for the consumers' groups
// and returns the function that stops it. A reader that cannot be built is
// logged: the consumers run without the gauge.
func startLagGauges(ctx context.Context, logger *slog.Logger, c *eventbus.Consumers, m *telemetry.Metrics) (stop func()) {
	collector, closeReader, err := c.LagGauges(m)
	if err != nil {
		logger.Error("kafka_consumer_lag not exported", slog.Any("error", err))
		return func() {}
	}
	stopPoll := telemetry.StartPoller(ctx, lagGaugeInterval, collector)
	return func() {
		stopPoll()
		closeReader()
	}
}

// smtpCheck is the non-fatal readiness probe: a TCP dial to the SMTP host. A
// mail outage leaves deliveries pending (they are retried), it does not make
// the worker unready.
func smtpCheck(c config.SMTPConfig) runtime.Check {
	addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	return func(ctx context.Context) error {
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return err
		}
		return conn.Close()
	}
}
