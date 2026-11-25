package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/jobs"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

// Task types of the two course-specific jobs, also their scheduler entry names:
// the same string on every replica (the housekeeping names are in libs/jobs).
const (
	TypeReconcileStalePublishes = "maintenance:reconcile-stale-publishes"
	TypeEnrollmentCounts        = "maintenance:enrollment-counts"
)

// queueGaugeInterval is how often the Asynq queue gauges read Redis.
const queueGaugeInterval = 15 * time.Second

// publishReconciler is course.Service's stale-publish reconciliation.
type publishReconciler interface {
	ReconcileStalePublishes(ctx context.Context) (course.ReconcileResult, error)
}

// reconcileStalePublishes releases `publishing` versions whose workflow is gone
// (ADR-0028). It talks to Temporal, so a Temporal outage fails the tick and
// Asynq retries it; the next tick tries again regardless.
func reconcileStalePublishes(svc publishReconciler, spec string) jobs.Job {
	return jobs.Job{Name: TypeReconcileStalePublishes, Spec: spec, Timeout: jobs.EveryMinuteTimeout, Run: func(ctx context.Context) error {
		res, err := svc.ReconcileStalePublishes(ctx)
		if res.Checked > 0 || err != nil {
			slog.Info("stale publishes reconciled",
				slog.Int("checked", res.Checked), slog.Int("released", res.Released), slog.Int("running", res.Running),
				slog.Int("lost", res.Lost), slog.Int("errors", res.Errors))
		}
		return err
	}}
}

// reconcileEnrollmentCounts repairs courses.enrollment_count drift.
func reconcileEnrollmentCounts(db *gorm.DB, spec string) jobs.Job {
	return jobs.Job{Name: TypeEnrollmentCounts, Spec: spec, Timeout: jobs.HourlyTimeout, Run: func(ctx context.Context) error {
		res, err := enrollment.ReconcileEnrollmentCounts(ctx, db)
		for _, d := range res.Repaired {
			slog.Warn("enrollment_count drift repaired",
				slog.String("course_id", d.CourseID.String()), slog.Int("stored", d.Stored), slog.Int("actual", d.Actual))
		}
		return err
	}}
}

// buildJobs assembles the course service's scheduled jobs from cfg:
//
//   - stale-publish reconciliation (JOBS_RECONCILE_PUBLISHES_INTERVAL, 1 m);
//   - enrollment-count reconciliation (JOBS_ENROLLMENT_COUNT_INTERVAL);
//   - idempotency-key pruning (JOBS_IDEMPOTENCY_PRUNE_INTERVAL);
//   - outbox pruning (JOBS_OUTBOX_PRUNE_INTERVAL, OUTBOX_RETENTION);
//   - processed_events pruning, on the outbox prune interval, with
//     processedRetention.
//
// db must be the svc_course connection: every prune acts on the course schema.
func buildJobs(db *gorm.DB, cfg *config.Config, publishes publishReconciler) ([]jobs.Job, error) {
	j := cfg.Jobs
	specs := map[string]string{}
	for name, interval := range map[string]time.Duration{
		"publishes": j.ReconcilePublishesInterval, "enrollments": j.EnrollmentCountInterval,
		"idempotency": j.IdempotencyPruneInterval, "outbox": j.OutboxPruneInterval,
	} {
		s, err := config.CronSpec(interval)
		if err != nil {
			return nil, fmt.Errorf("%s job interval: %w", name, err)
		}
		specs[name] = s
	}
	pruneOutbox, err := jobs.PruneOutbox(db, j.OutboxRetention, specs["outbox"])
	if err != nil {
		return nil, err
	}
	pruneProcessed, err := jobs.PruneProcessedEvents(db, j.ProcessedEventsRetention, specs["outbox"])
	if err != nil {
		return nil, err
	}
	return []jobs.Job{
		reconcileStalePublishes(publishes, specs["publishes"]),
		reconcileEnrollmentCounts(db, specs["enrollments"]),
		jobs.PruneIdempotencyKeys(db, specs["idempotency"]),
		pruneOutbox,
		pruneProcessed,
	}, nil
}

// buildAsynq builds the Asynq side: one pool on the low queue (the maintenance
// queue, RFC §6.7, where libs/jobs installs every job) sized by
// ASYNQ_LOW_CONCURRENCY, and the scheduler that enqueues js on their ticks.
// Nothing is started here.
func buildAsynq(cfg *config.Config, logger *slog.Logger, js []jobs.Job) (*asynqx.Servers, *asynqx.Scheduler, error) {
	redis := asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	a := cfg.Asynq
	servers, err := asynqx.NewServers(asynqx.ServersConfig{
		Redis:           redis,
		Pools:           map[string]int{asynqx.QueueLow: a.LowConcurrency},
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

// runJobs starts the pool and the scheduler and runs until ctx is cancelled,
// then stops the scheduler first (no new ticks) and drains the pool. Start does
// not fail while Redis is down: Asynq logs and keeps trying, and the pool and
// the scheduler pick up when it returns. With metrics on it also exports the
// Asynq queue gauges.
func runJobs(ctx context.Context, logger *slog.Logger, cfg *config.Config, servers *asynqx.Servers, scheduler *asynqx.Scheduler, scheduled int, m *telemetry.Metrics) error {
	if m != nil {
		inspector := asynqx.NewInspector(asynqx.RedisClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB))
		stop := telemetry.StartPoller(ctx, queueGaugeInterval, inspector.Gauges(m))
		defer func() {
			stop()
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
	logger.Info("asynq started", slog.Any("queues", servers.Queues()), slog.Int("scheduled_jobs", scheduled))
	<-ctx.Done()
	scheduler.Shutdown()
	servers.Shutdown()
	logger.Info("asynq stopped")
	return nil
}
