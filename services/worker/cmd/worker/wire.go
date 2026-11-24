package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/assets"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/workflows"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/jobs"
)

// buildRelay wires every module's relay handlers into one Relay instance.
// services/analytics/analytics, services/course/course, and services/course/enrollment register
// handlers today (enrollment withdraws active enrollments when a course is
// archived — see enrollment.handleCourseArchived; course keeps
// instructor_mirror current from identity's events — see
// course.handleUserRegistered, ADR-0024); services/worker/search's search-indexer
// and services/worker/notification's dispatcher are the natural next additions —
// the relay itself doesn't change when they land, only this list grows.
// cfg.Relay.Workers (RELAY_WORKERS) sizes the relay's pool; Load has already
// checked the Postgres pool can hold it (ADR-0027).
func buildRelay(deps *infra.Dependencies, cfg *config.Config) *relay.Relay {
	rl := relay.New(deps.DB, relay.WithWorkerCount(cfg.Relay.Workers))
	for _, h := range analytics.Handlers() {
		rl.Register(h)
	}
	for _, h := range course.Handlers() {
		rl.Register(h)
	}
	for _, h := range enrollment.Handlers() {
		rl.Register(h)
	}
	return rl
}

// buildPublishWorker wires services/course/workflows' PublishCourseWorkflow and its
// Activities onto a Temporal worker.Worker, polling cfg.Temporal.TaskQueue.
// Independent of buildRelay (one Temporal, one the in-process outbox relay);
// main.go runs them as two separate goroutines so a Temporal outage never
// takes the relay down with it (ADR-0009/0025).
func buildPublishWorker(deps *infra.Dependencies, cfg *config.Config, temporal temporalclient.Client) temporalworker.Worker {
	courseSvc := newCourseService(deps, cfg, temporal)
	activities := workflows.NewActivities(courseSvc)

	w := temporalworker.New(temporal, cfg.Temporal.TaskQueue, temporalworker.Options{})
	// Registered under course.PublishWorkflowName explicitly, rather than
	// relying on the default (the Go function's own name) — that's the
	// same string course.Service.Publish uses to start it by name, which
	// is how it starts a workflow without importing this package (course
	// must not import services/course/workflows; services/course/workflows imports
	// course for Activities, so the dependency can only run one way).
	w.RegisterWorkflowWithOptions(workflows.PublishCourseWorkflow, workflow.RegisterOptions{Name: course.PublishWorkflowName})
	w.RegisterActivity(activities)
	return w
}

// newCourseService builds the course service the worker's Temporal activities
// and scheduled jobs use, with object storage attached the way course-api does
// it. Without a store, VerifyAssets fails every publish whose lessons reference
// an asset (fail closed), so a MinIO misconfiguration is logged loudly here but
// never takes the relay down.
func newCourseService(deps *infra.Dependencies, cfg *config.Config, temporal temporalclient.Client) *course.Service {
	svc := course.NewService(course.NewRepository(deps.DB), deps.RDB, temporal, cfg.Temporal.TaskQueue)
	if store, err := assets.New(cfg.MinIO); err != nil {
		slog.Error("asset storage unavailable; publishes whose lessons reference assets will fail VerifyAssets", slog.Any("error", err))
	} else {
		svc.WithAssetStore(store)
	}
	return svc
}

// analyticsReconciler returns the analytics reconciliation (RFC §9.1) the
// scheduled job runs, or nil while none is wired (the job is then not
// scheduled). This is the one place to plug it in:
//
//	return analytics.NewReconciler(deps.DB, deps.RDB)
func analyticsReconciler(deps *infra.Dependencies) jobs.Runner {
	return nil
}

// buildJobs assembles the scheduled jobs from cfg. A job whose dependency is
// nil (no publish reconciler, no analytics Runner) is left out with a warning
// rather than scheduled to fail every tick.
func buildJobs(deps *infra.Dependencies, cfg *config.Config, publishes jobs.PublishReconciler, analyticsRunner jobs.Runner) ([]jobs.Job, error) {
	j := cfg.Jobs
	specs := map[string]string{}
	for name, interval := range map[string]time.Duration{
		"publishes": j.ReconcilePublishesInterval, "analytics": j.AnalyticsReconcileInterval,
		"enrollments": j.EnrollmentCountInterval, "idempotency": j.IdempotencyPruneInterval, "outbox": j.OutboxPruneInterval,
	} {
		s, err := config.CronSpec(interval)
		if err != nil {
			return nil, fmt.Errorf("%s job interval: %w", name, err)
		}
		specs[name] = s
	}

	var out []jobs.Job
	if publishes != nil {
		out = append(out, jobs.ReconcileStalePublishes(publishes, specs["publishes"]))
	} else {
		slog.Warn("stale-publish reconciliation not scheduled: no course service")
	}
	if analyticsRunner != nil {
		out = append(out, jobs.ReconcileAnalytics(analyticsRunner, specs["analytics"]))
	} else {
		slog.Warn("analytics reconciliation not scheduled: no Runner wired (see analyticsReconciler in wire.go)")
	}
	pruneOutbox, err := jobs.PruneOutbox(deps.DB, j.OutboxRetention, specs["outbox"])
	if err != nil {
		return nil, err
	}
	out = append(out,
		jobs.ReconcileEnrollmentCounts(deps.DB, specs["enrollments"]),
		jobs.PruneIdempotencyKeys(deps.DB, specs["idempotency"]),
		pruneOutbox,
	)
	return out, nil
}

// buildAsynq builds the Asynq side of the worker: one server per queue with
// RFC §10.2's pool sizes from cfg, and the scheduler that enqueues js on their
// ticks. Handlers owned by other packages (the notification dispatcher's
// delivery task, for example) are registered on the returned servers with
// Register(taskType, queue, fn) before they start. Nothing is started here.
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
			// Parking as a failed event arrives with the Mongo failed_events store.
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
