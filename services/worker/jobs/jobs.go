// Package jobs is the worker's set of scheduled maintenance jobs (RFC §9.5,
// §6.7): reconciliation that repairs drift and pruning that bounds tables.
//
// Each job is a Job (a name, a cron spec and a function). Register hands them
// to the asynqx scheduler, which enqueues one task per tick however many worker
// replicas run it, and to the asynqx servers, which run the task on the low
// queue. Every job here is safe to run twice or from two replicas at once, so
// Asynq's at-least-once delivery costs nothing.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

// Task types, also the scheduler entry names: the same string on every replica.
const (
	TypeReconcileStalePublishes = "maintenance:reconcile-stale-publishes"
	TypeAnalyticsReconcile      = "maintenance:analytics-reconcile"
	TypeEnrollmentCounts        = "maintenance:enrollment-counts"
	TypeIdempotencyPrune        = "maintenance:idempotency-prune"
	TypeOutboxPrune             = "maintenance:outbox-prune"
)

// Per-attempt timeouts. A job that runs every minute must end inside it, or the
// next tick would start a second copy on top of it.
const (
	everyMinuteTimeout = 55 * time.Second
	hourlyTimeout      = 10 * time.Minute
)

// Runner is one run of a reconciliation owned by another package. The analytics
// reconciler (analytics.NewReconciler(db, rdb)) satisfies it.
type Runner interface {
	Run(ctx context.Context) error
}

// PublishReconciler is course.Service's stale-publish reconciliation.
type PublishReconciler interface {
	ReconcileStalePublishes(ctx context.Context) (course.ReconcileResult, error)
}

// Job is one scheduled job.
type Job struct {
	Name    string // task type and scheduler entry name
	Spec    string // 5-field cron expression (config.CronSpec renders one from an interval)
	Timeout time.Duration
	Run     func(ctx context.Context) error
}

// Scheduler is the part of *asynqx.Scheduler that Register uses.
type Scheduler interface {
	Register(e asynqx.Entry) error
}

// Registry is the part of *asynqx.Servers that Register uses.
type Registry interface {
	Register(taskType, queue string, fn func(ctx context.Context, payload []byte) error) error
}

// Register schedules every job on sc and installs its handler on the low queue
// (RFC §6.7: maintenance). maxRetry is the retry budget of each tick's task.
// Call it before starting either.
func Register(sc Scheduler, reg Registry, maxRetry int, js ...Job) error {
	for _, j := range js {
		if j.Run == nil {
			return fmt.Errorf("jobs: %s has no Run", j.Name)
		}
		err := sc.Register(asynqx.Entry{
			Name: j.Name, Spec: j.Spec, Type: j.Name,
			Queue: asynqx.QueueLow, MaxRetry: maxRetry, Timeout: j.Timeout,
		})
		if err != nil {
			return fmt.Errorf("schedule %s: %w", j.Name, err)
		}
		run, name := j.Run, j.Name
		err = reg.Register(j.Name, asynqx.QueueLow, func(ctx context.Context, _ []byte) error {
			start := time.Now()
			if err := run(ctx); err != nil {
				slog.Error("scheduled job failed", slog.String("job", name), slog.Any("error", err), slog.Duration("took", time.Since(start)))
				return err
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("register handler %s: %w", name, err)
		}
	}
	return nil
}

// ReconcileStalePublishes releases `publishing` versions whose workflow is gone
// (ADR-0028). It talks to Temporal, so a Temporal outage fails the tick and
// Asynq retries it; the next tick tries again regardless.
func ReconcileStalePublishes(svc PublishReconciler, spec string) Job {
	return Job{Name: TypeReconcileStalePublishes, Spec: spec, Timeout: everyMinuteTimeout, Run: func(ctx context.Context) error {
		res, err := svc.ReconcileStalePublishes(ctx)
		if res.Checked > 0 || err != nil {
			slog.Info("stale publishes reconciled",
				slog.Int("checked", res.Checked), slog.Int("released", res.Released), slog.Int("running", res.Running),
				slog.Int("lost", res.Lost), slog.Int("errors", res.Errors))
		}
		return err
	}}
}

// ReconcileAnalytics runs the analytics reconciler (RFC §9.1). A nil Runner is
// a programming error and surfaces as a failing job, not a panic in a worker.
func ReconcileAnalytics(r Runner, spec string) Job {
	return Job{Name: TypeAnalyticsReconcile, Spec: spec, Timeout: everyMinuteTimeout, Run: func(ctx context.Context) error {
		if r == nil {
			return errors.New("jobs: analytics reconciliation has no Runner")
		}
		return r.Run(ctx)
	}}
}

// ReconcileEnrollmentCounts repairs courses.enrollment_count drift.
func ReconcileEnrollmentCounts(db *gorm.DB, spec string) Job {
	return Job{Name: TypeEnrollmentCounts, Spec: spec, Timeout: hourlyTimeout, Run: func(ctx context.Context) error {
		res, err := enrollment.ReconcileEnrollmentCounts(ctx, db)
		for _, d := range res.Repaired {
			slog.Warn("enrollment_count drift repaired",
				slog.String("course_id", d.CourseID.String()), slog.Int("stored", d.Stored), slog.Int("actual", d.Actual))
		}
		return err
	}}
}

// PruneIdempotencyKeys deletes expired Idempotency-Key rows (RFC §9.5).
func PruneIdempotencyKeys(db *gorm.DB, spec string) Job {
	return Job{Name: TypeIdempotencyPrune, Spec: spec, Timeout: hourlyTimeout, Run: func(ctx context.Context) error {
		n, err := idempotency.PruneExpired(ctx, db)
		if err == nil && n > 0 {
			slog.Info("expired idempotency keys pruned", slog.Int64("rows", n))
		}
		return err
	}}
}

// PruneOutbox deletes published outbox rows older than retention. Unpublished
// rows are never touched (outbox.Prune).
func PruneOutbox(db *gorm.DB, retention time.Duration, spec string) (Job, error) {
	if retention <= 0 {
		return Job{}, errors.New("jobs: outbox retention must be positive")
	}
	return Job{Name: TypeOutboxPrune, Spec: spec, Timeout: hourlyTimeout, Run: func(ctx context.Context) error {
		n, err := outbox.Prune(ctx, db, retention)
		if err == nil && n > 0 {
			slog.Info("published outbox rows pruned", slog.Int64("rows", n), slog.Duration("retention", retention))
		}
		return err
	}}, nil
}
