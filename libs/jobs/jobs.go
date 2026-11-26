// Package jobs is the shared plumbing for a service worker's scheduled
// maintenance jobs (RFC §9.5, §6.7): a Job is a name, a cron spec and a
// function; Register hands them to the asynqx scheduler, which enqueues one
// task per tick however many worker replicas run it, and to the asynqx
// servers, which run the task on the low queue. Every job must be safe to run
// twice or from two replicas at once, so Asynq's at-least-once delivery costs
// nothing.
//
// Housekeeping every service worker needs (pruning its own outbox, its own
// idempotency keys and its own processed_events) lives here. Reconciliations
// that belong to one service (stale publishes, enrollment counters, analytics)
// are built by that service from Job and Runner.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
)

// Task types, also the scheduler entry names: the same string on every replica.
// Services add their own beside these.
const (
	TypeIdempotencyPrune = "maintenance:idempotency-prune"
	TypeOutboxPrune      = "maintenance:outbox-prune"
	TypeProcessedPrune   = "maintenance:processed-events-prune"
)

// Per-attempt timeouts. A job that runs every minute must end inside it, or the
// next tick would start a second copy on top of it.
const (
	EveryMinuteTimeout = 55 * time.Second
	HourlyTimeout      = 10 * time.Minute
)

// Runner is one run of a reconciliation owned by a service package (for
// example analytics.NewReconciler(db, rdb)).
type Runner interface {
	Run(ctx context.Context) error
}

// Job is one scheduled job.
type Job struct {
	Name    string // task type and scheduler entry name
	Spec    string // 5-field cron expression (config.CronSpec renders one from an interval)
	Timeout time.Duration
	Run     func(ctx context.Context) error
}

// RunnerJob builds a Job from a Runner. A nil Runner is a programming error
// and surfaces as a failing job, not a panic in a worker.
func RunnerJob(name, spec string, timeout time.Duration, r Runner) Job {
	return Job{Name: name, Spec: spec, Timeout: timeout, Run: func(ctx context.Context) error {
		if r == nil {
			return fmt.Errorf("jobs: %s has no Runner", name)
		}
		return r.Run(ctx)
	}}
}

// Scheduler is the part of *asynqx.Scheduler that Register uses.
type Scheduler interface {
	Register(e asynqx.Entry) error
}

// Registry is the part of *asynqx.Servers that Register uses.
type Registry interface {
	Register(taskType, queue string, fn func(ctx context.Context, payload []byte) error) error
}

// Namespaced is implemented by *asynqx.Scheduler and *asynqx.Servers: the queue
// namespace they enqueue to and consume from.
type Namespaced interface {
	Namespace() asynqx.Namespace
}

// ErrNamespaceMismatch is returned by Register when the scheduler and the
// registry use different queue namespaces: the ticks would be enqueued where
// no server consumes them.
var ErrNamespaceMismatch = errors.New("jobs: scheduler and registry use different asynqx namespaces")

// Register schedules every job on sc and installs its handler on the low queue
// (RFC §6.7: maintenance). The queue is the logical QueueLow; the asynqx
// scheduler and servers apply their own namespace, so it is the service's
// namespaced low queue, and Register refuses a scheduler and a registry whose
// namespaces differ. maxRetry is the retry budget of each tick's task. Call it
// before starting either.
func Register(sc Scheduler, reg Registry, maxRetry int, js ...Job) error {
	if a, ok := sc.(Namespaced); ok {
		if b, ok := reg.(Namespaced); ok && a.Namespace() != b.Namespace() {
			return fmt.Errorf("%w: %q vs %q", ErrNamespaceMismatch, a.Namespace(), b.Namespace())
		}
	}
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

// PruneIdempotencyKeys deletes the service's expired Idempotency-Key rows
// (RFC §9.5). db must be the service's own connection: the table it prunes is
// the one in the service's schema.
func PruneIdempotencyKeys(db *gorm.DB, spec string) Job {
	return Job{Name: TypeIdempotencyPrune, Spec: spec, Timeout: HourlyTimeout, Run: func(ctx context.Context) error {
		n, err := idempotency.PruneExpired(ctx, db)
		if err == nil && n > 0 {
			slog.Info("expired idempotency keys pruned", slog.Int64("rows", n))
		}
		return err
	}}
}

// PruneOutbox deletes the service's published outbox rows older than
// retention. Unpublished rows are never touched (outbox.Prune).
func PruneOutbox(db *gorm.DB, retention time.Duration, spec string) (Job, error) {
	if retention <= 0 {
		return Job{}, errors.New("jobs: outbox retention must be positive")
	}
	return Job{Name: TypeOutboxPrune, Spec: spec, Timeout: HourlyTimeout, Run: func(ctx context.Context) error {
		n, err := outbox.Prune(ctx, db, retention)
		if err == nil && n > 0 {
			slog.Info("published outbox rows pruned", slog.Int64("rows", n), slog.Duration("retention", retention))
		}
		return err
	}}, nil
}

// PruneProcessedEvents deletes the service's processed_events rows older than
// retention. The retention is the duplicate-absorption window: it must exceed
// both the Kafka topic retention and the longest park-to-replay delay, or a
// redelivered event could be applied twice (eventbus.PruneProcessed).
func PruneProcessedEvents(db *gorm.DB, retention time.Duration, spec string) (Job, error) {
	if retention <= 0 {
		return Job{}, errors.New("jobs: processed_events retention must be positive")
	}
	return Job{Name: TypeProcessedPrune, Spec: spec, Timeout: HourlyTimeout, Run: func(ctx context.Context) error {
		n, err := eventbus.PruneProcessed(ctx, db, retention)
		if err == nil && n > 0 {
			slog.Info("processed_events pruned", slog.Int64("rows", n), slog.Duration("retention", retention))
		}
		return err
	}}, nil
}
