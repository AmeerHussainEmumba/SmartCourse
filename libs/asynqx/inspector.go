package asynqx

import (
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
)

// QueueStats is a point-in-time view of one queue, shaped for a metrics
// exporter: Pending (plus Scheduled and Retry) is the backlog behind
// asynq_queue_depth, Retry is the size of the retry set behind
// asynq_retry_count.
type QueueStats struct {
	Queue     string
	Pending   int // ready to run
	Active    int // currently executing
	Scheduled int // delayed first runs
	Retry     int // failed and waiting for the next attempt
	Archived  int // exhausted retries (Asynq's dead set)
	Completed int // retained after success (see Job.Retention)
	Paused    bool
	// Latency is how long the oldest pending task has waited.
	Latency time.Duration
}

// Depth is the number of tasks not yet finished and not parked:
// pending + active + scheduled + retry.
func (q QueueStats) Depth() int { return q.Pending + q.Active + q.Scheduled + q.Retry }

// Inspector reads queue state. Safe for concurrent use.
type Inspector struct {
	i *asynq.Inspector
}

// NewInspector connects to Redis. Close releases the connection.
func NewInspector(redis RedisOpt) *Inspector {
	return &Inspector{i: asynq.NewInspector(redis)}
}

// Close closes the underlying Redis connection.
func (i *Inspector) Close() error { return i.i.Close() }

// Stats returns stats for the named queues, or the three standard queues when
// none are named. A queue that has never held a task reports zeros rather than
// an error, so a gauge exists from the first scrape.
func (i *Inspector) Stats(queues ...string) ([]QueueStats, error) {
	if len(queues) == 0 {
		queues = []string{QueueCritical, QueueDefault, QueueLow}
	}
	out := make([]QueueStats, 0, len(queues))
	for _, q := range queues {
		info, err := i.i.GetQueueInfo(q)
		if err != nil {
			if errors.Is(err, asynq.ErrQueueNotFound) {
				out = append(out, QueueStats{Queue: q})
				continue
			}
			return nil, fmt.Errorf("asynqx: queue info %s: %w", q, err)
		}
		out = append(out, QueueStats{
			Queue: q, Pending: info.Pending, Active: info.Active, Scheduled: info.Scheduled,
			Retry: info.Retry, Archived: info.Archived, Completed: info.Completed,
			Paused: info.Paused, Latency: info.Latency,
		})
	}
	return out, nil
}

// TaskState returns the lifecycle state of a task ("pending", "active",
// "scheduled", "retry", "archived", "completed", "aggregating") and whether
// the task exists.
func (i *Inspector) TaskState(queue, id string) (state string, found bool, err error) {
	info, err := i.i.GetTaskInfo(queue, id)
	switch {
	case errors.Is(err, asynq.ErrTaskNotFound), errors.Is(err, asynq.ErrQueueNotFound):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("asynqx: task info %s/%s: %w", queue, id, err)
	}
	return info.State.String(), true, nil
}
