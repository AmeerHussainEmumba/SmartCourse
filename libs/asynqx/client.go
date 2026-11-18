package asynqx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
)

// Defaults applied by Client when a Job leaves a field zero.
const (
	DefaultMaxRetry = 5
	DefaultTimeout  = 30 * time.Second
)

// Job describes one logical unit of background work.
type Job struct {
	// Type routes the job to a handler registered with Server.Handle.
	Type    string
	Payload []byte

	// ID is required and must be deterministic: derive it from what makes the
	// job logically unique (see DeterministicID). A second Enqueue with the
	// same ID is a no-op while the first task still exists in Redis.
	ID string

	// Queue is critical, default or low; empty means default.
	Queue string

	// MaxRetry is the number of retries after the first attempt. Zero means
	// DefaultMaxRetry; use NoRetry for "never retry".
	MaxRetry int
	NoRetry  bool

	// Timeout bounds one attempt; zero means DefaultTimeout.
	Timeout time.Duration

	// ProcessIn delays first execution.
	ProcessIn time.Duration

	// Retention keeps a completed task (and so its ID) in Redis for this long.
	// Without it, a completed task is deleted and the same ID can be enqueued
	// again; set it to the window in which duplicates must stay no-ops.
	Retention time.Duration
}

// Result reports what Enqueue did.
type Result struct {
	ID    string
	Queue string
	// Duplicate is true when a task with this ID already existed and nothing
	// was enqueued. It is not an error: the logical job is already queued.
	Duplicate bool
}

// ErrInvalidJob is returned for a Job that cannot be enqueued as described.
var ErrInvalidJob = errors.New("asynqx: invalid job")

// Client enqueues jobs. It is safe for concurrent use.
type Client struct {
	c *asynq.Client
}

// NewClient connects to Redis. Close releases the connection.
func NewClient(redis RedisOpt) *Client {
	return &Client{c: asynq.NewClient(redis)}
}

// Close closes the underlying Redis connection.
func (c *Client) Close() error { return c.c.Close() }

// Enqueue adds the job unless one with the same ID already exists.
func (c *Client) Enqueue(ctx context.Context, j Job) (Result, error) {
	opts, queue, err := j.options()
	if err != nil {
		return Result{}, err
	}
	info, err := c.c.EnqueueContext(ctx, asynq.NewTask(j.Type, j.Payload), opts...)
	switch {
	case errors.Is(err, asynq.ErrTaskIDConflict):
		return Result{ID: j.ID, Queue: queue, Duplicate: true}, nil
	case err != nil:
		return Result{}, fmt.Errorf("asynqx: enqueue %s: %w", j.Type, err)
	}
	return Result{ID: info.ID, Queue: info.Queue}, nil
}

func (j Job) options() ([]asynq.Option, string, error) {
	switch {
	case j.Type == "":
		return nil, "", fmt.Errorf("%w: empty type", ErrInvalidJob)
	case j.ID == "":
		return nil, "", fmt.Errorf("%w: empty ID (a deterministic task ID is required)", ErrInvalidJob)
	}
	queue := j.Queue
	if queue == "" {
		queue = QueueDefault
	}
	if !validQueue(queue) {
		return nil, "", fmt.Errorf("%w: unknown queue %q", ErrInvalidJob, queue)
	}
	if j.MaxRetry < 0 || j.Timeout < 0 || j.ProcessIn < 0 || j.Retention < 0 {
		return nil, "", fmt.Errorf("%w: negative option", ErrInvalidJob)
	}
	maxRetry := j.MaxRetry
	if maxRetry == 0 && !j.NoRetry {
		maxRetry = DefaultMaxRetry
	}
	timeout := j.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	opts := []asynq.Option{
		asynq.TaskID(j.ID),
		asynq.Queue(queue),
		asynq.MaxRetry(maxRetry),
		asynq.Timeout(timeout),
	}
	if j.ProcessIn > 0 {
		opts = append(opts, asynq.ProcessIn(j.ProcessIn))
	}
	if j.Retention > 0 {
		opts = append(opts, asynq.Retention(j.Retention))
	}
	return opts, queue, nil
}
