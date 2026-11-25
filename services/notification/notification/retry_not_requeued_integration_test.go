//go:build integration

// E07 evidence: a failed send puts the delivery row back to 'pending' and
// returns the error so the QUEUE retries it. Nothing else may re-enqueue the
// row early: the sweeper (every 2s in the worker) must leave a pending row it
// enqueued within the last RequeueAfter alone, and a row claimed again after a
// failure is claimed by the Asynq retry, not by the sweeper. The retry gaps the
// E2E run saw (about 2s, then about 6s, flat) came from the queue's 5s
// delayed-task check (libs/asynqx), not from this path.
package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFailedSend_IsNotReEnqueuedBySweepBeforeRequeueAfter(t *testing.T) {
	env := setup(t)
	student := uuid.New()
	register(t, env.DB, student, "ada@example.com", "Ada", 1)
	enq := &memEnqueuer{}
	if n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil || n != 1 {
		t.Fatalf("first sweep enqueued %d (err %v), want 1", n, err)
	}
	task := enq.drain()[0]

	handle := NewDeliveryHandler(env.DB, &recordingProvider{fn: func(int) error { return errors.New("connection refused") }})
	for i := 1; i <= 3; i++ {
		if err := handle(context.Background(), task); err == nil {
			t.Fatalf("run %d: want the send error returned to the queue", i)
		}
		d := deliveries(t, env.DB)[0]
		if d.Status != "pending" || d.Attempts != i {
			t.Fatalf("run %d: status=%s attempts=%d, want pending and %d", i, d.Status, d.Attempts, i)
		}
		// The worker sweeps every 2s with RequeueAfter at its default (5m). A
		// failed row is pending again, but it was enqueued moments ago.
		if n, err := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{}); err != nil || n != 0 {
			t.Fatalf("run %d: the sweeper re-enqueued a failed row (%d, err %v): it would bypass the queue's backoff", i, n, err)
		}
	}
	// Only once RequeueAfter has passed does the sweep act (the lost-task repair).
	if n, _ := EnqueuePending(context.Background(), env.DB, enq, SweepOptions{RequeueAfter: time.Nanosecond}); n != 1 {
		t.Fatalf("a row older than RequeueAfter must be re-enqueued by the repair path, got %d", n)
	}
}
