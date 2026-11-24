package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// TaskTypeDeliver is the queue task type the Asynq side registers
// NewDeliveryHandler under. The payload is DeliveryPayload(id).
const TaskTypeDeliver = "notification:deliver"

// Enqueuer hands a delivery task to the queue. Implementations (the Asynq
// adapter) must treat an already-queued duplicate as success: the sweep may
// legitimately enqueue a row twice, and the delivery handler is idempotent.
type Enqueuer interface {
	Enqueue(ctx context.Context, taskType string, payload []byte) error
}

// DeliveryPayload is the task payload for one delivery.
func DeliveryPayload(id uuid.UUID) []byte {
	b, _ := json.Marshal(struct {
		DeliveryID uuid.UUID `json:"delivery_id"`
	}{id})
	return b
}

// SweepOptions tunes EnqueuePending.
type SweepOptions struct {
	// Batch caps rows handled per call (default 100).
	Batch int
	// RequeueAfter is how long a pending row stays "already enqueued" before
	// the sweep enqueues it again (default 5m). It must exceed the queue's
	// normal first-delivery latency, otherwise every sweep double-queues; a
	// task that was lost (Redis flushed, queue entry archived) is retried
	// after this long.
	RequeueAfter time.Duration
	// Lease is how long a 'sending' claim is honoured before it is treated
	// as a crashed worker and the row becomes claimable again (default 5m;
	// same value the delivery handler uses).
	Lease time.Duration
	// MaxAttempts: pending rows at or above this many claims are marked
	// failed instead of enqueued again (default 10).
	MaxAttempts int
}

func (o SweepOptions) withDefaults() SweepOptions {
	if o.Batch <= 0 {
		o.Batch = 100
	}
	if o.RequeueAfter <= 0 {
		o.RequeueAfter = 5 * time.Minute
	}
	if o.Lease <= 0 {
		o.Lease = defaultLease
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = defaultMaxAttempts
	}
	return o
}

// EnqueuePending is the bridge between the delivery table and the queue. It
// (1) promotes parked rows whose recipient has since arrived, (2) fails rows
// that exhausted their attempts, and (3) enqueues every pending row not
// enqueued recently, plus 'sending' rows whose lease expired. It returns the
// number enqueued.
//
// Why a sweep and not an enqueue inside the event handler: the relay commits
// the handler's transaction itself, so the handler cannot enqueue "after
// commit". The delivery row is therefore the source of truth and this
// function is the retry: if the process dies after the commit and before
// the enqueue (or Redis is down, or a queued task is lost), the row is
// still pending and the next sweep enqueues it. Enqueueing can happen twice
// for one row (a crash after Enqueue but before the row is stamped); that is
// harmless because the delivery task is idempotent.
//
// Several workers may sweep at once: rows are locked FOR UPDATE SKIP LOCKED
// for the duration of the batch, so each row is handled by one sweeper at a
// time. The worker should call this on a short interval (RunSweeper, a few
// seconds) and once at startup.
func EnqueuePending(ctx context.Context, db *gorm.DB, enq Enqueuer, opts SweepOptions) (int, error) {
	opts = opts.withDefaults()
	if _, err := PromoteParked(ctx, db, nil); err != nil {
		return 0, fmt.Errorf("promote parked: %w", err)
	}
	if n, err := failExhausted(ctx, db, opts.MaxAttempts); err != nil {
		return 0, fmt.Errorf("fail exhausted: %w", err)
	} else if n > 0 {
		slog.Error("notification deliveries failed permanently after max attempts", slog.Int64("count", n))
	}

	enqueued := 0
	var firstErr error
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ids []uuid.UUID
		if err := tx.Raw(`
			SELECT id FROM notification.deliveries
			WHERE (status = 'pending' AND (enqueued_at IS NULL OR enqueued_at < now() - make_interval(secs => ?)))
			   OR (status = 'sending' AND claimed_at < now() - make_interval(secs => ?))
			ORDER BY created_at
			LIMIT ?
			FOR UPDATE SKIP LOCKED`, opts.RequeueAfter.Seconds(), opts.Lease.Seconds(), opts.Batch).Scan(&ids).Error; err != nil {
			return err
		}
		for _, id := range ids {
			if err := enq.Enqueue(ctx, TaskTypeDeliver, DeliveryPayload(id)); err != nil {
				// Leave the row as is: the next sweep retries it. Keep going,
				// one bad enqueue must not block the rest of the batch.
				if firstErr == nil {
					firstErr = fmt.Errorf("enqueue delivery %s: %w", id, err)
				}
				continue
			}
			// A stamped 'sending' row is reset so the next claim is clean.
			if err := tx.Exec(`UPDATE notification.deliveries
				SET enqueued_at = now(), status = CASE WHEN status = 'sending' THEN 'pending' ELSE status END
				WHERE id = ?`, id).Error; err != nil {
				return err
			}
			enqueued++
		}
		return nil
	})
	if err != nil {
		return enqueued, err
	}
	return enqueued, firstErr
}

// RunSweeper calls EnqueuePending every interval until ctx ends. Errors are
// logged, not fatal: the next tick retries. Call it once from the worker.
func RunSweeper(ctx context.Context, db *gorm.DB, enq Enqueuer, interval time.Duration, opts SweepOptions) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if n, err := EnqueuePending(ctx, db, enq, opts); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("notification sweep failed", slog.Any("error", err), slog.Int("enqueued", n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
