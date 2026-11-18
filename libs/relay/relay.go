// Package relay is the bounded worker pool that drains
// libs/outbox and dispatches each event to every registered
// handler (RFC §6.3, ADR-0006) — the piece that turns "an event was
// recorded" into "an event was acted on." It's where SmartCourse.md §7's
// worker-pool requirements (fixed-size pool, job queue, graceful shutdown,
// retry, backpressure) are demonstrated against real, useful work, not a
// synthetic example: without this package, every outbox row written by
// identity/course/enrollment/progress just accumulates, unread.
//
// Today this dispatches to in-process Go handlers, not Kafka consumers —
// Kafka, the Schema Registry, and Protobuf encoding are still M3 scope
// (RFC §21.0). See ADR-0021 for why this exists ahead of Kafka rather than
// waiting: when Kafka lands, this package's poll loop is replaced by
// "publish to Kafka," and today's in-process Handler functions become
// Kafka consumer handlers with the same signature — the fan-out and
// idempotency logic here doesn't change.
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// DecodedEvent is an outbox row, decoded for handler consumption.
type DecodedEvent struct {
	OutboxID      int64
	EventID       uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	SchemaVersion int
	Payload       json.RawMessage
	OccurredAt    time.Time
	Attempts      int
	// TraceID/SpanID are the W3C trace context the writer recorded (RFC
	// §6.4); empty when the row had none. ActorID is nil for
	// system-originated events or rows written before migration 000003.
	TraceID string
	SpanID  string
	ActorID *uuid.UUID
}

// HandlerFunc processes one event for one named consumer. It receives tx
// so the handler's effect and the processed_events idempotency marker
// commit in the same transaction (RFC §6.6) — exactly-once from the
// consumer's perspective despite the outbox's at-least-once delivery.
type HandlerFunc func(ctx context.Context, tx *gorm.DB, evt DecodedEvent) error

// Handler is one named consumer's reaction to a set of event types.
// Multiple Handlers can register for the same event type — RFC §6.5's
// consumer table shows real events read by several consumers each (e.g.
// StudentEnrolled read by analytics, search, and notifications); this is
// the fan-out that makes that true here too.
type Handler struct {
	ConsumerName string
	EventTypes   map[string]bool
	Handle       HandlerFunc
}

func NewHandler(consumerName string, eventTypes []string, fn HandlerFunc) Handler {
	set := make(map[string]bool, len(eventTypes))
	for _, t := range eventTypes {
		set[t] = true
	}
	return Handler{ConsumerName: consumerName, EventTypes: set, Handle: fn}
}

type Relay struct {
	db           *gorm.DB
	pollInterval time.Duration
	batchSize    int
	workerCount  int
	maxAttempts  int

	handlers []Handler

	// cooldown is process-local (RFC §10.3): rows THIS instance just failed
	// to deliver and when it may retry them, so a failing row is not
	// re-claimed in a hot loop. Cross-replica safety comes from the row
	// lock held for the whole delivery.
	cooldownMu sync.Mutex
	cooldown   map[int64]time.Time

	processed atomic.Int64
	failed    atomic.Int64
	givenUp   atomic.Int64
}

type Option func(*Relay)

func WithPollInterval(d time.Duration) Option { return func(r *Relay) { r.pollInterval = d } }
func WithBatchSize(n int) Option              { return func(r *Relay) { r.batchSize = n } }
func WithWorkerCount(n int) Option            { return func(r *Relay) { r.workerCount = n } }
func WithMaxAttempts(n int) Option            { return func(r *Relay) { r.maxAttempts = n } }

// New builds a Relay. Defaults match RFC §6.3 (200ms poll interval) and
// §10.2 (pool sizes bounded by the database connection budget, not CPU —
// 4 workers is conservative for a single relay instance sharing a pool
// with the API).
func New(db *gorm.DB, opts ...Option) *Relay {
	r := &Relay{
		db:           db,
		pollInterval: 200 * time.Millisecond,
		batchSize:    100,
		workerCount:  4,
		maxAttempts:  5,
		cooldown:     make(map[int64]time.Time),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Relay) Register(h Handler) {
	r.handlers = append(r.handlers, h)
}

// Stats reports processed/failed/given-up counts — sync/atomic, not a
// mutex, because a handful of independent counters is exactly the case
// atomic.Int64 fits and a mutex would be the wrong tool for (RFC §10.3:
// "Metrics counters — sync/atomic — Single machine word, no critical
// section"). A poor-man's metric ahead of Prometheus (M4).
func (r *Relay) Stats() (processed, failed, givenUp int64) {
	return r.processed.Load(), r.failed.Load(), r.givenUp.Load()
}

// Run drains the outbox until ctx is cancelled. Bounded worker pool
// (SmartCourse.md §7 — "Fixed-size worker pools... preventing resource
// exhaustion"): a fixed number of workers, each claiming one row at a time.
//
// Shutdown is graceful (§7 — "Graceful worker shutdown"): on cancellation,
// workers stop claiming new rows and a sync.WaitGroup blocks Run's return
// until every worker finishes the event it is on. A kill mid-job is still
// safe (at-least-once delivery + idempotent handlers), just wasteful.
func (r *Relay) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < r.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r.worker(ctx, workerID)
		}(i)
	}
	<-ctx.Done()
	wg.Wait()
	p, f, g := r.Stats()
	slog.Info("relay stopped", slog.Int64("processed", p), slog.Int64("failed", f), slog.Int64("given_up", g))
}

// worker claims ONE unpublished row at a time inside its own transaction and
// keeps that transaction (and so the FOR UPDATE row lock) open until the
// handlers have run and the row is marked published (or its attempt count
// and last_error recorded). This is what makes SKIP LOCKED actually protect
// processing across replicas (ADR-0006, F-04): a second replica skips the
// row for as long as the first is handling it. The pool is bounded by
// workerCount (each worker holds one connection for the claim plus the
// handlers' own transactions), and backpressure is structural: a worker
// takes no new row until it finishes the current one.
//
// Graceful shutdown: ctx only stops a worker from claiming NEW rows. Once a
// row is claimed, delivery runs on a context detached from cancellation
// (bounded by a timeout) so the commit that records the result cannot fail
// with "context canceled" at the moment the work is done.
func (r *Relay) worker(ctx context.Context, id int) {
	for ctx.Err() == nil {
		claimed, err := r.claimAndProcess(ctx)
		if err != nil {
			slog.Error("relay poll failed", slog.Int("worker", id), slog.Any("error", err))
		}
		if claimed && err == nil {
			continue // more work may be waiting; don't sleep
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.pollInterval):
		}
	}
}

type outboxRow struct {
	ID            int64
	EventID       uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	SchemaVersion int
	Payload       []byte
	OccurredAt    time.Time
	Attempts      int
	TraceID       *string
	SpanID        *string
	ActorID       *uuid.UUID
}

// claimAndProcess returns claimed=true when a row was found and delivered
// (successfully or not).
func (r *Relay) claimAndProcess(ctx context.Context) (claimed bool, err error) {
	if ctx.Err() != nil {
		return false, nil
	}
	workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	tx := r.db.WithContext(workCtx).Begin()
	if tx.Error != nil {
		return false, fmt.Errorf("begin claim tx: %w", tx.Error)
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	skip := r.activeCooldowns()
	q := `SELECT id, event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, occurred_at, attempts,
		trace_id, span_id, actor_id
		FROM outbox WHERE published_at IS NULL`
	args := []any{}
	if len(skip) > 0 {
		q += ` AND id NOT IN ?`
		args = append(args, skip)
	}
	q += ` ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`
	var rows []outboxRow
	if err := tx.Raw(q, args...).Scan(&rows).Error; err != nil {
		return false, fmt.Errorf("poll outbox: %w", err)
	}
	if len(rows) == 0 {
		return false, nil
	}
	row := rows[0]
	evt := DecodedEvent{
		OutboxID: row.ID, EventID: row.EventID, AggregateType: row.AggregateType,
		AggregateID: row.AggregateID, EventType: row.EventType, SchemaVersion: row.SchemaVersion,
		Payload: row.Payload, OccurredAt: row.OccurredAt, Attempts: row.Attempts,
		TraceID: deref(row.TraceID), SpanID: deref(row.SpanID), ActorID: row.ActorID,
	}

	if lastErr := r.process(workCtx, evt); lastErr != nil {
		err = r.recordFailure(tx, evt, lastErr)
	} else {
		err = tx.Exec(`UPDATE outbox SET published_at = ? WHERE id = ?`, time.Now().UTC(), evt.OutboxID).Error
		if err == nil {
			r.processed.Add(1)
		}
	}
	if err != nil {
		return true, fmt.Errorf("record result for outbox %d: %w", evt.OutboxID, err)
	}
	if err := tx.Commit().Error; err != nil {
		return true, fmt.Errorf("commit claim tx: %w", err)
	}
	committed = true
	return true, nil
}

func (r *Relay) activeCooldowns() []int64 {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()
	now := time.Now()
	var ids []int64
	for id, until := range r.cooldown {
		if now.After(until) {
			delete(r.cooldown, id)
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// process runs every registered handler for evt.EventType and returns a
// non-nil error summarising failures. One handler's failure doesn't block
// another's (RFC §4.6): each runs in its own transaction with its own
// idempotency check.
func (r *Relay) process(ctx context.Context, evt DecodedEvent) error {
	var errs []error
	for _, h := range r.handlers {
		if !h.EventTypes[evt.EventType] {
			continue
		}
		if err := r.runOne(ctx, h, evt); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", h.ConsumerName, err))
			r.failed.Add(1)
			slog.Error("relay handler failed",
				slog.String("consumer", h.ConsumerName), slog.String("event_type", evt.EventType),
				slog.Int64("outbox_id", evt.OutboxID), slog.Any("error", err))
		}
	}
	return errors.Join(errs...)
}

// runOne is RFC §6.6's idempotency rule, concretely: check
// processed_events before acting, write the marker in the SAME
// transaction as the handler's own effect — so a replayed event cannot
// double-apply. A panic in the handler is recovered and reported as a
// failed delivery (F-05); the surrounding transaction is rolled back.
func (r *Relay) runOne(ctx context.Context, h Handler, evt DecodedEvent) (err error) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("relay handler panicked",
				slog.String("consumer", h.ConsumerName), slog.Int64("outbox_id", evt.OutboxID),
				slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var already int64
		if err := tx.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = ? AND event_id = ?`,
			h.ConsumerName, evt.EventID).Scan(&already).Error; err != nil {
			return err
		}
		if already > 0 {
			return nil // this consumer has already handled this event — no-op
		}
		if err := h.Handle(ctx, tx, evt); err != nil {
			return err
		}
		return tx.Exec(`INSERT INTO processed_events (consumer_name, event_id) VALUES (?, ?)`,
			h.ConsumerName, evt.EventID).Error
	})
}

const maxLastErrorLen = 2000

// recordFailure is the retry mechanism (SmartCourse.md §7). It persists the
// attempt count and last_error in the claim transaction. No dead-letter
// store exists yet (MongoDB failed_events is M3 scope, RFC §21.0), so
// exceeding maxAttempts means giving up by marking the row published
// anyway — logged loudly at ERROR, with the reason kept in last_error.
func (r *Relay) recordFailure(tx *gorm.DB, evt DecodedEvent, cause error) error {
	attempts := evt.Attempts + 1
	msg := cause.Error()
	if len(msg) > maxLastErrorLen {
		msg = msg[:maxLastErrorLen]
	}
	msg = strings.ToValidUTF8(msg, "")
	if attempts >= r.maxAttempts {
		r.givenUp.Add(1)
		slog.Error("relay: giving up on event after max attempts",
			slog.String("event_type", evt.EventType), slog.Int64("outbox_id", evt.OutboxID), slog.Int("attempts", attempts))
		return tx.Exec(`UPDATE outbox SET attempts = ?, last_error = ?, published_at = ? WHERE id = ?`,
			attempts, msg, time.Now().UTC(), evt.OutboxID).Error
	}
	r.cooldownMu.Lock()
	r.cooldown[evt.OutboxID] = time.Now().Add(r.pollInterval)
	r.cooldownMu.Unlock()
	return tx.Exec(`UPDATE outbox SET attempts = ?, last_error = ? WHERE id = ?`, attempts, msg, evt.OutboxID).Error
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
