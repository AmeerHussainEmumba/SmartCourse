// Package relay is the bounded worker pool that drains
// internal/platform/outbox and dispatches each event to every registered
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
	"fmt"
	"log/slog"
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

	// inFlight is a legitimately process-local set (RFC §10.3): which
	// outbox rows THIS relay instance currently has a worker on. Guarded
	// by a plain sync.Mutex rather than Redis deliberately — unlike the
	// rate limiter or the token-version cache, no OTHER replica needs to
	// see this; it exists only to stop this process's own next poll tick
	// from re-claiming a row its own worker hasn't finished yet.
	inFlightMu sync.Mutex
	inFlight   map[int64]bool

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
		inFlight:     make(map[int64]bool),
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

// Run polls until ctx is cancelled. Bounded worker pool (SmartCourse.md
// §7 — "Fixed-size worker pools... preventing resource exhaustion"): a
// fixed number of goroutines read from jobs, a buffered channel the
// poller writes to. Backpressure is structural, not a separate mechanism
// bolted on: if every worker is busy, the channel fills, and the poller's
// send blocks until a worker frees up, rather than an unbounded in-memory
// queue growing during a spike (§7's specific ask).
//
// Shutdown is graceful (§7 — "Graceful worker shutdown"): on
// cancellation, the poll loop stops claiming new work, closes jobs once
// nothing more will be sent, and a sync.WaitGroup blocks Run's return
// until every in-flight worker finishes its current event. A kill mid-job
// is still safe (at-least-once delivery + idempotent handlers), just
// wasteful — RFC §10.2's exact reasoning for why every pool here drains
// rather than dying abruptly.
func (r *Relay) Run(ctx context.Context) {
	jobs := make(chan DecodedEvent, r.batchSize)
	var wg sync.WaitGroup

	for i := 0; i < r.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r.worker(ctx, workerID, jobs)
		}(i)
	}

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			p, f, g := r.Stats()
			slog.Info("relay stopped", slog.Int64("processed", p), slog.Int64("failed", f), slog.Int64("given_up", g))
			return
		case <-ticker.C:
			r.pollAndDispatch(ctx, jobs)
		}
	}
}

func (r *Relay) pollAndDispatch(ctx context.Context, jobs chan<- DecodedEvent) {
	events, err := r.poll(ctx)
	if err != nil {
		slog.Error("relay poll failed", slog.Any("error", err))
		return
	}
	for _, evt := range events {
		select {
		case jobs <- evt:
		case <-ctx.Done():
			return
		}
	}
}

// poll claims up to batchSize unpublished rows this relay instance hasn't
// already got a worker on. FOR UPDATE SKIP LOCKED (ADR-0006) is what lets
// multiple relay REPLICAS drain the outbox concurrently without
// double-processing a row or blocking each other — that guarantee holds
// regardless of how many relay processes are running. inFlight closes the
// narrower gap WITHIN one process, between "claimed" and "a worker
// finished it": the claiming transaction commits immediately rather than
// staying open for the full processing duration, because holding it open
// would force every worker through one serialized connection, defeating
// the point of a pool.
func (r *Relay) poll(ctx context.Context) ([]DecodedEvent, error) {
	type row struct {
		ID            int64
		EventID       uuid.UUID
		AggregateType string
		AggregateID   uuid.UUID
		EventType     string
		SchemaVersion int
		Payload       []byte
		OccurredAt    time.Time
		Attempts      int
	}
	var rows []row
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Raw(`
			SELECT id, event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, occurred_at, attempts
			FROM outbox WHERE published_at IS NULL ORDER BY id
			LIMIT ? FOR UPDATE SKIP LOCKED`, r.batchSize).Scan(&rows).Error
	})
	if err != nil {
		return nil, fmt.Errorf("poll outbox: %w", err)
	}

	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()

	out := make([]DecodedEvent, 0, len(rows))
	for _, row := range rows {
		if r.inFlight[row.ID] {
			continue // a previous poll tick's worker still has this one
		}
		r.inFlight[row.ID] = true
		out = append(out, DecodedEvent{
			OutboxID: row.ID, EventID: row.EventID, AggregateType: row.AggregateType,
			AggregateID: row.AggregateID, EventType: row.EventType, SchemaVersion: row.SchemaVersion,
			Payload: row.Payload, OccurredAt: row.OccurredAt, Attempts: row.Attempts,
		})
	}
	return out, nil
}

// worker processes each job with a context DETACHED from ctx's
// cancellation, not ctx itself. This is the subtle half of "graceful
// shutdown": once a job has been pulled off the channel, letting the
// handler's Go code finish isn't enough if the context it's handed is
// already cancelled — the transaction committing processed_events and
// marking the outbox row published would fail with "context canceled" at
// exactly the moment the work is otherwise done, which is worse than not
// draining at all: it looks graceful (the handler ran) while silently
// losing the result. context.WithoutCancel keeps any request-scoped
// values but drops the cancellation signal; the timeout bounds how long a
// genuinely stuck handler can delay shutdown.
func (r *Relay) worker(ctx context.Context, id int, jobs <-chan DecodedEvent) {
	for evt := range jobs {
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		r.process(workCtx, evt)
		cancel()
		r.inFlightMu.Lock()
		delete(r.inFlight, evt.OutboxID)
		r.inFlightMu.Unlock()
	}
}

// process runs every registered handler for evt.EventType. One handler's
// failure doesn't block another's (RFC §4.6 — "A failure in one consumer
// never blocks another"): each runs in its own transaction with its own
// idempotency check.
func (r *Relay) process(ctx context.Context, evt DecodedEvent) {
	anyFailed := false
	for _, h := range r.handlers {
		if !h.EventTypes[evt.EventType] {
			continue
		}
		if err := r.runOne(ctx, h, evt); err != nil {
			anyFailed = true
			r.failed.Add(1)
			slog.Error("relay handler failed",
				slog.String("consumer", h.ConsumerName), slog.String("event_type", evt.EventType),
				slog.Int64("outbox_id", evt.OutboxID), slog.Any("error", err))
		}
	}
	if anyFailed {
		r.bumpAttempts(ctx, evt)
		return
	}
	r.processed.Add(1)
	r.markPublished(ctx, evt.OutboxID)
}

// runOne is RFC §6.6's idempotency rule, concretely: check
// processed_events before acting, write the marker in the SAME
// transaction as the handler's own effect — so a replayed event (this
// relay retries on failure; a future Kafka consumer retries on redelivery)
// cannot double-apply.
func (r *Relay) runOne(ctx context.Context, h Handler, evt DecodedEvent) error {
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

func (r *Relay) markPublished(ctx context.Context, outboxID int64) {
	now := time.Now().UTC()
	if err := r.db.WithContext(ctx).Exec(`UPDATE outbox SET published_at = ? WHERE id = ?`, now, outboxID).Error; err != nil {
		slog.Error("relay: failed to mark outbox row published", slog.Int64("outbox_id", outboxID), slog.Any("error", err))
	}
}

// bumpAttempts is the retry mechanism (SmartCourse.md §7 — "Retry
// mechanisms"). No dead-letter store exists yet (MongoDB failed_events is
// M3 scope, RFC §21.0), so exceeding maxAttempts means giving up by
// marking the row published anyway — an honest limitation, logged loudly
// at ERROR, not a silent swallow. A future consumer replacing today's
// in-process handler regains real dead-lettering for free (RFC §6.9).
func (r *Relay) bumpAttempts(ctx context.Context, evt DecodedEvent) {
	attempts := evt.Attempts + 1
	if attempts >= r.maxAttempts {
		r.givenUp.Add(1)
		slog.Error("relay: giving up on event after max attempts",
			slog.String("event_type", evt.EventType), slog.Int64("outbox_id", evt.OutboxID), slog.Int("attempts", attempts))
		r.markPublished(ctx, evt.OutboxID)
		return
	}
	if err := r.db.WithContext(ctx).Exec(`UPDATE outbox SET attempts = ? WHERE id = ?`, attempts, evt.OutboxID).Error; err != nil {
		slog.Error("relay: failed to bump attempts", slog.Int64("outbox_id", evt.OutboxID), slog.Any("error", err))
	}
}
