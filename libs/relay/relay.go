// Package relay is the bounded worker pool that drains
// libs/outbox and dispatches each event to every registered
// handler (RFC §6.3, ADR-0006) — the piece that turns "an event was
// recorded" into "an event was acted on." It's where SmartCourse.md §7's
// worker-pool requirements (fixed-size pool, job queue, graceful shutdown,
// retry, backpressure) are demonstrated against real, useful work, not a
// synthetic example: without this package, every outbox row written by
// identity/course/enrollment/progress just accumulates, unread.
//
// In the per-service topology (ADR-0033) every relay is built WithPublisher
// (ADR-0029): the per-row step is "hand the row to a Publisher and mark it
// published only once the Publisher returned nil", and the handlers are run by
// Kafka consumers in libs/eventbus, through Deliver. This package imports no
// Kafka client: the Publisher is the seam, implemented by libs/eventbus.
//
// A Relay built without WithPublisher still runs registered handlers
// in-process (the original ADR-0021 behaviour, superseded by ADR-0033). No
// service uses that path; it remains only in this library and its tests.
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

	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
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

	// publisher, when set, switches the relay to publish mode (ADR-0029): the
	// per-row step publishes instead of running handlers.
	publisher Publisher
	// publishBackoff is how long a row whose publish failed for a transient
	// reason (broker down) is left alone by this instance.
	publishBackoff time.Duration
	// sink receives events the relay gives up on, before they are dropped.
	// Nil keeps the log-and-drop behaviour (see DeadLetterSink).
	sink DeadLetterSink
	// observer and handlerHook are the observability seams (see Observer);
	// nil means none.
	observer    Observer
	handlerHook HandlerHook

	// cooldown is process-local (RFC §10.3): rows THIS instance just failed
	// to deliver and when it may retry them, so a failing row is not
	// re-claimed in a hot loop. Cross-replica safety comes from the row
	// lock held for the whole delivery.
	cooldownMu sync.Mutex
	cooldown   map[int64]time.Time

	processed    atomic.Int64
	failed       atomic.Int64
	givenUp      atomic.Int64
	parkFailures atomic.Int64
}

type Option func(*Relay)

func WithPollInterval(d time.Duration) Option { return func(r *Relay) { r.pollInterval = d } }
func WithBatchSize(n int) Option              { return func(r *Relay) { r.batchSize = n } }
func WithWorkerCount(n int) Option            { return func(r *Relay) { r.workerCount = n } }
func WithMaxAttempts(n int) Option            { return func(r *Relay) { r.maxAttempts = n } }

// WithPublisher switches the relay to publish mode (ADR-0029). Registered
// handlers are then not run by the relay: consumers run them from the broker.
// A nil publisher keeps the legacy in-process delivery (unused by any service).
func WithPublisher(p Publisher) Option { return func(r *Relay) { r.publisher = p } }

// WithPublishBackoff sets how long a row that failed to publish for a
// transient reason is left alone by this instance (default 1s).
func WithPublishBackoff(d time.Duration) Option { return func(r *Relay) { r.publishBackoff = d } }

// WithDeadLetterSink sets where the relay parks an event it gives up on. See
// DeadLetterSink for the contract. Without one the relay logs and drops.
func WithDeadLetterSink(s DeadLetterSink) Option { return func(r *Relay) { r.sink = s } }

// WithObserver installs the per-row delivery observer (see Observer).
func WithObserver(o Observer) Option { return func(r *Relay) { r.observer = o } }

// WithHandlerHook installs the per-handler outcome hook (see HandlerHook).
func WithHandlerHook(h HandlerHook) Option { return func(r *Relay) { r.handlerHook = h } }

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

		publishBackoff: time.Second,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Relay) Register(h Handler) {
	r.handlers = append(r.handlers, h)
}

// Handlers returns the registered handlers. In publish mode the
// service's consumers (libs/eventbus) are built from these handlers.
func (r *Relay) Handlers() []Handler {
	return append([]Handler(nil), r.handlers...)
}

// Publishing reports whether the relay is in publish mode.
func (r *Relay) Publishing() bool { return r.publisher != nil }

// ParkFailures counts the times the dead-letter sink refused an event; the
// event's row stays unpublished each time.
func (r *Relay) ParkFailures() int64 { return r.parkFailures.Load() }

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
		FROM outbox o WHERE published_at IS NULL`
	args := []any{}
	if len(skip) > 0 {
		q += ` AND id NOT IN ?`
		args = append(args, skip)
	}
	if r.publisher != nil {
		// Publish mode keeps per-aggregate order (ADR-0029): a row is only
		// claimable while no earlier row of its aggregate is unpublished. An
		// earlier row that another worker holds (not yet acknowledged), one
		// in cooldown, or one that failed all keep their successors back, so
		// two workers never produce the same key concurrently and a retry
		// cannot be overtaken. Other aggregates are not held up.
		q += ` AND NOT EXISTS (SELECT 1 FROM outbox e WHERE e.aggregate_id = o.aggregate_id AND e.published_at IS NULL AND e.id < o.id)`
	}
	q += ` ORDER BY id LIMIT 1 FOR UPDATE OF o SKIP LOCKED`
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

	// dctx is workCtx with whatever the observer attached (a span continuing
	// the row's trace); everything that runs "for" the event uses it, while the
	// claim transaction keeps workCtx.
	mode := ModeProcess
	if r.publisher != nil {
		mode = ModePublish
	}
	dctx, finish := r.observe(workCtx, evt, mode)
	var failures []consumerFailure
	if r.publisher != nil {
		failures = r.publish(dctx, evt)
	} else {
		failures = r.process(dctx, evt)
	}
	if len(failures) > 0 {
		errs := make([]error, len(failures))
		for i, f := range failures {
			errs[i] = fmt.Errorf("%s: %w", f.consumer, f.err)
		}
		finish(errors.Join(errs...))
		err = r.recordFailure(dctx, tx, evt, failures)
	} else {
		finish(nil)
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

// publish hands evt to the Publisher. The caller marks the row published only
// when this returns no failures, i.e. after the transport's acknowledgement
// (ADR-0029): a crash in between re-publishes the row (a duplicate that
// consumers absorb through processed_events), never loses it. A panic in the
// Publisher is reported as a failed attempt.
func (r *Relay) publish(ctx context.Context, evt DecodedEvent) (failures []consumerFailure) {
	defer func() {
		if p := recover(); p != nil {
			eventLog(ctx, evt).Error("relay publisher panicked", slog.Int64("outbox_id", evt.OutboxID),
				slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
			failures = []consumerFailure{{RelayConsumer, fmt.Errorf("publisher panic: %v", p)}}
		}
	}()
	if err := r.publisher.Publish(ctx, evt); err != nil {
		r.failed.Add(1)
		eventLog(ctx, evt).Error("relay publish failed",
			slog.String("event_type", evt.EventType), slog.Int64("outbox_id", evt.OutboxID),
			slog.Bool("transient", IsTransient(err)), slog.Any("error", err))
		return []consumerFailure{{RelayConsumer, err}}
	}
	return nil
}

// process runs every registered handler for evt.EventType and returns the
// failures. One handler's failure doesn't block another's (RFC §4.6): each
// runs in its own transaction with its own idempotency check.
func (r *Relay) process(ctx context.Context, evt DecodedEvent) []consumerFailure {
	var failures []consumerFailure
	for _, h := range r.handlers {
		if !h.EventTypes[evt.EventType] {
			continue
		}
		duplicate, err := DeliverEvent(ctx, r.db, h, evt)
		r.hook(h.ConsumerName, duplicate, err)
		if err != nil {
			failures = append(failures, consumerFailure{h.ConsumerName, err})
			r.failed.Add(1)
			eventLog(ctx, evt).Error("relay handler failed",
				slog.String("consumer", h.ConsumerName), slog.String("event_type", evt.EventType),
				slog.Int64("outbox_id", evt.OutboxID), slog.Any("error", err))
		}
	}
	return failures
}

const maxLastErrorLen = 2000

// recordFailure is the retry mechanism (SmartCourse.md §7). It persists the
// attempt count and last_error in the claim transaction.
//
// A failure marked Transient (the transport is down, not the event bad) records
// last_error and backs the row off, but does not count an attempt: an outage
// must not use up the budget of good events.
//
// Exceeding maxAttempts means giving up. With a DeadLetterSink the event is
// parked first (one DeadLetter per failed consumer) and the row is marked
// published only after every Park succeeded; if one fails the row stays
// unpublished, the failure is logged and counted, and the next claim tries
// again, so nothing is dropped unparked. Without a sink the row is marked
// published anyway, logged loudly, with the reason kept in last_error: the
// documented gap.
func (r *Relay) recordFailure(ctx context.Context, tx *gorm.DB, evt DecodedEvent, failures []consumerFailure) error {
	errs := make([]error, 0, len(failures))
	transient := true
	for _, f := range failures {
		errs = append(errs, fmt.Errorf("%s: %w", f.consumer, f.err))
		transient = transient && IsTransient(f.err)
	}
	msg := errors.Join(errs...).Error()
	if len(msg) > maxLastErrorLen {
		msg = msg[:maxLastErrorLen]
	}
	msg = strings.ToValidUTF8(msg, "")

	if transient {
		r.setCooldown(evt.OutboxID, r.publishBackoff)
		return tx.Exec(`UPDATE outbox SET last_error = ? WHERE id = ?`, msg, evt.OutboxID).Error
	}

	attempts := evt.Attempts + 1
	if attempts >= r.maxAttempts {
		if r.sink != nil {
			evt.Attempts = attempts
			parked := true
			for _, f := range failures {
				if err := park(ctx, r.sink, DeadLetter{Event: evt, Consumer: f.consumer, Err: f.err, Attempts: attempts}); err != nil {
					parked = false
					r.parkFailures.Add(1)
					eventLog(ctx, evt).Error("relay: dead-letter sink refused the event; leaving the row unpublished",
						slog.String("consumer", f.consumer), slog.String("event_type", evt.EventType),
						slog.Int64("outbox_id", evt.OutboxID), slog.Any("error", err))
				}
			}
			if !parked {
				r.setCooldown(evt.OutboxID, max(r.publishBackoff, r.pollInterval))
				return tx.Exec(`UPDATE outbox SET attempts = ?, last_error = ? WHERE id = ?`, attempts, msg, evt.OutboxID).Error
			}
		}
		r.givenUp.Add(1)
		eventLog(ctx, evt).Error("relay: giving up on event after max attempts",
			slog.String("event_type", evt.EventType), slog.Int64("outbox_id", evt.OutboxID), slog.Int("attempts", attempts),
			slog.Bool("parked", r.sink != nil))
		return tx.Exec(`UPDATE outbox SET attempts = ?, last_error = ?, published_at = ? WHERE id = ?`,
			attempts, msg, time.Now().UTC(), evt.OutboxID).Error
	}
	r.setCooldown(evt.OutboxID, r.pollInterval)
	return tx.Exec(`UPDATE outbox SET attempts = ?, last_error = ? WHERE id = ?`, attempts, msg, evt.OutboxID).Error
}

func (r *Relay) setCooldown(id int64, d time.Duration) {
	r.cooldownMu.Lock()
	r.cooldown[id] = time.Now().Add(d)
	r.cooldownMu.Unlock()
}

// eventLog is the logger for a line about one event: event_id (RFC §12.4) plus
// the trace_id and span_id of the active span when the observer started one.
func eventLog(ctx context.Context, evt DecodedEvent) *slog.Logger {
	return logging.WithEventID(logging.Correlated(ctx), evt.EventID.String())
}

// observe starts the observer's span for evt, or returns ctx and a no-op. A
// panicking observer is logged and ignored: observability must never fail a
// delivery.
func (r *Relay) observe(ctx context.Context, evt DecodedEvent, mode string) (octx context.Context, finish func(error)) {
	noop := func(error) {}
	if r.observer == nil {
		return ctx, noop
	}
	defer func() {
		if p := recover(); p != nil {
			slog.Error("relay observer panicked; delivering without it", slog.Any("panic", p))
			octx, finish = ctx, noop
		}
	}()
	octx, fin := r.observer(ctx, evt, mode)
	if octx == nil {
		octx = ctx
	}
	if fin == nil {
		return octx, noop
	}
	return octx, func(err error) {
		defer func() {
			if p := recover(); p != nil {
				slog.Error("relay observer finish panicked", slog.Any("panic", p))
			}
		}()
		fin(err)
	}
}

// hook reports one handler's outcome to the HandlerHook, shielding the relay
// from a panic in it.
func (r *Relay) hook(consumer string, duplicate bool, err error) {
	if r.handlerHook == nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			slog.Error("relay handler hook panicked", slog.Any("panic", p))
		}
	}()
	r.handlerHook(consumer, duplicate, err)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
