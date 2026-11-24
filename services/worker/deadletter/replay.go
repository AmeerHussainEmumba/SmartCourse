package deadletter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// DefaultReplayTimeout bounds the handler's work for one record.
const DefaultReplayTimeout = 30 * time.Second

var (
	// ErrNotFound means no failed event has that event id.
	ErrNotFound = errors.New("deadletter: failed event not found")
	// ErrInvalidID means the event id is not a UUID.
	ErrInvalidID = errors.New("deadletter: event id is not a UUID")
)

// Outcome statuses.
const (
	// StatusReplayed: this call re-applied the event (or found it already
	// applied by the normal path) and moved the record parked -> replayed.
	StatusReplayed = "replayed"
	// StatusAlreadyReplayed: the record was already replayed; nothing was done.
	StatusAlreadyReplayed = "already_replayed"
	// StatusFailed: the handler failed again (the new error was added to the
	// record, which stays parked) or could not be run; see Outcome.Error.
	StatusFailed = "failed"
)

// Outcome is the result for one (event, consumer) record.
type Outcome struct {
	Consumer string
	Status   string
	// Applied is true when the handler actually ran in this call, false when
	// the consumer had already processed the event (its processed_events
	// marker existed) so only the record was marked.
	Applied bool
	Error   string
}

// Result is what Replay did for an event id: one Outcome per consumer that
// parked it.
type Result struct {
	EventID  string
	Outcomes []Outcome
}

// Replayed reports whether any record was moved to replayed by this call.
func (r *Result) Replayed() bool {
	for _, o := range r.Outcomes {
		if o.Status == StatusReplayed {
			return true
		}
	}
	return false
}

// Failed reports whether any record's replay failed.
func (r *Result) Failed() bool {
	for _, o := range r.Outcomes {
		if o.Status == StatusFailed {
			return true
		}
	}
	return false
}

// ReplayStore is the part of *mongostore.Store the Replayer uses.
type ReplayStore interface {
	ParkStore
	ListFailedEvents(ctx context.Context, f mongostore.FailedFilter, cursor string, limit int) (*mongostore.FailedPage, error)
	GetFailedEvent(ctx context.Context, eventID, consumer string) (*mongostore.FailedEvent, error)
	MarkReplayed(ctx context.Context, eventID, consumer string, at time.Time) (bool, error)
}

// Replayer re-applies parked events through the consumers' own handlers.
type Replayer struct {
	db       *gorm.DB
	store    ReplayStore
	handlers map[string][]relay.Handler // by consumer name
	timeout  time.Duration
	now      func() time.Time
}

// NewReplayer builds a Replayer. handlers is the registry of every consumer
// that can park an event, the same relay.Handler values the transport runs
// (several may share a consumer name, one per event type); a record whose
// consumer has no handler for its event type cannot be replayed.
func NewReplayer(db *gorm.DB, store ReplayStore, handlers []relay.Handler) *Replayer {
	reg := map[string][]relay.Handler{}
	for _, h := range handlers {
		reg[h.ConsumerName] = append(reg[h.ConsumerName], h)
	}
	return &Replayer{db: db, store: store, handlers: reg, timeout: DefaultReplayTimeout, now: func() time.Time { return time.Now().UTC() }}
}

// Replay re-applies every parked record of eventID (one per consumer that
// gave up on it).
//
// Each record goes through the normal idempotent path: the consumer's handler
// runs in a transaction together with its processed_events marker, so an
// event the consumer already handled is not applied twice. Only then is the
// record marked replayed, with mongostore's single conditional update, so of
// any number of concurrent replays exactly one transitions it. Replaying an
// already replayed record is a no-op reported as StatusAlreadyReplayed. If the
// handler fails again, the record stays parked with the new error appended.
//
// A crash after the handler committed and before the mark leaves the record
// parked; replaying again finds the marker, skips the handler and marks it.
//
// It returns ErrInvalidID for a malformed id and ErrNotFound when nothing was
// ever parked under it; a MongoDB failure is returned as is.
func (r *Replayer) Replay(ctx context.Context, eventID string) (*Result, error) {
	if r == nil || r.store == nil || r.db == nil {
		return nil, ErrNoStore
	}
	id, err := uuid.Parse(eventID)
	if err != nil {
		return nil, ErrInvalidID
	}
	eventID = id.String() // canonical form, as it was parked
	recs, err := r.records(ctx, eventID)
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, ErrNotFound
	}
	res := &Result{EventID: eventID}
	for _, rec := range recs {
		res.Outcomes = append(res.Outcomes, r.replayOne(ctx, rec))
	}
	return res, nil
}

// records lists the failed_events documents for one event id (without payloads).
func (r *Replayer) records(ctx context.Context, eventID string) ([]mongostore.FailedEvent, error) {
	var out []mongostore.FailedEvent
	cursor := ""
	for {
		page, err := r.store.ListFailedEvents(ctx, mongostore.FailedFilter{EventID: eventID}, cursor, mongostore.MaxPageSize)
		if err != nil {
			return nil, fmt.Errorf("deadletter: list failed events: %w", err)
		}
		out = append(out, page.Items...)
		if page.NextCursor == "" {
			return out, nil
		}
		cursor = page.NextCursor
	}
}

func (r *Replayer) replayOne(ctx context.Context, listed mongostore.FailedEvent) Outcome {
	o := Outcome{Consumer: listed.Consumer}
	if listed.Status == mongostore.StatusReplayed {
		o.Status = StatusAlreadyReplayed
		return o
	}
	rec, err := r.store.GetFailedEvent(ctx, listed.EventID, listed.Consumer)
	if err != nil {
		return failed(o, fmt.Errorf("load record: %w", err))
	}
	if rec.Status == mongostore.StatusReplayed { // replayed between the list and the get
		o.Status = StatusAlreadyReplayed
		return o
	}
	h, ok := r.handlerFor(rec.Consumer, rec.EventType)
	if !ok {
		return failed(o, fmt.Errorf("no handler registered for consumer %q and event type %q", rec.Consumer, rec.EventType))
	}
	evt, err := decode(rec)
	if err != nil {
		return failed(o, err)
	}

	applied, err := r.apply(ctx, h, evt)
	if err != nil {
		o.Status, o.Error = StatusFailed, err.Error()
		// Leave the record parked with this failure added to its history. If
		// even that fails the old record is untouched, which is still correct.
		if _, perr := r.store.ParkFailedEvent(ctx, mongostore.ParkInput{
			EventID: rec.EventID, Consumer: rec.Consumer, EventType: rec.EventType,
			Attempts: 1, Err: errMessage(fmt.Errorf("replay: %w", err)), FailedAt: r.now(),
		}); perr != nil {
			slog.Error("replay: could not record the failed replay", slog.String("event_id", rec.EventID),
				slog.String("consumer", rec.Consumer), slog.Any("error", perr))
		}
		return o
	}
	o.Applied = applied
	transitioned, err := r.store.MarkReplayed(ctx, rec.EventID, rec.Consumer, r.now())
	switch {
	case err != nil:
		// The effect is applied and idempotent; the record stays parked and a
		// later replay marks it without applying again.
		return failed(o, fmt.Errorf("applied but could not mark the record replayed: %w", err))
	case transitioned:
		o.Status = StatusReplayed
	default:
		// A concurrent replay won the transition: its call reports the replay.
		o.Status, o.Applied = StatusAlreadyReplayed, false
	}
	return o
}

func failed(o Outcome, err error) Outcome {
	o.Status, o.Error = StatusFailed, err.Error()
	return o
}

func (r *Replayer) handlerFor(consumer, eventType string) (relay.Handler, bool) {
	for _, h := range r.handlers[consumer] {
		if h.EventTypes[eventType] {
			return h, true
		}
	}
	return relay.Handler{}, false
}

// apply runs h for evt the way the transport does (libs/relay runOne): in one
// transaction, skip if the consumer's processed_events marker exists, else run
// the handler and write the marker. A transaction-scoped advisory lock on
// (consumer, event) serialises concurrent replays, so the second one waits and
// then sees the marker instead of running the handler or failing on the
// marker's primary key. It reports whether the handler ran.
func (r *Replayer) apply(ctx context.Context, h relay.Handler, evt relay.DecodedEvent) (applied bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("replay: handler panicked", slog.String("consumer", h.ConsumerName),
				slog.String("event_id", evt.EventID.String()), slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
			applied, err = false, fmt.Errorf("handler panic: %v", p)
		}
	}()
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.timeout)
	defer cancel()
	err = r.db.WithContext(wctx).Transaction(func(tx *gorm.DB) error {
		applied = false
		if err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtextextended(?, 0))`,
			"deadletter-replay:"+h.ConsumerName+":"+evt.EventID.String()).Error; err != nil {
			return err
		}
		var already int64
		if err := tx.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = ? AND event_id = ?`,
			h.ConsumerName, evt.EventID).Scan(&already).Error; err != nil {
			return err
		}
		if already > 0 {
			return nil
		}
		if err := h.Handle(wctx, tx, evt); err != nil {
			return err
		}
		if err := tx.Exec(`INSERT INTO processed_events (consumer_name, event_id) VALUES (?, ?)`,
			h.ConsumerName, evt.EventID).Error; err != nil {
			return err
		}
		applied = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// decode rebuilds the event the consumer saw from its failed_events record.
func decode(rec *mongostore.FailedEvent) (relay.DecodedEvent, error) {
	eid, err := uuid.Parse(rec.EventID)
	if err != nil {
		return relay.DecodedEvent{}, fmt.Errorf("record has a malformed event id: %w", err)
	}
	evt := relay.DecodedEvent{
		OutboxID:      rec.OutboxID,
		EventID:       eid,
		AggregateType: rec.AggregateType,
		EventType:     rec.EventType,
		SchemaVersion: int(rec.SchemaVersion),
		Payload:       rec.Payload,
		OccurredAt:    rec.OccurredAt,
		TraceID:       rec.TraceID,
		SpanID:        rec.SpanID,
	}
	if aid, err := uuid.Parse(rec.AggregateID); err == nil {
		evt.AggregateID = aid
	}
	if rec.ActorID != "" {
		if a, err := uuid.Parse(rec.ActorID); err == nil {
			evt.ActorID = &a
		}
	}
	return evt, nil
}
