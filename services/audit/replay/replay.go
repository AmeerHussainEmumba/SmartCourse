// Package replay replays a parked event by republishing it
// (docs/plans/microservices-split.md §4.1; replaces the worker's Replayer,
// which re-invoked the consumers' handlers and so had to link every service).
//
// A replay loads the parked record, encodes it with eventbus.EncodeForRetry
// (the stored JSON payload, event type, aggregate id, schema id and version,
// outbox id and trace fields) and produces it to `retry.<consumer>`. The
// consuming service's own consumer group handles that topic through its normal
// idempotent path (handler plus processed_events in one transaction), so this
// package knows no handler and imports no other service.
//
// # Ordering: produce, then mark
//
// The record is marked replayed only after the broker acknowledged the retry
// record. The two writes are in different systems, so one of them can fail
// between them; the order decides which failure is harmless:
//
//   - produce fails: nothing was sent, the record stays parked, the caller
//     gets a 503 and may try again. Nothing is lost.
//   - the process dies, or Mongo fails, after the produce and before the mark:
//     the record stays parked although the event was sent. A second replay
//     produces it again and the consumer's processed_events marker absorbs the
//     duplicate. Nothing is lost and nothing is applied twice.
//   - the reverse order (mark, then produce) would turn the same crash into a
//     record that says replayed for an event that was never sent: a silent
//     loss. That is why the order is not negotiable.
//
// Guarantees under N concurrent replays of the same record: MarkReplayed
// transitions it exactly once (one conditional update), so exactly one caller
// reports "replayed" and the others "already_replayed"; the number of retry
// records produced is at least 1 and at most N (each racing caller may produce
// before it learns it lost the mark); the consumer applies the event once
// whatever the count. A call that starts after the record is marked produces
// nothing.
//
// A record that cannot be decoded or encoded for a retry is never produced:
// it is refused with a clear reason and stays parked (finding B-01).
package replay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

var (
	// ErrNotFound means no failed event has that event id.
	ErrNotFound = errors.New("replay: failed event not found")
	// ErrInvalidID means the event id is not a UUID.
	ErrInvalidID = errors.New("replay: event id is not a UUID")
	// ErrNoStore means the Replayer has no store or no producer.
	ErrNoStore = errors.New("replay: no store or producer configured")
	// ErrUnavailable means MongoDB or Kafka failed during the replay. The
	// Result says how far it got; the cause is for the log only.
	ErrUnavailable = errors.New("replay: store or broker unavailable")
)

// Outcome statuses.
const (
	// StatusReplayed: this call produced the retry record and moved the record
	// parked -> replayed.
	StatusReplayed = "replayed"
	// StatusAlreadyReplayed: the record was already replayed (or a concurrent
	// replay won the mark); this call changed nothing.
	StatusAlreadyReplayed = "already_replayed"
	// StatusRefused: the record cannot be decoded or encoded for a retry. It
	// stays parked and nothing was produced.
	StatusRefused = "refused"
	// StatusProduceFailed: the broker did not acknowledge the retry record.
	// The record stays parked.
	StatusProduceFailed = "produce_failed"
	// StatusProducedNotMarked: the retry record was acknowledged but the record
	// could not be marked replayed. It stays parked; replaying again is safe
	// (processed_events absorbs the duplicate).
	StatusProducedNotMarked = "produced_not_marked"
)

// Outcome is the result for one (event, consumer) record.
type Outcome struct {
	Consumer string
	Status   string
	// Topic is the retry topic the record was produced to (empty when nothing
	// was produced).
	Topic string
	// Error is a short, fixed-vocabulary reason for a refusal; never a driver
	// message.
	Error string
}

// Result is what Replay did for an event id: one Outcome per record it
// reached, in order.
type Result struct {
	EventID  string
	Outcomes []Outcome
}

// Replayed reports whether any record was moved to replayed by this call.
func (r *Result) Replayed() bool { return r.has(StatusReplayed) }

// Refused reports whether any record was refused.
func (r *Result) Refused() bool { return r.has(StatusRefused) }

func (r *Result) has(status string) bool {
	for _, o := range r.Outcomes {
		if o.Status == status {
			return true
		}
	}
	return false
}

// Store is the part of *mongostore.Store the Replayer uses.
type Store interface {
	ListFailedEvents(ctx context.Context, f mongostore.FailedFilter, cursor string, limit int) (*mongostore.FailedPage, error)
	GetFailedEvent(ctx context.Context, eventID, consumer string) (*mongostore.FailedEvent, error)
	MarkReplayed(ctx context.Context, eventID, consumer string, at time.Time) (bool, error)
}

// Producer is *eventbus.Publisher's replay half.
type Producer interface {
	PublishRetry(ctx context.Context, consumer string, schemaID uint32, evt relay.DecodedEvent) error
}

// DefaultProduceTimeout bounds one produce; keep it below the HTTP write timeout.
const DefaultProduceTimeout = 20 * time.Second

// markTimeout bounds the mark after a successful produce. It runs detached
// from the request: a client that hangs up after the broker acked must not
// leave the record unmarked.
const markTimeout = 5 * time.Second

// Replayer republishes parked events.
type Replayer struct {
	store   Store
	prod    Producer
	timeout time.Duration
	now     func() time.Time
	log     *slog.Logger
	known   map[string]bool
}

// Option configures New.
type Option func(*Replayer)

// WithProduceTimeout bounds each produce (default DefaultProduceTimeout).
func WithProduceTimeout(d time.Duration) Option {
	return func(r *Replayer) {
		if d > 0 {
			r.timeout = d
		}
	}
}

// WithConsumers replaces the set of consumers a record may be replayed for
// (default topics.Consumers()). A record parked for any other name is refused:
// nobody reads its retry topic, so producing there and marking the record
// replayed would lose the event silently. A new consumer must be added to
// libs/kafka/topics, which also provisions its retry topic.
func WithConsumers(names ...string) Option {
	return func(r *Replayer) {
		r.known = map[string]bool{}
		for _, n := range names {
			r.known[n] = true
		}
	}
}

// WithLogger sets the logger (default slog.Default).
func WithLogger(l *slog.Logger) Option { return func(r *Replayer) { r.log = l } }

// New builds a Replayer.
func New(store Store, prod Producer, opts ...Option) *Replayer {
	r := &Replayer{store: store, prod: prod, timeout: DefaultProduceTimeout, now: func() time.Time { return time.Now().UTC() }, log: slog.Default()}
	WithConsumers(topics.Consumers()...)(r)
	for _, o := range opts {
		o(r)
	}
	return r
}

// Replay republishes every parked record of eventID (one per consumer that
// gave up on it). It returns ErrInvalidID for a malformed id and ErrNotFound
// when nothing was ever parked under it. When MongoDB or Kafka fails it
// returns the Result so far together with an error wrapping ErrUnavailable and
// stops: the remaining records are untouched. A refused record does not stop
// the others.
func (r *Replayer) Replay(ctx context.Context, eventID string) (*Result, error) {
	if r == nil || r.store == nil || r.prod == nil {
		return nil, ErrNoStore
	}
	id, err := uuid.Parse(eventID)
	if err != nil {
		return nil, ErrInvalidID
	}
	eventID = id.String() // canonical form, as it was parked
	recs, err := r.records(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(recs) == 0 {
		return nil, ErrNotFound
	}
	res := &Result{EventID: eventID}
	for _, listed := range recs {
		o, err := r.replayOne(ctx, listed)
		res.Outcomes = append(res.Outcomes, o)
		if err != nil {
			return res, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
	}
	return res, nil
}

func (r *Replayer) records(ctx context.Context, eventID string) ([]mongostore.FailedEvent, error) {
	var out []mongostore.FailedEvent
	cursor := ""
	for {
		page, err := r.store.ListFailedEvents(ctx, mongostore.FailedFilter{EventID: eventID}, cursor, mongostore.MaxPageSize)
		if err != nil {
			return nil, fmt.Errorf("list failed events: %w", err)
		}
		out = append(out, page.Items...)
		if page.NextCursor == "" {
			return out, nil
		}
		cursor = page.NextCursor
	}
}

// replayOne handles one record. A non-nil error is an infrastructure failure
// (the Outcome says which step); a refusal is an Outcome with a nil error.
func (r *Replayer) replayOne(ctx context.Context, listed mongostore.FailedEvent) (Outcome, error) {
	o := Outcome{Consumer: listed.Consumer}
	if listed.Status == mongostore.StatusReplayed {
		o.Status = StatusAlreadyReplayed
		return o, nil
	}
	rec, err := r.store.GetFailedEvent(ctx, listed.EventID, listed.Consumer)
	if errors.Is(err, mongostore.ErrNotFound) { // removed between the list and the get
		o.Status = StatusAlreadyReplayed
		return o, nil
	}
	if err != nil {
		o.Status = StatusProduceFailed
		return o, fmt.Errorf("load record: %w", err)
	}
	if rec.Status == mongostore.StatusReplayed { // replayed between the list and the get
		o.Status = StatusAlreadyReplayed
		return o, nil
	}

	if !r.known[rec.Consumer] {
		return r.refuse(o, rec, "the record's consumer is not a known consumer, so nothing reads its retry topic", nil), nil
	}
	evt, reason := decode(rec)
	if reason != "" {
		return r.refuse(o, rec, reason, nil), nil
	}
	schemaID := uint32(rec.SchemaID)
	// Dry-run the encoding: refuse here, with nothing sent, whatever
	// PublishRetry would reject later.
	if _, err := eventbus.EncodeForRetry(rec.Consumer, schemaID, evt); err != nil {
		return r.refuse(o, rec, "the stored record cannot be encoded for a retry", err), nil
	}

	pctx, cancel := context.WithTimeout(ctx, r.timeout)
	err = r.prod.PublishRetry(pctx, rec.Consumer, schemaID, evt)
	cancel()
	switch {
	case errors.Is(err, eventbus.ErrUnencodable):
		return r.refuse(o, rec, "the stored record cannot be encoded for a retry", err), nil
	case err != nil:
		o.Status = StatusProduceFailed
		return o, fmt.Errorf("produce to retry topic: %w", err)
	}
	o.Topic = topics.Retry(rec.Consumer)

	// Only now, with the broker's ack in hand, is the record marked replayed.
	mctx, mcancel := context.WithTimeout(context.WithoutCancel(ctx), markTimeout)
	defer mcancel()
	transitioned, err := r.store.MarkReplayed(mctx, rec.EventID, rec.Consumer, r.now())
	switch {
	case err != nil:
		o.Status = StatusProducedNotMarked
		return o, fmt.Errorf("mark replayed: %w", err)
	case transitioned:
		o.Status = StatusReplayed
	default:
		// A concurrent replay won the mark: its call reports the replay.
		o.Status = StatusAlreadyReplayed
	}
	return o, nil
}

func (r *Replayer) refuse(o Outcome, rec *mongostore.FailedEvent, reason string, cause error) Outcome {
	o.Status, o.Error = StatusRefused, reason
	r.log.Warn("replay refused: the parked record stays parked",
		slog.String("event_id", rec.EventID), slog.String("consumer", rec.Consumer), slog.String("reason", reason), slog.Any("error", cause))
	return o
}

// decode rebuilds the event the consumer saw from its failed_events record,
// or says why it cannot (a fixed reason, safe to show).
func decode(rec *mongostore.FailedEvent) (relay.DecodedEvent, string) {
	eid, err := uuid.Parse(rec.EventID)
	if err != nil {
		return relay.DecodedEvent{}, "the stored record has a malformed event id"
	}
	if _, ok := events.Lookup(rec.EventType); !ok {
		return relay.DecodedEvent{}, "the stored record has an unknown or missing event type (an undecodable record is never replayed)"
	}
	if rec.SchemaID <= 0 || rec.SchemaVersion <= 0 {
		return relay.DecodedEvent{}, "the stored record has no schema id or version, so it cannot be framed"
	}
	if len(rec.Payload) == 0 {
		return relay.DecodedEvent{}, "the stored record has no payload"
	}
	aid, err := uuid.Parse(rec.AggregateID)
	if err != nil {
		return relay.DecodedEvent{}, "the stored record has a malformed aggregate id"
	}
	evt := relay.DecodedEvent{
		OutboxID:      rec.OutboxID,
		EventID:       eid,
		AggregateType: rec.AggregateType,
		AggregateID:   aid,
		EventType:     rec.EventType,
		SchemaVersion: int(rec.SchemaVersion),
		Payload:       rec.Payload,
		OccurredAt:    rec.OccurredAt,
		TraceID:       rec.TraceID,
		SpanID:        rec.SpanID,
	}
	if rec.ActorID != "" {
		if a, err := uuid.Parse(rec.ActorID); err == nil {
			evt.ActorID = &a
		}
	}
	return evt, ""
}
