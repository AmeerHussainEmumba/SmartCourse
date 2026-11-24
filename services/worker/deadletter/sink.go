// Package deadletter parks events a consumer gave up on, and replays them
// (RFC §6.9, ADR-0031).
//
// Sink is the write side: the transport hands it an event whose retry budget
// is spent and it records payload, consumer, attempts and the error in
// MongoDB `failed_events`, idempotently per (event_id, consumer). It is built
// so that parking cannot stall the stream: every call is bounded by a timeout
// and returns an error, never blocks or panics, when MongoDB is unreachable.
// An error means "not parked": the caller must then keep the event retryable
// instead of dropping it.
//
// Replayer is the read side: it re-applies a parked event through the
// consumer's own handler in the normal idempotent path and marks the record
// replayed exactly once.
package deadletter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// DefaultParkTimeout bounds one Park call.
const DefaultParkTimeout = 5 * time.Second

// maxErrorLen caps one stored error message, like the relay's last_error.
const maxErrorLen = 2000

// ErrNoStore is returned when a Sink or Replayer has no MongoDB store.
var ErrNoStore = errors.New("deadletter: no MongoDB store configured")

// Parked is one give-up: the event a consumer could not process and why.
// It carries the same information as libs/relay's DeadLetter, so the
// transport's sink hook adapts to it field for field.
type Parked struct {
	Event relay.DecodedEvent
	// SchemaID is the Schema Registry id of the payload, 0 when unknown.
	SchemaID int32
	// Topic is the Kafka topic the event came from, empty in-process.
	Topic string
	// Consumer is the consumer name (the handler's ConsumerName).
	Consumer string
	// Err is why the last attempt failed. Each Park call adds one entry to
	// the record's error history (capped by mongostore).
	Err error
	// Attempts is how many delivery attempts this parking represents.
	Attempts int
}

// ParkStore is the part of *mongostore.Store the Sink uses.
type ParkStore interface {
	ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error)
}

// Sink writes parked events to failed_events.
type Sink struct {
	store   ParkStore
	timeout time.Duration
}

// SinkOption configures NewSink.
type SinkOption func(*Sink)

// WithParkTimeout bounds each Park call (default DefaultParkTimeout).
func WithParkTimeout(d time.Duration) SinkOption {
	return func(s *Sink) {
		if d > 0 {
			s.timeout = d
		}
	}
}

// NewSink builds a Sink over store.
func NewSink(store ParkStore, opts ...SinkOption) *Sink {
	s := &Sink{store: store, timeout: DefaultParkTimeout}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Park records that p.Consumer gave up on p.Event. It is idempotent per
// (event_id, consumer): parking again appends to the error history of the
// one document. It returns within the timeout; a non-nil error means the
// event was NOT parked (MongoDB down, payload over the size cap, bad input)
// and the caller must keep the event instead of dropping it.
//
// The call is detached from ctx's cancellation (a process shutting down
// should still record what it was parking) but bounded by the timeout.
func (s *Sink) Park(ctx context.Context, p Parked) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("deadletter: panic parking event: %v", r)
		}
	}()
	if s == nil || s.store == nil {
		return ErrNoStore
	}
	if p.Consumer == "" {
		return fmt.Errorf("deadletter: park: %w: consumer is required", mongostore.ErrInvalid)
	}
	e := p.Event
	in := mongostore.ParkInput{
		EventID:       e.EventID.String(),
		Consumer:      p.Consumer,
		EventType:     e.EventType,
		Topic:         p.Topic,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID.String(),
		Payload:       []byte(e.Payload),
		OutboxID:      e.OutboxID,
		SchemaID:      p.SchemaID,
		SchemaVersion: int32(e.SchemaVersion),
		OccurredAt:    e.OccurredAt.UTC(),
		TraceID:       e.TraceID,
		SpanID:        e.SpanID,
		Attempts:      p.Attempts,
		Err:           errMessage(p.Err),
	}
	if e.ActorID != nil {
		in.ActorID = e.ActorID.String()
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.timeout)
	defer cancel()
	if _, err := s.store.ParkFailedEvent(pctx, in); err != nil {
		return fmt.Errorf("deadletter: park event %s for %s: %w", in.EventID, p.Consumer, err)
	}
	return nil
}

func errMessage(err error) string {
	if err == nil {
		return "unknown error"
	}
	msg := strings.ToValidUTF8(err.Error(), "")
	if len(msg) > maxErrorLen {
		msg = strings.ToValidUTF8(msg[:maxErrorLen], "")
	}
	return msg
}
