package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"

	"gorm.io/gorm"
)

// RelayConsumer is the DeadLetter.Consumer of an event the relay itself gave up
// publishing (publish mode), as opposed to a named handler's consumer.
const RelayConsumer = "relay"

// DeadLetter is an event something gave up on: the in-process relay after its
// retry budget, or a Kafka consumer after its own.
type DeadLetter struct {
	Event DecodedEvent
	// Consumer is the handler's ConsumerName (the consumer group in Kafka
	// mode), or RelayConsumer when publishing itself failed.
	Consumer string
	// Err is the last error.
	Err error
	// History is the error of every attempt, in order, when the caller kept
	// them (the Kafka consumer does). Empty means only Err is known.
	History []error
	// Attempts is how many tries were made.
	Attempts int
}

// DeadLetterSink durably parks events that were given up on (RFC §6.9,
// ADR-0031). Park returns nil only once the event is stored: the caller drops
// the event only after that, and keeps it (the outbox row stays unpublished, the
// consumer's offset is still advanced but the failure is logged and counted)
// when Park fails. Park may be called again for the same event and consumer
// (a retry after a failed Park, a replay of a partially parked event), so it
// must be idempotent on (Event.EventID, Consumer).
//
// The gap this closes: with no sink the relay logs loudly, keeps the reason in
// outbox.last_error and drops the event. That default is the pre-Week-3
// behaviour, not a recommendation.
type DeadLetterSink interface {
	Park(ctx context.Context, d DeadLetter) error
}

// Publisher hands one outbox row to the event transport (ADR-0029). It returns
// nil only once the transport has durably accepted the event (for Kafka: after
// the broker's acknowledgement); the relay marks the row published only then.
//
// An error is retried against the row's attempt budget unless it is marked
// with Transient, which means the transport is unavailable rather than the
// event being bad: such a failure never consumes the budget, because an outage
// must not dead-letter good events.
type Publisher interface {
	Publish(ctx context.Context, evt DecodedEvent) error
}

type transientError struct{ err error }

func (e transientError) Error() string { return e.err.Error() }
func (e transientError) Unwrap() error { return e.err }

// Transient marks err as a transport outage: the row is retried later without
// counting an attempt. A nil err stays nil.
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return transientError{err}
}

// IsTransient reports whether err was marked with Transient.
func IsTransient(err error) bool {
	var t transientError
	return errors.As(err, &t)
}

// The two delivery modes an Observer is told about.
const (
	// ModePublish: the relay hands the row to the Publisher (kafka transport).
	ModePublish = "publish"
	// ModeProcess: the relay runs the registered handlers itself (in-process).
	ModeProcess = "process"
)

// Observer is the observability seam of the relay: it is called once per
// delivery attempt of an outbox row, after the row is claimed and before the
// Publisher or the handlers run, and returns the context they run under plus a
// finish function called with the attempt's outcome (nil on success). The
// worker implements it with libs/telemetry (a span continuing the trace stored
// on the row, which the Publisher then injects into the Kafka record); this
// package imports no tracing library, so identity-api and analytics-api, which
// link the relay, do not pay for it.
//
// The returned context must derive from ctx. An observer that returns a nil
// context or finish function is treated as having none; a panic in either is
// logged and ignored.
type Observer func(ctx context.Context, evt DecodedEvent, mode string) (context.Context, func(err error))

// HandlerHook is called after each handler the in-process relay runs for a row:
// duplicate is true when the consumer had already processed the event (the
// handler did not run), err is the handler's error. The worker counts
// event_processed_total from it. It is not called in publish mode, where the
// Kafka consumers run the handlers.
type HandlerHook func(consumer string, duplicate bool, err error)

// Handles reports whether h is registered for eventType.
func (h Handler) Handles(eventType string) bool { return h.EventTypes[eventType] }

// Deliver runs h for evt on db: RFC §6.6's idempotency rule, the same code path
// the in-process relay uses. In one transaction it skips an event the consumer
// already processed, otherwise runs the handler and writes the processed_events
// marker, so a replay cannot apply twice. A handler panic is recovered and
// returned as an error (the transaction rolls back). The Kafka consumers call
// this, which is what makes the same handlers run under both transports.
func Deliver(ctx context.Context, db *gorm.DB, h Handler, evt DecodedEvent) error {
	_, err := DeliverEvent(ctx, db, h, evt)
	return err
}

// DeliverEvent is Deliver that also reports whether the consumer had already
// processed the event (duplicate), in which case the handler did not run. The
// Kafka consumers count that as the "duplicate" outcome of event_processed_total.
func DeliverEvent(ctx context.Context, db *gorm.DB, h Handler, evt DecodedEvent) (duplicate bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			eventLog(ctx, evt).Error("relay handler panicked",
				slog.String("consumer", h.ConsumerName), slog.Int64("outbox_id", evt.OutboxID),
				slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
			duplicate, err = false, fmt.Errorf("handler panic: %v", p)
		}
	}()
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var already int64
		if err := tx.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = ? AND event_id = ?`,
			h.ConsumerName, evt.EventID).Scan(&already).Error; err != nil {
			return err
		}
		if already > 0 {
			duplicate = true
			return nil // this consumer has already handled this event — no-op
		}
		if err := h.Handle(ctx, tx, evt); err != nil {
			return err
		}
		return tx.Exec(`INSERT INTO processed_events (consumer_name, event_id) VALUES (?, ?)`,
			h.ConsumerName, evt.EventID).Error
	})
	if err != nil {
		duplicate = false
	}
	return duplicate, err
}

// consumerFailure is one consumer's (or the publisher's) failed attempt on a row.
type consumerFailure struct {
	consumer string
	err      error
}

// park hands d to the sink, recovering a panic into an error so a bad sink can
// neither crash a relay worker nor let an unparked event be dropped.
func park(ctx context.Context, s DeadLetterSink, d DeadLetter) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("dead-letter sink panicked: %v", p)
		}
	}()
	return s.Park(ctx, d)
}
