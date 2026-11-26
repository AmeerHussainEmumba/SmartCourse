package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

const (
	// parkTimeout bounds one Park call.
	parkTimeout = 5 * time.Second
	// maxErrorLen caps one stored error message, like the relay's last_error.
	maxErrorLen = 2000
)

var errMongoNotReady = errors.New("mongodb is not connected")

// parkStore is the part of *mongostore.Store the sink uses.
type parkStore interface {
	ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error)
}

// deadLetters is the worker's relay.DeadLetterSink: an event a consumer gave up
// on is parked in MongoDB failed_events (RFC §6.9, ADR-0031) before the
// consumer's offset advances ("park before drop"). It is only built when
// MONGO_URI is configured; without it the consumers have no sink and log the
// event in full and drop it, as the old worker did.
//
// MongoDB connects in the background (run) and the worker never waits for it.
// While it is down Park returns an error, which means "not parked": the
// consumers retry the sink a few times and then log the event loudly. Idempotent
// per (event_id, consumer) because mongostore.ParkFailedEvent is.
type deadLetters struct {
	cfg     config.MongoConfig
	metrics *telemetry.Metrics
	store   atomic.Pointer[mongostore.Store]
	// override replaces the connected store (tests).
	override parkStore
}

var _ relay.DeadLetterSink = (*deadLetters)(nil)

func newDeadLetters(cfg config.MongoConfig, m *telemetry.Metrics) *deadLetters {
	return &deadLetters{cfg: cfg, metrics: m}
}

func (d *deadLetters) get() (parkStore, error) {
	if d.override != nil {
		return d.override, nil
	}
	if s := d.store.Load(); s != nil {
		return s, nil
	}
	return nil, errMongoNotReady
}

// run connects to MongoDB, retrying with capped backoff while it is down, then
// holds the connection until ctx is cancelled and closes it.
func (d *deadLetters) run(ctx context.Context, logger *slog.Logger) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		s, err := mongostore.Open(ctx, mongostore.Config{
			URI: d.cfg.URI, Database: d.cfg.Database, ConnectTimeout: d.cfg.ConnectTimeout,
			ServerSelectionTimeout: d.cfg.ServerSelectionTimeout, OpTimeout: d.cfg.OpTimeout,
		})
		if err == nil {
			d.store.Store(s)
			logger.Info("mongodb connected", slog.String("database", d.cfg.Database))
			<-ctx.Done()
			d.store.Store(nil)
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = s.Close(closeCtx)
			return nil
		}
		// err comes from mongostore and never contains the URI.
		logger.Error("mongodb unavailable: dead-letter parking is degraded (events that exhaust their retries are logged and dropped); the aggregates are unaffected",
			slog.Any("error", err), slog.Duration("retry_in", backoff))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// Ping is the non-fatal readiness check.
func (d *deadLetters) Ping(ctx context.Context) error {
	if d.override != nil {
		return nil
	}
	s := d.store.Load()
	if s == nil {
		return errMongoNotReady
	}
	return s.Ping(ctx)
}

// Park implements relay.DeadLetterSink. The call is detached from ctx's
// cancellation (a process shutting down should still record what it was
// parking) but bounded by parkTimeout. A non-nil error means NOT parked.
func (d *deadLetters) Park(ctx context.Context, dl relay.DeadLetter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("deadletter: panic parking event: %v", r)
		}
	}()
	if dl.Consumer == "" {
		return fmt.Errorf("deadletter: park: %w: consumer is required", mongostore.ErrInvalid)
	}
	store, err := d.get()
	if err != nil {
		return fmt.Errorf("deadletter: park event %s for %s: %w", dl.Event.EventID, dl.Consumer, err)
	}
	e := dl.Event
	in := mongostore.ParkInput{
		EventID:       e.EventID.String(),
		Consumer:      dl.Consumer,
		EventType:     e.EventType,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID.String(),
		Payload:       []byte(e.Payload),
		OutboxID:      e.OutboxID,
		SchemaVersion: int32(e.SchemaVersion),
		OccurredAt:    e.OccurredAt.UTC(),
		TraceID:       e.TraceID,
		SpanID:        e.SpanID,
		Attempts:      dl.Attempts,
		Err:           errMessage(dl.Err),
	}
	if src, ok := relay.SourceFrom(ctx); ok {
		in.Topic, in.SchemaID = src.Topic, int32(src.SchemaID)
	}
	for _, h := range dl.History {
		in.History = append(in.History, errMessage(h))
	}
	if e.ActorID != nil {
		in.ActorID = e.ActorID.String()
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), parkTimeout)
	defer cancel()
	if _, err := store.ParkFailedEvent(pctx, in); err != nil {
		return fmt.Errorf("deadletter: park event %s for %s: %w", in.EventID, dl.Consumer, err)
	}
	if d.metrics != nil {
		d.metrics.DeadLettered(dl.Consumer) // only an event really parked is a dead letter
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
