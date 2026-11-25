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

// The dead-letter path (RFC §6.9, ADR-0031). When a Kafka consumer spends its
// retry budget, or the relay cannot publish a row, the event is parked in
// MongoDB `failed_events` before it is dropped; audit-api lists and replays
// those records (by republishing to retry.<consumer>, never by re-invoking a
// handler here). The course-worker only WRITES. With MONGO_URI unset there is
// no sink at all: the transport logs the event in full and drops it, the
// documented gap, rather than keeping a poison event unpublished forever.

const (
	// parkTimeout bounds one Park call, as the old worker's deadletter.Sink did.
	parkTimeout = 5 * time.Second
	// maxErrorLen caps one stored error message, like the relay's last_error.
	maxErrorLen = 2000
)

var errMongoNotReady = errors.New("mongodb is not connected")

// parker is the part of *mongostore.Store the sink uses.
type parker interface {
	ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error)
}

// mongoConn holds the background MongoDB connection. MongoDB is off every
// synchronous path (ADR-0004): it connects in the background and the worker
// never waits for it. Until it is up every call returns errMongoNotReady, which
// the sink reports as "not parked" so the transport keeps the event retryable.
type mongoConn struct {
	cfg   config.MongoConfig
	store atomic.Pointer[mongostore.Store]
}

func (m *mongoConn) get() (*mongostore.Store, error) {
	if s := m.store.Load(); s != nil {
		return s, nil
	}
	return nil, errMongoNotReady
}

// ParkFailedEvent implements parker over the live store.
func (m *mongoConn) ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	s, err := m.get()
	if err != nil {
		return nil, err
	}
	return s.ParkFailedEvent(ctx, in)
}

// Ping is the optional readiness probe.
func (m *mongoConn) Ping(ctx context.Context) error {
	s, err := m.get()
	if err != nil {
		return err
	}
	return s.Ping(ctx)
}

// run connects, retrying with capped backoff while MongoDB is down, then holds
// the connection until ctx is cancelled and closes it.
func (m *mongoConn) run(ctx context.Context, logger *slog.Logger) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		s, err := mongostore.Open(ctx, mongostore.Config{
			URI: m.cfg.URI, Database: m.cfg.Database, ConnectTimeout: m.cfg.ConnectTimeout,
			ServerSelectionTimeout: m.cfg.ServerSelectionTimeout, OpTimeout: m.cfg.OpTimeout,
		})
		if err == nil {
			m.store.Store(s)
			logger.Info("mongodb connected", slog.String("database", m.cfg.Database))
			<-ctx.Done()
			m.store.Store(nil)
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = s.Close(closeCtx)
			return nil
		}
		// err comes from mongostore and never contains the URI.
		logger.Error("mongodb unavailable: dead-letter parking is degraded; the relay and the consumers keep running",
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

// parkSink is the relay.DeadLetterSink over a parker. A non-nil error from Park
// means "not parked": the relay keeps the row unpublished and the consumers
// keep the record retryable rather than dropping the event.
type parkSink struct {
	store   parker
	metrics *telemetry.Metrics // counts dead_letter_total{consumer}; nil when metrics are off
	timeout time.Duration
}

func newParkSink(store parker, m *telemetry.Metrics) *parkSink {
	return &parkSink{store: store, metrics: m, timeout: parkTimeout}
}

// Park records that d.Consumer gave up on d.Event, idempotently per
// (event_id, consumer). It is detached from ctx's cancellation (a process shutting
// down should still record what it was parking) but bounded by the timeout, and
// it never panics.
func (s *parkSink) Park(ctx context.Context, d relay.DeadLetter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("deadletter: panic parking event: %v", r)
		}
	}()
	if s == nil || s.store == nil {
		return errMongoNotReady
	}
	if d.Consumer == "" {
		return errors.New("deadletter: park: consumer is required")
	}
	e := d.Event
	in := mongostore.ParkInput{
		EventID:       e.EventID.String(),
		Consumer:      d.Consumer,
		EventType:     e.EventType,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID.String(),
		Payload:       []byte(e.Payload),
		OutboxID:      e.OutboxID,
		SchemaVersion: int32(e.SchemaVersion),
		OccurredAt:    e.OccurredAt.UTC(),
		TraceID:       e.TraceID,
		SpanID:        e.SpanID,
		Attempts:      d.Attempts,
		Err:           errMessage(d.Err),
	}
	for _, h := range d.History {
		in.History = append(in.History, errMessage(h))
	}
	if e.ActorID != nil {
		in.ActorID = e.ActorID.String()
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.timeout)
	defer cancel()
	if _, err := s.store.ParkFailedEvent(pctx, in); err != nil {
		return fmt.Errorf("deadletter: park event %s for %s: %w", in.EventID, d.Consumer, err)
	}
	// Counted only for an event that was really parked.
	if s.metrics != nil {
		s.metrics.DeadLettered(d.Consumer)
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
