package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

var errMongoNotReady = errors.New("mongodb is not connected")

// maxParkedErrorLen caps one stored error message, like the relay's last_error.
const maxParkedErrorLen = 2000

// parkTimeout bounds one Park call.
const parkTimeout = 5 * time.Second

// mongoSink is the dead-letter sink over MongoDB failed_events. It is built
// only when MONGO_URI is configured (without a store the consumers log the event
// and drop it, the documented gap). MongoDB is off every synchronous path
// (ADR-0004): it connects in the background (run), and until it is up Park
// returns an error, which tells the consumer the event was NOT parked.
type mongoSink struct {
	cfg     config.MongoConfig
	metrics *telemetry.Metrics
	store   atomic.Pointer[mongostore.Store]
}

var _ relay.DeadLetterSink = (*mongoSink)(nil)

func newMongoSink(cfg config.MongoConfig, m *telemetry.Metrics) *mongoSink {
	return &mongoSink{cfg: cfg, metrics: m}
}

func (m *mongoSink) get() (*mongostore.Store, error) {
	if s := m.store.Load(); s != nil {
		return s, nil
	}
	return nil, errMongoNotReady
}

// Ping is the readiness check (non-fatal).
func (m *mongoSink) Ping(ctx context.Context) error {
	s, err := m.get()
	if err != nil {
		return err
	}
	return s.Ping(ctx)
}

// run connects, retrying with capped backoff while MongoDB is down, then holds
// the connection until ctx ends and closes it.
func (m *mongoSink) run(ctx context.Context, logger *slog.Logger) error {
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
		logger.Error("mongodb unavailable: dead-letter parking is degraded (events that exhaust their retries are not parked)",
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

// Park records that d.Consumer gave up on d.Event, idempotently per
// (event_id, consumer). A non-nil error means the event was NOT parked. The call
// is detached from ctx's cancellation (a process shutting down should still
// record what it was parking) and bounded by parkTimeout.
func (m *mongoSink) Park(ctx context.Context, d relay.DeadLetter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("dead letter: panic parking event")
		}
	}()
	if d.Consumer == "" {
		return errors.New("dead letter: park: consumer is required")
	}
	s, err := m.get()
	if err != nil {
		return err
	}
	e := d.Event
	in := mongostore.ParkInput{
		EventID: e.EventID.String(), Consumer: d.Consumer, EventType: e.EventType,
		AggregateType: e.AggregateType, AggregateID: e.AggregateID.String(),
		Payload: []byte(e.Payload), OutboxID: e.OutboxID, SchemaVersion: int32(e.SchemaVersion),
		OccurredAt: e.OccurredAt.UTC(), TraceID: e.TraceID, SpanID: e.SpanID,
		Attempts: d.Attempts, Err: errMessage(d.Err),
	}
	if src, ok := relay.SourceFrom(ctx); ok {
		in.Topic, in.SchemaID = src.Topic, int32(src.SchemaID) //nolint:gosec // registry ids are small
	}
	for _, h := range d.History {
		in.History = append(in.History, errMessage(h))
	}
	if e.ActorID != nil {
		in.ActorID = e.ActorID.String()
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), parkTimeout)
	defer cancel()
	if _, err := s.ParkFailedEvent(pctx, in); err != nil {
		return err
	}
	if m.metrics != nil {
		m.metrics.DeadLettered(in.Consumer) // only an event really parked is a dead letter
	}
	return nil
}

func errMessage(err error) string {
	if err == nil {
		return "unknown error"
	}
	msg := strings.ToValidUTF8(err.Error(), "")
	if len(msg) > maxParkedErrorLen {
		msg = strings.ToValidUTF8(msg[:maxParkedErrorLen], "")
	}
	return msg
}
