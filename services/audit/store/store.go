// Package store is the audit service's handle on MongoDB (archive `events`,
// `audit_log`, `failed_events`). MongoDB is the service's only database, but
// the processes must not die or hang because it is down (ADR-0004, ADR-0031):
// Wiring connects in the background with capped backoff, and until it is up
// every call returns ErrNotReady, which the archiver reports to its consumer
// (the record is retried, then parked), the dead-letter sink reports to its
// caller and the admin routes turn into 503. It implements archive.Store,
// deadletter.ParkStore, admin.FailedStore, admin.Auditor and replay.Store.
package store

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
)

// ErrNotReady is returned while MongoDB is not connected.
var ErrNotReady = errors.New("mongodb is not connected")

// ErrNotConfigured is returned when MONGO_URI is unset.
var ErrNotConfigured = errors.New("MONGO_URI is not set")

// Wiring is the lazily connected store.
type Wiring struct {
	cfg   config.MongoConfig
	store atomic.Pointer[mongostore.Store]
}

// New builds a Wiring; Run connects it.
func New(cfg config.MongoConfig) *Wiring { return &Wiring{cfg: cfg} }

// Use installs an already open store (tests).
func (m *Wiring) Use(s *mongostore.Store) { m.store.Store(s) }

func (m *Wiring) get() (*mongostore.Store, error) {
	if s := m.store.Load(); s != nil {
		return s, nil
	}
	return nil, ErrNotReady
}

// Run connects to MongoDB, retrying with capped backoff while it is down, then
// holds the connection until ctx is cancelled and closes it. It returns
// ErrNotConfigured when MONGO_URI is not set (nothing to connect to); every
// failed attempt is logged and the loop goes on.
func (m *Wiring) Run(ctx context.Context, logger *slog.Logger) error {
	if !m.cfg.Configured() {
		return ErrNotConfigured
	}
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
		logger.Error("mongodb unavailable: the archive, dead-letter parking and failed-event replay are degraded",
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

// Ping is the readiness check.
func (m *Wiring) Ping(ctx context.Context) error {
	s, err := m.get()
	if err != nil {
		return err
	}
	return s.Ping(ctx)
}

func (m *Wiring) ArchiveEvent(ctx context.Context, e mongostore.Event) (bool, error) {
	s, err := m.get()
	if err != nil {
		return false, err
	}
	return s.ArchiveEvent(ctx, e)
}

func (m *Wiring) AppendAuditOnce(ctx context.Context, a mongostore.AuditEntry) (bool, error) {
	s, err := m.get()
	if err != nil {
		return false, err
	}
	return s.AppendAuditOnce(ctx, a)
}

func (m *Wiring) AppendAudit(ctx context.Context, a mongostore.AuditEntry) (string, error) {
	s, err := m.get()
	if err != nil {
		return "", err
	}
	return s.AppendAudit(ctx, a)
}

func (m *Wiring) ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	s, err := m.get()
	if err != nil {
		return nil, err
	}
	return s.ParkFailedEvent(ctx, in)
}

func (m *Wiring) ListFailedEvents(ctx context.Context, f mongostore.FailedFilter, cursor string, limit int) (*mongostore.FailedPage, error) {
	s, err := m.get()
	if err != nil {
		return nil, err
	}
	return s.ListFailedEvents(ctx, f, cursor, limit)
}

func (m *Wiring) GetFailedEvent(ctx context.Context, eventID, consumer string) (*mongostore.FailedEvent, error) {
	s, err := m.get()
	if err != nil {
		return nil, err
	}
	return s.GetFailedEvent(ctx, eventID, consumer)
}

func (m *Wiring) MarkReplayed(ctx context.Context, eventID, consumer string, at time.Time) (bool, error) {
	s, err := m.get()
	if err != nil {
		return false, err
	}
	return s.MarkReplayed(ctx, eventID, consumer, at)
}
