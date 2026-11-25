package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/admin"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/archive"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/deadletter"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/notification"
)

// consumerHandlers is the one list of every consumer the worker runs. buildRelay
// registers it with the relay, and the dead-letter Replayer re-runs parked
// events through the same handlers, so the two cannot drift apart. In Kafka
// mode the audit-archiver is added to both (see mongoWiring.ArchiveHandlers).
func consumerHandlers() []relay.Handler {
	var all []relay.Handler
	all = append(all, analytics.Handlers()...)
	all = append(all, course.Handlers()...)
	all = append(all, enrollment.Handlers()...)
	all = append(all, certificate.Handlers()...)
	all = append(all, notification.Handlers()...)
	return all
}

var errMongoNotReady = errors.New("mongodb is not connected")

// mongoWiring is the worker's handle on MongoDB (archive, audit log,
// failed_events). MongoDB is off every synchronous path (ADR-0004), so it
// connects in the background and the worker never waits for it: until it is
// up (or when MONGO_URI is unset) every call returns an error, which the
// archiver reports to its transport, the dead-letter sink reports to its
// caller and the admin routes turn into 503. The relay and analytics never
// touch it. It implements archive.Store, deadletter.ReplayStore and
// admin.FailedStore and admin.Auditor.
type mongoWiring struct {
	cfg   config.MongoConfig
	store atomic.Pointer[mongostore.Store]
}

func newMongoWiring(cfg *config.Config) *mongoWiring { return &mongoWiring{cfg: cfg.Mongo} }

func (m *mongoWiring) get() (*mongostore.Store, error) {
	if s := m.store.Load(); s != nil {
		return s, nil
	}
	return nil, errMongoNotReady
}

// run connects to MongoDB, retrying with capped backoff while it is down, then
// holds the connection until ctx is cancelled and closes it. It logs loudly
// each failed attempt and returns at once when MONGO_URI is not set.
func (m *mongoWiring) run(ctx context.Context, logger *slog.Logger) {
	if !m.cfg.Configured() {
		logger.Warn("MONGO_URI is not set: the audit archive, dead-letter parking and failed-event replay are disabled; the relay and analytics are unaffected")
		return
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
			return
		}
		// err comes from mongostore and never contains the URI.
		logger.Error("mongodb unavailable: archive, dead-letter parking and failed-event replay are degraded; the relay and analytics are unaffected",
			slog.Any("error", err), slog.Duration("retry_in", backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// Ping is the optional readiness check for /health/ready.
func (m *mongoWiring) Ping(ctx context.Context) error {
	if !m.cfg.Configured() {
		return admin.ErrNotConfigured
	}
	s, err := m.get()
	if err != nil {
		return err
	}
	return s.Ping(ctx)
}

// ArchiveHandlers returns the audit-archiver's handlers over this store.
// Register them with the Kafka consumer runner only, never with the
// in-process relay: there a failing handler fails the whole row, so a MongoDB
// outage would stall analytics (README of services/worker/archive).
func (m *mongoWiring) ArchiveHandlers(opts ...archive.Option) []relay.Handler {
	return archive.Handlers(m, opts...)
}

// Sink is the dead-letter sink to hand to the transport. Every event it parks
// is counted in dead_letter_total{consumer} when metrics is non-nil (see
// countParked).
func (m *mongoWiring) Sink(metrics *telemetry.Metrics, opts ...deadletter.SinkOption) *deadletter.Sink {
	return deadletter.NewSink(countParked(m, metrics), opts...)
}

func (m *mongoWiring) ArchiveEvent(ctx context.Context, e mongostore.Event) (bool, error) {
	s, err := m.get()
	if err != nil {
		return false, err
	}
	return s.ArchiveEvent(ctx, e)
}

func (m *mongoWiring) AppendAuditOnce(ctx context.Context, a mongostore.AuditEntry) (bool, error) {
	s, err := m.get()
	if err != nil {
		return false, err
	}
	return s.AppendAuditOnce(ctx, a)
}

func (m *mongoWiring) AppendAudit(ctx context.Context, a mongostore.AuditEntry) (string, error) {
	s, err := m.get()
	if err != nil {
		return "", err
	}
	return s.AppendAudit(ctx, a)
}

func (m *mongoWiring) ParkFailedEvent(ctx context.Context, in mongostore.ParkInput) (*mongostore.FailedEvent, error) {
	s, err := m.get()
	if err != nil {
		return nil, err
	}
	return s.ParkFailedEvent(ctx, in)
}

func (m *mongoWiring) ListFailedEvents(ctx context.Context, f mongostore.FailedFilter, cursor string, limit int) (*mongostore.FailedPage, error) {
	s, err := m.get()
	if err != nil {
		return nil, err
	}
	return s.ListFailedEvents(ctx, f, cursor, limit)
}

func (m *mongoWiring) GetFailedEvent(ctx context.Context, eventID, consumer string) (*mongostore.FailedEvent, error) {
	s, err := m.get()
	if err != nil {
		return nil, err
	}
	return s.GetFailedEvent(ctx, eventID, consumer)
}

func (m *mongoWiring) MarkReplayed(ctx context.Context, eventID, consumer string, at time.Time) (bool, error) {
	s, err := m.get()
	if err != nil {
		return false, err
	}
	return s.MarkReplayed(ctx, eventID, consumer, at)
}

// newAdminHandler builds the admin HTTP engine: health plus the failed-events
// routes, replaying through handlers (the same list the transport runs).
func newAdminHandler(deps *infra.Dependencies, cfg *config.Config, mg *mongoWiring, handlers []relay.Handler) (http.Handler, error) {
	return admin.Handler(admin.Deps{
		Config:   cfg,
		RDB:      deps.RDB,
		Failed:   mg,
		Replayer: deadletter.NewReplayer(deps.DB, mg, handlers),
		Auditor:  mg,
		Required: deps.HealthCheckers(),
		Optional: map[string]router.Checker{"mongodb": mg.Ping},
	})
}

// serveAdmin runs the admin HTTP server until ctx is cancelled, then drains it.
// A failure to build or bind the server is logged and ends only this
// goroutine: the relay, Temporal worker and Asynq keep running, same
// independence as startAsynq.
func serveAdmin(ctx context.Context, logger *slog.Logger, deps *infra.Dependencies, cfg *config.Config, mg *mongoWiring, handlers []relay.Handler) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("admin goroutine panicked; relay and workers are unaffected", slog.Any("panic", r))
		}
	}()
	h, err := newAdminHandler(deps, cfg, mg, handlers)
	if err != nil {
		logger.Error("admin server not started", slog.Any("error", err))
		return
	}
	srv := admin.NewServer(cfg, h)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("worker admin listening", slog.String("addr", cfg.Admin.Addr))
	select {
	case err := <-errc:
		logger.Error("admin server stopped; failed-event replay and the worker's health endpoint are down, the relay is unaffected", slog.Any("error", err))
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			logger.Error("admin graceful shutdown failed", slog.Any("error", err))
		}
	}
}
