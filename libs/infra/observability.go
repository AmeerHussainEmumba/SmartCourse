package infra

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/cache"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/db"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

// outboxPollInterval is how often the outbox gauges query Postgres. The gauges
// are refreshed by this poller, never by a scrape, so the database sees one
// small query per interval however often (or however many times) Prometheus
// scrapes.
const outboxPollInterval = 15 * time.Second

// ObserveOptions tells Observe what this binary exposes.
type ObserveOptions struct {
	// Service is the default service.name ("course-api"); OTEL_SERVICE_NAME
	// overrides it.
	Service string
	// Outbox adds outbox_backlog and outbox_oldest_unpublished_seconds, a
	// query over platform.outbox. Set it only for binaries whose
	// POSTGRES_SEARCH_PATH includes platform (identity-api, course-api,
	// worker); analytics-api has no access to the table.
	Outbox bool
	// Workflows keeps workflow_started/completed/failed_total, which only the
	// binary running the Temporal worker feeds (the worker). Without it the
	// three counters are left out of /metrics instead of reading a permanent
	// zero that looks like "no workflows ran".
	Workflows bool
	// OutboxPollInterval overrides how often the outbox gauges query Postgres
	// (default 15s). Tests shorten it; production keeps the default so the
	// database cost does not depend on the scrape rate.
	OutboxPollInterval time.Duration
	// SpanExporter replaces the OTLP/HTTP exporter built from
	// OTEL_EXPORTER_OTLP_ENDPOINT. It exists for tests (an in-memory
	// exporter); tracing is still gated by the config (an endpoint, or any
	// non-empty one, must be set and OTEL_SDK_DISABLED must not be true).
	SpanExporter sdktrace.SpanExporter
}

// Observability is a binary's tracing and metrics runtime (ADR-0032). The zero
// value, and the value Observe returns when everything is disabled, is inert:
// no middleware, no routes, nothing to shut down.
//
// Telemetry never stops a service: Observe has no error return, a tracing
// set-up failure is logged and the service runs untraced, and an unreachable
// collector only makes the exporter drop spans (logged at WARN).
type Observability struct {
	// Metrics is nil when METRICS_ENABLED=false. Other packages feed their
	// own counters through it (workflows, dead letters, Asynq gauges).
	Metrics *telemetry.Metrics

	reg      *prometheus.Registry
	provider *telemetry.Provider
	tracing  bool
	stopPoll func()
}

// Observe builds the runtime from cfg.Telemetry and instruments d.
//
//   - Tracing (OTEL_EXPORTER_OTLP_ENDPOINT set and OTEL_SDK_DISABLED not true):
//     installs the tracer provider and, only then, the GORM and go-redis
//     hooks. Without it neither hook is registered, so disabled means zero
//     overhead on the query path.
//   - Metrics (METRICS_ENABLED, default true): a registry with the Go runtime
//     and process collectors, the RED metrics, db_connections_in_use/max and,
//     with ObserveOptions.Outbox, the outbox gauges.
//
// ctx bounds the background poller; call Shutdown before exit to flush spans.
func (d *Dependencies) Observe(ctx context.Context, cfg *config.Config, o ObserveOptions) *Observability {
	obs := &Observability{}
	t := cfg.Telemetry

	if t.TracingEnabled() {
		name := t.ServiceName
		if name == "" {
			name = o.Service
		}
		tc := telemetry.Config{
			Enabled:     true,
			ServiceName: name,
			Environment: cfg.Env,
			Endpoint:    t.OTLPEndpoint,
			Insecure:    true, // applies to host:port only; an https:// URL carries its own scheme
			SampleRatio: t.SampleRatio,
		}
		var p *telemetry.Provider
		var err error
		if o.SpanExporter != nil {
			p, err = telemetry.SetupWithExporter(tc, o.SpanExporter)
		} else {
			p, err = telemetry.Setup(ctx, tc)
		}
		if err != nil {
			slog.Warn("tracing disabled: set-up failed", slog.String("error", err.Error()))
		} else {
			obs.provider, obs.tracing = p, true
			if err := db.EnableTracing(d.DB); err != nil {
				slog.Warn("database tracing not enabled", slog.String("error", err.Error()))
			}
			cache.EnableTracing(d.RDB)
			slog.Info("tracing enabled", slog.String("service", name), slog.Float64("sample_ratio", t.SampleRatio))
		}
	}

	if t.MetricsEnabled {
		if err := obs.initMetrics(ctx, d, o); err != nil {
			slog.Warn("metrics disabled: set-up failed", slog.String("error", err.Error()))
			obs.Metrics, obs.reg = nil, nil
		}
	}
	return obs
}

func (o *Observability) initMetrics(ctx context.Context, d *Dependencies, opts ObserveOptions) error {
	reg := telemetry.NewRegistry()
	m, err := telemetry.NewMetrics(reg)
	if err != nil {
		return err
	}
	sqlDB, err := d.DB.DB()
	if err != nil {
		return fmt.Errorf("get sql.DB: %w", err)
	}
	if err := telemetry.RegisterDBStats(reg, sqlDB); err != nil {
		return err
	}
	o.reg, o.Metrics = reg, m
	// A metric this binary never feeds must be absent, not a constant 0: an
	// analytics-api outbox_backlog of 0 would read as "nothing is waiting".
	// The collectors stay usable (they just are not exported).
	if !opts.Outbox {
		reg.Unregister(m.OutboxBacklog)
		reg.Unregister(m.OutboxOldestUnpublish)
	}
	if !opts.Workflows {
		reg.Unregister(m.WorkflowStartedTotal)
		reg.Unregister(m.WorkflowCompletedTotal)
		reg.Unregister(m.WorkflowFailedTotal)
	}
	if opts.Outbox {
		interval := opts.OutboxPollInterval
		if interval <= 0 {
			interval = outboxPollInterval
		}
		o.stopPoll = telemetry.StartPoller(ctx, interval,
			telemetry.OutboxGauges(m, func(ctx context.Context) (telemetry.OutboxStats, error) {
				return QueryOutboxStats(ctx, sqlDB)
			}))
	}
	return nil
}

// QueryOutboxStats counts the unpublished outbox rows and the age of the
// oldest. It is one aggregate over the partial index of unpublished rows
// (idx_outbox_unpublished); an empty outbox gives zeros.
func QueryOutboxStats(ctx context.Context, sqlDB *sql.DB) (telemetry.OutboxStats, error) {
	var backlog int64
	var oldest sql.NullFloat64
	err := sqlDB.QueryRowContext(ctx,
		`SELECT count(*), EXTRACT(EPOCH FROM now() - min(occurred_at)) FROM outbox WHERE published_at IS NULL`,
	).Scan(&backlog, &oldest)
	if err != nil {
		return telemetry.OutboxStats{}, fmt.Errorf("query outbox backlog: %w", err)
	}
	s := telemetry.OutboxStats{Backlog: backlog}
	if oldest.Valid && oldest.Float64 > 0 {
		s.OldestUnpublished = time.Duration(oldest.Float64 * float64(time.Second))
	}
	return s, nil
}

// TracingEnabled reports whether spans are being recorded and exported.
func (o *Observability) TracingEnabled() bool { return o != nil && o.tracing }

// Middleware returns the Gin middleware to pass as router.Options.Observe:
// request tracing (when enabled) then RED metrics (when enabled). It is empty
// when both are off.
func (o *Observability) Middleware() []gin.HandlerFunc {
	if o == nil {
		return nil
	}
	var mw []gin.HandlerFunc
	if o.tracing {
		mw = append(mw, telemetry.HTTPTracing())
	}
	if o.Metrics != nil {
		mw = append(mw, telemetry.HTTPMetrics(o.Metrics))
	}
	return mw
}

// RegisterMetricsRoute serves GET /metrics on r when metrics are enabled.
//
// The route is unauthenticated by design: Prometheus scrapes it without a
// credential (RFC §12.3). It exposes route templates, counts and process
// statistics, no request data, but it should not face the internet: compose
// publishes the port on 127.0.0.1 only, and a deployment must keep it on the
// internal network or filter /metrics at the ingress.
func (o *Observability) RegisterMetricsRoute(r *gin.Engine) {
	if o == nil || o.reg == nil {
		return
	}
	r.GET("/metrics", telemetry.GinHandler(o.reg))
}

// MetricsHandler is the /metrics handler for a binary that serves it on its
// own listener (the worker), or nil when metrics are disabled.
func (o *Observability) MetricsHandler() http.Handler {
	if o == nil || o.reg == nil {
		return nil
	}
	return telemetry.Handler(o.reg)
}

// ForceFlush exports buffered spans now (tests, and a deliberate flush before
// a long wait). It is a no-op when tracing is off.
func (o *Observability) ForceFlush(ctx context.Context) error {
	if o == nil {
		return nil
	}
	return o.provider.ForceFlush(ctx)
}

// Gatherer is the metrics registry, or nil when metrics are disabled. Tests
// pass it to telemetry.CheckLabels.
func (o *Observability) Gatherer() prometheus.Gatherer {
	if o == nil || o.reg == nil {
		return nil
	}
	return o.reg
}

// Shutdown stops the outbox poller and flushes buffered spans. ctx should
// carry a deadline; an unreachable collector therefore costs at most that
// long at exit. Safe on a nil or inert value and idempotent.
func (o *Observability) Shutdown(ctx context.Context) {
	if o == nil {
		return
	}
	if o.stopPoll != nil {
		o.stopPoll()
		o.stopPoll = nil
	}
	if err := o.provider.Shutdown(ctx); err != nil {
		slog.Warn("flushing spans on shutdown failed", slog.String("error", err.Error()))
	}
}
