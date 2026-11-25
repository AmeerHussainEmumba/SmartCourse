package infra

import (
	"context"
	"log/slog"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/cache"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/db"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra/observe"
)

// Observability and ObserveOptions are the database-free runtime in
// libs/infra/observe, re-exported so a binary that already imports infra keeps
// one import. libs/runtime imports observe directly, never this package.
type (
	Observability  = observe.Observability
	ObserveOptions = observe.Options
)

// Observe builds the tracing and metrics runtime and instruments d.
//
//   - Tracing (OTEL_EXPORTER_OTLP_ENDPOINT set and OTEL_SDK_DISABLED not true):
//     installs the tracer provider and, only then, the GORM and go-redis
//     hooks. Without it neither hook is registered, so disabled means zero
//     overhead on the query path.
//   - Metrics (METRICS_ENABLED, default true): the registry of observe.New plus
//     db_connections_in_use/max and, with ObserveOptions.Outbox, the outbox
//     gauges, both from d's Postgres.
//
// A nil or empty Dependencies is allowed: no hook and no db_* or outbox series.
func (d *Dependencies) Observe(ctx context.Context, cfg *config.Config, o ObserveOptions) *Observability {
	if d != nil && d.DB != nil {
		if sqlDB, err := d.DB.DB(); err != nil {
			slog.Warn("database metrics not enabled", slog.String("error", err.Error()))
		} else {
			o.SQLDB = sqlDB
		}
	}
	o.OnTracing = func() {
		if d != nil && d.DB != nil {
			if err := db.EnableTracing(d.DB); err != nil {
				slog.Warn("database tracing not enabled", slog.String("error", err.Error()))
			}
		}
		if d != nil && d.RDB != nil {
			cache.EnableTracing(d.RDB)
		}
	}
	return observe.New(ctx, cfg, o)
}
