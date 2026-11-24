package logging

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// Log attribute keys of RFC §12.4. Dashboards and log queries rely on these
// exact names.
const (
	KeyTraceID   = "trace_id"
	KeySpanID    = "span_id"
	KeyRequestID = "request_id"
	KeyEventID   = "event_id"
	KeyJobID     = "job_id"
)

// Correlated returns the logger from ctx (see FromContext) enriched with the
// trace_id and span_id of the active span, so request-scoped code logs lines
// that join to a trace. With no valid span in ctx (tracing disabled, a
// background goroutine) it returns the plain logger: no empty trace fields.
func Correlated(ctx context.Context) *slog.Logger {
	l := FromContext(ctx)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return l.With(slog.String(KeyTraceID, sc.TraceID().String()), slog.String(KeySpanID, sc.SpanID().String()))
	}
	return l
}

// WithEventID returns l with event_id set. The relay and Kafka consumers use
// it for every line about one event (RFC §12.4). An empty id is ignored.
func WithEventID(l *slog.Logger, eventID string) *slog.Logger {
	if eventID == "" {
		return l
	}
	return l.With(slog.String(KeyEventID, eventID))
}

// WithJobID returns l with job_id set. Asynq task handlers use it for every
// line about one task (RFC §12.4). An empty id is ignored.
func WithJobID(l *slog.Logger, jobID string) *slog.Logger {
	if jobID == "" {
		return l
	}
	return l.With(slog.String(KeyJobID, jobID))
}
