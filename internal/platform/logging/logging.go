// Package logging configures SmartCourse's structured logger.
//
// Structured JSON via slog (RFC §12.4). Every line is expected to carry
// trace_id/request_id via context — see platform/httpx for how those are
// attached to a request-scoped logger. Logs are for detail after a trace has
// localised a problem; they are not the primary diagnostic tool (RFC §12.1).
package logging

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey struct{}

// New builds the process-wide base logger. level is one of
// "debug"|"info"|"warn"|"error"; unrecognised values fall back to "info".
func New(env string, level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	logger := slog.New(h).With(slog.String("env", env))
	slog.SetDefault(logger)
	return logger
}

// WithContext attaches a logger (already enriched with trace_id/request_id)
// to ctx, so downstream code can retrieve it without threading it through
// every function signature.
func WithContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext returns the request-scoped logger, or slog.Default() if none
// was attached — callers never need a nil check.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}
