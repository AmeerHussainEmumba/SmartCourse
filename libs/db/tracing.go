package db

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

// tracerName is the instrumentation scope of the spans this file starts.
const tracerName = "github.com/AmeerHussainEmumba/SmartCourse/libs/db"

// maxStatementLen bounds the db.query.text attribute.
const maxStatementLen = 512

// spanKey is where a statement's span is kept between GORM's before and after
// callbacks (Statement.Settings is per statement and safe across goroutines).
const spanKey = "smartcourse:otel:span"

// EnableTracing makes every GORM statement that runs under a request span
// (a valid span in Statement.Context) a CLIENT child span named
// "<operation> <table>" with db.system, db.operation.name,
// db.collection.name and the statement text (GORM sends values as bind
// parameters, so the text holds placeholders, not data; it is truncated).
//
// A statement with no active span in its context starts nothing: the relay,
// Asynq jobs and pollers query constantly and would otherwise create a root
// trace per query. Call it once, only when tracing is on; with it never
// called there is no callback and no overhead. It returns an error if GORM
// rejects a callback registration (a duplicate name means it was already
// enabled).
//
// It is a hand-written hook, not gorm.io/plugin/opentelemetry or otelgorm: it
// is about 100 lines and adds no module (the available plugins pull in the
// OpenTelemetry metrics SDK or extra dependencies; see libs/telemetry README).
func EnableTracing(gdb *gorm.DB) error {
	cb := gdb.Callback()
	type reg struct {
		name   string
		before func(string, func(*gorm.DB)) error
		after  func(string, func(*gorm.DB)) error
		op     string
	}
	regs := []reg{
		{"create", cb.Create().Before("gorm:create").Register, cb.Create().After("gorm:create").Register, "INSERT"},
		{"query", cb.Query().Before("gorm:query").Register, cb.Query().After("gorm:query").Register, "SELECT"},
		{"update", cb.Update().Before("gorm:update").Register, cb.Update().After("gorm:update").Register, "UPDATE"},
		{"delete", cb.Delete().Before("gorm:delete").Register, cb.Delete().After("gorm:delete").Register, "DELETE"},
		{"row", cb.Row().Before("gorm:row").Register, cb.Row().After("gorm:row").Register, "SELECT"},
		{"raw", cb.Raw().Before("gorm:raw").Register, cb.Raw().After("gorm:raw").Register, "RAW"},
	}
	var errs []error
	for _, r := range regs {
		errs = append(errs,
			r.before("smartcourse:trace_before_"+r.name, startSpan(r.op)),
			r.after("smartcourse:trace_after_"+r.name, endSpan),
		)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("db: enable tracing: %w", err)
	}
	return nil
}

func startSpan(op string) func(*gorm.DB) {
	return func(tx *gorm.DB) {
		ctx := tx.Statement.Context
		if ctx == nil || !trace.SpanContextFromContext(ctx).IsValid() {
			return
		}
		name := op
		if t := tx.Statement.Table; t != "" {
			name = op + " " + t
		}
		ctx, span := otel.Tracer(tracerName).Start(ctx, name,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("db.system.name", "postgresql"),
				attribute.String("db.operation.name", op),
				attribute.String("db.collection.name", tx.Statement.Table),
			))
		tx.Statement.Context = ctx
		tx.Statement.Settings.Store(spanKey, span)
	}
}

func endSpan(tx *gorm.DB) {
	v, ok := tx.Statement.Settings.LoadAndDelete(spanKey)
	if !ok {
		return
	}
	span, ok := v.(trace.Span)
	if !ok {
		return
	}
	if sql := tx.Statement.SQL.String(); sql != "" {
		if len(sql) > maxStatementLen {
			sql = sql[:maxStatementLen]
		}
		span.SetAttributes(attribute.String("db.query.text", sql))
	}
	// A missing row is a normal answer, not a failure.
	if err := tx.Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		// Only the error type: a Postgres error message can quote the
		// offending row ("Key (email)=(...) already exists").
		span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
		span.SetStatus(codes.Error, "")
	}
	span.End()
}
