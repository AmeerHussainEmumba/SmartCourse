package eventbus

import (
	"context"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

// HeaderTraceparent is the Kafka record header the producer puts the W3C
// traceparent of its produce span in and the consumer continues from.
const HeaderTraceparent = telemetry.TraceparentHeader

// RelayObserver is the relay's observer (relay.WithObserver, ADR-0032): each
// delivery of an outbox row becomes a span that continues the trace stored on
// the row. The row stores trace_id and span_id, not a traceparent, and the span
// id is the HTTP server span's, so the relay span is that span's child (and the
// produce span is the relay span's child). A row
// with no ids (written with tracing off) starts its own trace. The context also
// carries a logger with event_id for code that logs through logging.Correlated.
func RelayObserver() relay.Observer {
	return func(ctx context.Context, evt relay.DecodedEvent, mode string) (context.Context, func(error)) {
		ctx, span := telemetry.StartRelay(ctx, telemetry.TraceparentFromIDs(evt.TraceID, evt.SpanID), mode, evt.EventType, evt.EventID.String())
		ctx = logging.WithContext(ctx, logging.WithEventID(logging.FromContext(ctx), evt.EventID.String()))
		return ctx, func(err error) { telemetry.EndSpan(span, err) }
	}
}

func outcomeOf(duplicate bool, err error) telemetry.Outcome {
	switch {
	case err != nil:
		return telemetry.OutcomeFailed
	case duplicate:
		return telemetry.OutcomeDuplicate
	default:
		return telemetry.OutcomeProcessed
	}
}

// consumeTraceparent is the traceparent a consumed record continues: the
// produce span's, from the record header, or when the record has none (written
// by a producer that did not trace, or with tracing off) the one rebuilt from
// the envelope's trace_id and span_id.
func consumeTraceparent(rec kafka.Record, evt relay.DecodedEvent) string {
	if v, ok := rec.Header(HeaderTraceparent); ok && len(v) > 0 {
		return string(v)
	}
	return telemetry.TraceparentFromIDs(evt.TraceID, evt.SpanID)
}
