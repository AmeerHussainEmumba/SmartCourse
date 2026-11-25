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
// produce span, or the in-process handlers' spans, are the relay span's). A row
// with no ids (written with tracing off) starts its own trace. The context also
// carries a logger with event_id for code that logs through logging.Correlated.
func RelayObserver() relay.Observer {
	return func(ctx context.Context, evt relay.DecodedEvent, mode string) (context.Context, func(error)) {
		ctx, span := telemetry.StartRelay(ctx, telemetry.TraceparentFromIDs(evt.TraceID, evt.SpanID), mode, evt.EventType, evt.EventID.String())
		ctx = logging.WithContext(ctx, logging.WithEventID(logging.FromContext(ctx), evt.EventID.String()))
		return ctx, func(err error) { telemetry.EndSpan(span, err) }
	}
}

// HandlerCounter is the relay's handler hook for the in-process transport: it
// counts event_processed_total per consumer and outcome, as Consumers does for
// the kafka transport. A nil m yields nil (no hook).
func HandlerCounter(m *telemetry.Metrics) relay.HandlerHook {
	if m == nil {
		return nil
	}
	return func(consumer string, duplicate bool, err error) {
		m.EventProcessed(consumer, outcomeOf(duplicate, err))
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
