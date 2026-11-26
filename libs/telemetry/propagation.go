package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const traceparentHeader = "traceparent"

// TraceparentHeader is the name under which every carrier (a Kafka record
// header, an Asynq task header) holds the W3C traceparent.
const TraceparentHeader = traceparentHeader

// TraceparentFromIDs builds a W3C traceparent from the trace_id and span_id
// columns of an outbox row (32 and 16 lowercase hex characters, RFC §6.4), for
// a writer that stored the two ids and not the header. The sampled flag is set:
// a row only has ids because a recording span existed when it was written, and
// the ids alone do not say whether the head sampler kept it, so a relay or
// consumer continuing the trace exports its spans even for a trace the HTTP
// side dropped (a fragment with no root, never a lost error). It returns "" for
// ids that are empty, mis-sized, not hex or all zero.
func TraceparentFromIDs(traceID, spanID string) string {
	if len(traceID) != 32 || len(spanID) != 16 {
		return ""
	}
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return ""
	}
	sid, err := trace.SpanIDFromHex(spanID)
	if err != nil {
		return ""
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	if !sc.IsValid() {
		return ""
	}
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"
}

// InjectTraceContext captures the trace context of the active span in ctx at
// the moment of a state change, so it can be stored on the outbox row and the
// event envelope (RFC §6.4: trace_id, span_id) and continued later.
//
// traceparent is the W3C header value ("00-<trace>-<span>-<flags>"); store it
// as the authoritative carrier. With no valid span in ctx (tracing disabled,
// or no request span) all three results are empty.
func InjectTraceContext(ctx context.Context) (traceID, spanID, traceparent string) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", "", ""
	}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return sc.TraceID().String(), sc.SpanID().String(), carrier.Get(traceparentHeader)
}

// ContinueFromEvent returns a context whose next span is a child of the
// remote span described by traceparent, which the relay, Kafka consumer or
// Asynq job read back from the envelope.
//
// A missing or malformed traceparent is not an error: the returned context
// carries no parent, so the next span starts a fresh trace, even when ctx
// already holds an active span (a consumer loop's span must not adopt an
// unrelated event).
func ContinueFromEvent(ctx context.Context, traceparent string) context.Context {
	if traceparent != "" {
		carrier := propagation.MapCarrier{traceparentHeader: traceparent}
		extracted := propagator.Extract(ctx, carrier)
		if sc := trace.SpanContextFromContext(extracted); sc.IsValid() && sc.IsRemote() {
			return extracted
		}
	}
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}

// TraceIDsFromContext returns the trace and span ids of the active span for
// log correlation (RFC §12.4), or empty strings when there is none.
func TraceIDsFromContext(ctx context.Context) (traceID, spanID string) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", ""
	}
	return sc.TraceID().String(), sc.SpanID().String()
}

func tracer() trace.Tracer { return otel.Tracer(tracerName) }

// StartProduce starts a PRODUCER span around publishing one event to Kafka.
// Call it with the context continued from the outbox row's traceparent. The
// span's own context is what InjectTraceContext should capture to put on the
// Kafka message headers, so the consumer becomes its child.
func StartProduce(ctx context.Context, topic, eventType, eventID string) (context.Context, trace.Span) {
	return tracer().Start(ctx, topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.type", "publish"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("smartcourse.event.type", eventType),
			attribute.String("smartcourse.event.id", eventID),
		))
}

// StartRelay continues the trace carried by traceparent (see TraceparentFromIDs)
// and starts the span of the outbox relay delivering one row. mode is
// "publish" (relay.ModePublish, the only mode now). The span is INTERNAL: the
// produce span (StartProduce) is its child.
func StartRelay(ctx context.Context, traceparent, mode, eventType, eventID string) (context.Context, trace.Span) {
	ctx = ContinueFromEvent(ctx, traceparent)
	return tracer().Start(ctx, "outbox relay "+mode,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("smartcourse.relay.mode", mode),
			attribute.String("smartcourse.event.type", eventType),
			attribute.String("smartcourse.event.id", eventID),
		))
}

// StartConsume continues the trace carried by traceparent (see
// ContinueFromEvent) and starts a CONSUMER span for processing one Kafka
// message in a consumer group.
func StartConsume(ctx context.Context, traceparent, group, topic, eventType, eventID string) (context.Context, trace.Span) {
	ctx = ContinueFromEvent(ctx, traceparent)
	return tracer().Start(ctx, topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.consumer.group.name", group),
			attribute.String("smartcourse.event.type", eventType),
			attribute.String("smartcourse.event.id", eventID),
		))
}

// StartJob continues the trace carried by traceparent and starts a CONSUMER
// span for running one Asynq task. The Asynq enqueue side should store
// InjectTraceContext's traceparent in the task payload.
func StartJob(ctx context.Context, traceparent, queue, jobType, jobID string) (context.Context, trace.Span) {
	ctx = ContinueFromEvent(ctx, traceparent)
	return tracer().Start(ctx, jobType+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "asynq"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination.name", queue),
			attribute.String("smartcourse.job.type", jobType),
			attribute.String("smartcourse.job.id", jobID),
		))
}

// StartEnqueue starts a PRODUCER span around enqueueing an Asynq task; capture
// InjectTraceContext from the returned context for the task payload.
func StartEnqueue(ctx context.Context, queue, jobType string) (context.Context, trace.Span) {
	return tracer().Start(ctx, jobType+" enqueue",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "asynq"),
			attribute.String("messaging.operation.type", "publish"),
			attribute.String("messaging.destination.name", queue),
			attribute.String("smartcourse.job.type", jobType),
		))
}

// EndSpan ends span, recording err and marking the span as failed when err is
// non-nil. A failed span is always exported, whatever the sample ratio.
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "")
	}
	span.End()
}
