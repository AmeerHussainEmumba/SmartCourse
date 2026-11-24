package cache

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope of the spans this file starts.
const tracerName = "github.com/AmeerHussainEmumba/SmartCourse/libs/cache"

// EnableTracing adds a go-redis hook that records each command run under a
// request span as a CLIENT child span named after the command ("get",
// "zadd"); a pipeline is one span "pipeline get set". The span carries
// db.system.name=redis and db.operation.name only: never the key or the
// arguments, which hold user ids and tokens.
//
// A command with no active span in its context starts nothing, so Asynq
// polling and pollers do not create a root trace per command. Call it once,
// only when tracing is on; never calling it leaves the client untouched and
// free of overhead. redis.Nil (a cache miss) is not an error.
//
// Hand-written instead of redisotel (go-redis/extra) to avoid a second
// module; see libs/telemetry README.
func EnableTracing(client *redis.Client) {
	client.AddHook(tracingHook{})
}

type tracingHook struct{}

func (tracingHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}

func (tracingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		ctx, span := start(ctx, cmd.Name())
		err := next(ctx, cmd)
		finish(span, err)
		return err
	}
}

func (tracingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		name := "pipeline"
		for i, c := range cmds {
			if i == 3 { // bound the span name
				name += " ..."
				break
			}
			name += " " + c.Name()
		}
		ctx, span := start(ctx, name)
		err := next(ctx, cmds)
		if span != nil {
			span.SetAttributes(attribute.Int("db.operation.batch.size", len(cmds)))
		}
		finish(span, err)
		return err
	}
}

func start(ctx context.Context, name string) (context.Context, trace.Span) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, nil
	}
	ctx, span := otel.Tracer(tracerName).Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "redis"),
			attribute.String("db.operation.name", name),
		))
	return ctx, span
}

func finish(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
		span.SetStatus(codes.Error, "")
	}
	span.End()
}
