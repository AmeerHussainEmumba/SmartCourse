package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope of every span this module starts.
const tracerName = "github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"

var errorHandlerOnce sync.Once

// Provider owns the tracer provider created by Setup. The zero value and the
// value returned for a disabled Config are valid no-ops.
type Provider struct {
	tp   *sdktrace.TracerProvider
	once sync.Once
	err  error
}

// Setup builds the tracer provider from cfg, exports to the OTLP/HTTP
// endpoint and installs it, plus the W3C propagator, as the process globals.
// It does not contact the collector, so an unreachable Jaeger never blocks
// boot; export failures are logged and spans are dropped.
//
// A disabled Config returns a no-op Provider without touching any global.
// Call Shutdown before exit to flush buffered spans.
func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return &Provider{}, nil
	}
	var opts []otlptracehttp.Option
	if strings.Contains(cfg.Endpoint, "://") {
		opts = append(opts, otlptracehttp.WithEndpointURL(cfg.Endpoint))
	} else {
		opts = append(opts, otlptracehttp.WithEndpoint(cfg.Endpoint))
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create OTLP exporter: %w", err)
	}
	return SetupWithExporter(cfg, exp)
}

// SetupWithExporter is Setup with a caller-supplied exporter (Endpoint is not
// required). Tests use it with an in-memory exporter. A disabled cfg returns a
// no-op Provider and the exporter is not used.
func SetupWithExporter(cfg Config, exp sdktrace.SpanExporter) (*Provider, error) {
	if err := cfg.validate(false); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return &Provider{}, nil
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(resourceAttrs(cfg)...))
	if err != nil {
		return nil, fmt.Errorf("telemetry: build resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(errorAwareSampler{inner: sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))}),
		sdktrace.WithSpanProcessor(promoteErrors{sdktrace.NewBatchSpanProcessor(exp)}),
	)
	errorHandlerOnce.Do(func() {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
			slog.Warn("opentelemetry error", slog.String("error", err.Error()))
		}))
	})
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagator)
	return &Provider{tp: tp}, nil
}

func resourceAttrs(cfg Config) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("service.name", cfg.ServiceName)}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, attribute.String("service.version", cfg.ServiceVersion))
	}
	if cfg.Environment != "" {
		attrs = append(attrs, attribute.String("deployment.environment.name", cfg.Environment))
	}
	return attrs
}

// Enabled reports whether this Provider records and exports spans.
func (p *Provider) Enabled() bool { return p != nil && p.tp != nil }

// ForceFlush exports buffered spans without shutting down.
func (p *Provider) ForceFlush(ctx context.Context) error {
	if !p.Enabled() {
		return nil
	}
	return p.tp.ForceFlush(ctx)
}

// Shutdown flushes buffered spans and stops the exporter. It is idempotent and
// safe on a nil or disabled Provider; pass a context with a deadline.
func (p *Provider) Shutdown(ctx context.Context) error {
	if !p.Enabled() {
		return nil
	}
	p.once.Do(func() { p.err = p.tp.Shutdown(ctx) })
	return p.err
}

// errorAwareSampler turns the inner sampler's Drop into RecordOnly. The span
// is still recorded (so its final status is known) but its sampled flag stays
// clear, so propagation and head-based semantics are unchanged.
type errorAwareSampler struct{ inner sdktrace.Sampler }

func (s errorAwareSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	r := s.inner.ShouldSample(p)
	if r.Decision == sdktrace.Drop {
		r.Decision = sdktrace.RecordOnly
	}
	return r
}

func (s errorAwareSampler) Description() string {
	return "ErrorAware{" + s.inner.Description() + "}"
}

// promoteErrors forwards a span the head sampler declined when it ended in
// error, so errors are never lost to sampling. The SDK batch processor ignores
// spans without the sampled flag, hence the wrapper that presents it.
type promoteErrors struct{ sdktrace.SpanProcessor }

func (p promoteErrors) OnEnd(s sdktrace.ReadOnlySpan) {
	if !s.SpanContext().IsSampled() {
		if s.Status().Code != codes.Error {
			return
		}
		s = promotedSpan{s}
	}
	p.SpanProcessor.OnEnd(s)
}

type promotedSpan struct{ sdktrace.ReadOnlySpan }

func (s promotedSpan) SpanContext() trace.SpanContext {
	sc := s.ReadOnlySpan.SpanContext()
	return sc.WithTraceFlags(sc.TraceFlags() | trace.FlagsSampled)
}

// propagator is the W3C trace-context propagator used by every helper. It is
// used directly rather than through the global so helpers behave identically
// whether or not Setup ran.
var propagator propagation.TextMapPropagator = propagation.TraceContext{}
