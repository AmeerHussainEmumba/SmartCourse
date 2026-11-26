package telemetry

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// newTestProvider installs a provider that exports to an in-memory exporter
// and restores the global tracer provider afterwards.
func newTestProvider(t *testing.T, ratio float64) (*Provider, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	p, err := SetupWithExporter(Config{Enabled: true, ServiceName: "test", SampleRatio: ratio}, exp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
		otel.SetTracerProvider(noop.NewTracerProvider())
	})
	return p, exp
}

func flush(t *testing.T, p *Provider) {
	t.Helper()
	if err := p.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"zero is disabled and valid", Config{}, true},
		{"disabled ignores junk", Config{SampleRatio: 7}, true},
		{"enabled complete", Config{Enabled: true, ServiceName: "a", Endpoint: "jaeger:4318", SampleRatio: 0.5}, true},
		{"missing service", Config{Enabled: true, Endpoint: "x:1", SampleRatio: 1}, false},
		{"missing endpoint", Config{Enabled: true, ServiceName: "a", SampleRatio: 1}, false},
		{"ratio above one", Config{Enabled: true, ServiceName: "a", Endpoint: "x:1", SampleRatio: 1.1}, false},
		{"ratio negative", Config{Enabled: true, ServiceName: "a", Endpoint: "x:1", SampleRatio: -0.1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestDisabledIsNoop(t *testing.T) {
	before := otel.GetTracerProvider()
	p, err := Setup(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if p.Enabled() {
		t.Fatal("disabled config produced an enabled provider")
	}
	if otel.GetTracerProvider() != before {
		t.Fatal("disabled Setup replaced the global tracer provider")
	}
	if err := p.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	var nilP *Provider
	if err := nilP.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	// No span in context: helpers return nothing and continuing starts a root.
	if a, b, c := InjectTraceContext(context.Background()); a != "" || b != "" || c != "" {
		t.Fatalf("got %q %q %q with no active span", a, b, c)
	}
}

func TestSetupRejectsInvalid(t *testing.T) {
	if _, err := Setup(context.Background(), Config{Enabled: true}); err == nil {
		t.Fatal("expected error for enabled config without service name and endpoint")
	}
}

func TestSetupOTLPDoesNotContactCollector(t *testing.T) {
	// Nothing listens on this port; boot must still succeed and shutdown must
	// return promptly.
	p, err := Setup(context.Background(), Config{
		Enabled: true, ServiceName: "t", Endpoint: "127.0.0.1:1", Insecure: true, SampleRatio: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Logf("shutdown reported export failure (expected, collector absent): %v", err)
	}
}

func TestSamplingRatioZeroKeepsOnlyErrors(t *testing.T) {
	p, exp := newTestProvider(t, 0)

	_, ok := tracer().Start(context.Background(), "fine")
	ok.End()
	_, bad := tracer().Start(context.Background(), "boom")
	EndSpan(bad, errors.New("failed"))
	flush(t, p)

	spans := exp.GetSpans()
	if len(spans) != 1 || spans[0].Name != "boom" {
		t.Fatalf("exported %d spans %v, want only the errored one", len(spans), names(spans))
	}
}

func TestSamplingRatioOneKeepsEverything(t *testing.T) {
	p, exp := newTestProvider(t, 1)
	_, a := tracer().Start(context.Background(), "a")
	a.End()
	_, b := tracer().Start(context.Background(), "b")
	EndSpan(b, errors.New("x"))
	flush(t, p)
	if n := len(exp.GetSpans()); n != 2 {
		t.Fatalf("exported %d spans, want 2", n)
	}
}

func TestUnsampledParentStaysUnsampledOnTheWire(t *testing.T) {
	_, _ = newTestProvider(t, 0)
	ctx, span := tracer().Start(context.Background(), "root")
	defer span.End()
	if !span.IsRecording() {
		t.Fatal("span must record so an error can still be exported")
	}
	_, _, tp := InjectTraceContext(ctx)
	if tp == "" || tp[len(tp)-2:] != "00" {
		t.Fatalf("traceparent %q should carry the unsampled flag", tp)
	}
}

// countingExporter counts spans handed to it (the in-memory exporter clears
// its buffer on Shutdown, which would hide a flush).
type countingExporter struct{ n atomic.Int32 }

func (c *countingExporter) ExportSpans(_ context.Context, s []sdktrace.ReadOnlySpan) error {
	c.n.Add(int32(len(s)))
	return nil
}
func (c *countingExporter) Shutdown(context.Context) error { return nil }

func TestShutdownFlushesAndIsIdempotent(t *testing.T) {
	exp := &countingExporter{}
	p, err := SetupWithExporter(Config{Enabled: true, ServiceName: "t", SampleRatio: 1}, exp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	_, s := tracer().Start(context.Background(), "pending")
	s.End()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := exp.n.Load(); n != 1 {
		t.Fatalf("shutdown exported %d spans, want 1", n)
	}
}

func names(s tracetest.SpanStubs) []string {
	out := make([]string, len(s))
	for i := range s {
		out[i] = s[i].Name
	}
	return out
}
