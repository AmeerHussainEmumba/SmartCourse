package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func spanByName(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()
	for _, s := range spans {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no span named %q in %v", name, names(spans))
	return tracetest.SpanStub{}
}

// TestTraceSurvivesHTTPOutboxConsumerJob simulates the RFC §12.2 chain:
// POST /enrollments writes an outbox row, the relay publishes, a Kafka
// consumer handles it and enqueues an Asynq job, the worker runs the job.
// Only strings (what the outbox row and message headers hold) cross each hop.
func TestTraceSurvivesHTTPOutboxConsumerJob(t *testing.T) {
	gin.SetMode(gin.TestMode)
	p, exp := newTestProvider(t, 1)

	// Hop 1: HTTP request; the handler writes the outbox row.
	var rowTraceID, rowSpanID, rowTraceparent string
	r := gin.New()
	r.Use(HTTPTracing())
	r.POST("/enrollments", func(c *gin.Context) {
		rowTraceID, rowSpanID, rowTraceparent = InjectTraceContext(c.Request.Context())
		c.Status(http.StatusAccepted)
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/enrollments", nil))
	if rowTraceID == "" || rowSpanID == "" || rowTraceparent == "" {
		t.Fatal("outbox row got no trace context")
	}

	// Hop 2: relay (a different goroutine, fresh context) publishes the row.
	relayCtx, produce := StartProduce(ContinueFromEvent(context.Background(), rowTraceparent),
		"enrollment.events", "enrollment.student_enrolled", "evt-1")
	_, _, kafkaHeader := InjectTraceContext(relayCtx)
	produce.End()

	// Hop 3: consumer, then it enqueues a job carrying the context in the payload.
	consumeCtx, consume := StartConsume(context.Background(), kafkaHeader,
		"notification-dispatcher", "enrollment.events", "enrollment.student_enrolled", "evt-1")
	enqCtx, enqueue := StartEnqueue(consumeCtx, "notifications", "email:enrollment")
	_, _, jobPayload := InjectTraceContext(enqCtx)
	enqueue.End()
	consume.End()

	// Hop 4: the worker runs the job.
	_, job := StartJob(context.Background(), jobPayload, "notifications", "email:enrollment", "job-1")
	EndSpan(job, nil)
	flush(t, p)

	spans := exp.GetSpans()
	if len(spans) != 5 {
		t.Fatalf("got %d spans %v, want 5", len(spans), names(spans))
	}
	for _, s := range spans {
		if got := s.SpanContext.TraceID().String(); got != rowTraceID {
			t.Errorf("span %q trace id %s, want %s", s.Name, got, rowTraceID)
		}
	}
	http1 := spanByName(t, spans, "POST /enrollments")
	if http1.SpanContext.SpanID().String() != rowSpanID {
		t.Error("outbox row span id is not the HTTP span")
	}
	prod := spanByName(t, spans, "enrollment.events publish")
	cons := spanByName(t, spans, "enrollment.events process")
	enq := spanByName(t, spans, "email:enrollment enqueue")
	jb := spanByName(t, spans, "email:enrollment process")
	wantParent := map[string]trace.SpanID{
		"publish": http1.SpanContext.SpanID(), // relay continues the HTTP span
		"consume": prod.SpanContext.SpanID(),  // consumer continues the publish span
		"enqueue": cons.SpanContext.SpanID(),
		"job":     enq.SpanContext.SpanID(),
	}
	for name, got := range map[string]trace.SpanID{
		"publish": prod.Parent.SpanID(), "consume": cons.Parent.SpanID(),
		"enqueue": enq.Parent.SpanID(), "job": jb.Parent.SpanID(),
	} {
		if got != wantParent[name] {
			t.Errorf("%s parent = %s, want %s", name, got, wantParent[name])
		}
	}
	if http1.Parent.IsValid() {
		t.Error("HTTP span should be the root")
	}
	if cons.SpanKind != trace.SpanKindConsumer || prod.SpanKind != trace.SpanKindProducer {
		t.Error("wrong span kinds")
	}
	if !hasAttr(cons, "messaging.consumer.group.name", "notification-dispatcher") ||
		!hasAttr(prod, "messaging.destination.name", "enrollment.events") ||
		!hasAttr(jb, "smartcourse.job.id", "job-1") {
		t.Error("missing messaging attributes")
	}
}

func hasAttr(s tracetest.SpanStub, key, want string) bool {
	for _, a := range s.Attributes {
		if string(a.Key) == key && a.Value.AsString() == want {
			return true
		}
	}
	return false
}

func TestContinueFromEventBadInputStartsFreshTrace(t *testing.T) {
	p, exp := newTestProvider(t, 1)

	// An unrelated active span in ctx must not be adopted either.
	outerCtx, outer := tracer().Start(context.Background(), "outer")
	defer outer.End()

	const zeros = "00-00000000000000000000000000000000-0000000000000000-01"
	cases := map[string]string{
		"empty":            "",
		"garbage":          "not-a-traceparent",
		"all zero ids":     zeros,
		"bad version":      "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"short trace id":   "00-4bf92f3577b34da6-00f067aa0ba902b7-01",
		"non hex":          "00-zzf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"truncated":        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
		"huge":             strings.Repeat("a", 1<<16),
		"newline injected": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\n",
	}
	for name, tp := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, span := tracer().Start(ContinueFromEvent(outerCtx, tp), "child")
			defer span.End()
			sc := span.SpanContext()
			if !sc.IsValid() {
				t.Fatal("no valid span started")
			}
			if sc.TraceID() == outer.SpanContext().TraceID() {
				t.Fatal("adopted the ambient trace instead of starting fresh")
			}
			if sc.TraceID().String() == "4bf92f3577b34da6a3ce929d0e0e4736" {
				t.Fatal("accepted a malformed traceparent")
			}
			if trace.SpanContextFromContext(ctx).IsRemote() {
				t.Fatal("span context should be local")
			}
		})
	}
	flush(t, p)
	for _, s := range exp.GetSpans() {
		if s.Name == "child" && s.Parent.IsValid() {
			t.Errorf("fresh-trace span has parent %s", s.Parent.SpanID())
		}
	}
}

func TestContinueFromEventValid(t *testing.T) {
	_, _ = newTestProvider(t, 1)
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	_, span := tracer().Start(ContinueFromEvent(context.Background(), tp), "child")
	defer span.End()
	if got := span.SpanContext().TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace id %s not continued", got)
	}
}

func TestInjectRoundTrip(t *testing.T) {
	_, _ = newTestProvider(t, 1)
	ctx, span := tracer().Start(context.Background(), "x")
	defer span.End()
	traceID, spanID, tp := InjectTraceContext(ctx)
	want := "00-" + traceID + "-" + spanID + "-01"
	if tp != want {
		t.Fatalf("traceparent %q, want %q", tp, want)
	}
	if a, b := TraceIDsFromContext(ctx); a != traceID || b != spanID {
		t.Fatalf("TraceIDsFromContext = %s %s", a, b)
	}
}

func TestHTTPTracingContinuesIncomingTraceparent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	p, exp := newTestProvider(t, 1)
	r := gin.New()
	r.Use(HTTPTracing())
	r.GET("/courses/:id", func(c *gin.Context) { c.Status(http.StatusInternalServerError) })
	req := httptest.NewRequest(http.MethodGet, "/courses/123", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	r.ServeHTTP(httptest.NewRecorder(), req)
	flush(t, p)

	s := spanByName(t, exp.GetSpans(), "GET /courses/:id")
	if s.SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Error("incoming trace not continued")
	}
	if s.Status.Code.String() != "Error" {
		t.Errorf("5xx span status %v, want Error", s.Status.Code)
	}
	if !hasAttr(s, "http.route", "/courses/:id") {
		t.Error("route attribute missing")
	}
}
