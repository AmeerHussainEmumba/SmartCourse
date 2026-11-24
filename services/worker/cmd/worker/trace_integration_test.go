//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/router"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/notification"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport"
)

// pipeline is a worker's event side plus the HTTP side that feeds it, in one
// process over real Postgres, Redis and (in kafka mode) Kafka, with spans
// exported to an in-memory exporter when tracing is on.
type pipeline struct {
	t       *testing.T
	env     *testenv.Env
	deps    *infra.Dependencies
	cfg     *config.Config
	obs     *infra.Observability
	exp     *tracetest.InMemoryExporter
	router  http.Handler
	student uuid.UUID
	course  uuid.UUID
	token   string
	rl      *relay.Relay

	mu   sync.Mutex
	sent []notification.Message
}

// startPipeline builds the pipeline for mode and starts the relay, the
// consumers, the Asynq servers and the notification sweeper. With tracing off,
// Observe is given no endpoint, so it installs no provider and no hooks.
func startPipeline(t *testing.T, mode string, tracing bool) *pipeline {
	t.Helper()
	p := &pipeline{t: t, env: testenv.Setup(t)}
	conn := p.env.OpenWithSearchPath(t, "platform,course,analytics,public")
	p.deps = &infra.Dependencies{DB: conn, RDB: p.env.RDB}
	p.cfg = asynqTestConfig(p.env.RDB.Options().Addr)
	p.cfg.Env = "test"
	p.cfg.Relay.Workers = 3
	p.cfg.JWT = config.JWTConfig{SigningKey: "test-signing-key-at-least-32-bytes-long!!", Issuer: "smartcourse-test", AccessTokenTTL: 15 * time.Minute}
	useTransport(t, p.cfg, mode)
	p.cfg.Telemetry = config.TelemetryConfig{SampleRatio: 1, MetricsEnabled: true}
	opts := infra.ObserveOptions{Service: "worker", Outbox: true, Workflows: true}
	if tracing {
		p.cfg.Telemetry.OTLPEndpoint = "jaeger:4318" // never contacted: the exporter below replaces OTLP
		p.exp = tracetest.NewInMemoryExporter()
		opts.SpanExporter = p.exp
	}
	p.obs = p.deps.Observe(context.Background(), p.cfg, opts)
	t.Cleanup(func() {
		p.obs.Shutdown(context.Background())
		otel.SetTracerProvider(noop.NewTracerProvider())
	})

	// The HTTP side: the enrollment routes, as course-api registers them, behind
	// the same observability middleware.
	r, err := router.NewWithOptions(router.Options{Env: "test", Observe: p.obs.Middleware()})
	if err != nil {
		t.Fatal(err)
	}
	enrollment.RegisterRoutes(r.Group("/api/v1"), enrollment.NewService(enrollment.NewRepository(p.deps.DB)), p.cfg.JWT)
	p.router = r

	// A published course with a seat, and a student the notification mirror
	// knows (the identity events that normally fill it are not under test here).
	instructor := uuid.New()
	p.student, p.course = uuid.New(), uuid.New()
	versionID, moduleID, lessonID := uuid.New(), uuid.New(), uuid.New()
	exec := func(q string, args ...any) {
		t.Helper()
		if err := p.env.DB.Exec(q, args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO courses (id, instructor_id, slug, enrollment_limit) VALUES (?, ?, 'trace-e2e', 10)`, p.course, instructor)
	exec(`INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 1, 'ready', 'Tracing 101', '{}')`, versionID, p.course)
	exec(`INSERT INTO modules (id, course_version_id, module_key, position, title) VALUES (?, ?, ?, 1, 'M1')`, moduleID, versionID, uuid.New())
	exec(`INSERT INTO lessons (id, module_id, lesson_key, position, title, content_type) VALUES (?, ?, ?, 1, 'L1', 'text')`, lessonID, moduleID, uuid.New())
	exec(`UPDATE courses SET published_version_id = ? WHERE id = ?`, versionID, p.course)
	exec(`INSERT INTO notification.recipients (user_id, email, name) VALUES (?, 'ada@example.com', 'Ada')`, p.student)
	exec(`INSERT INTO notification.course_titles (course_id, title) VALUES (?, 'Tracing 101')`, p.course)
	p.token, err = auth.IssueAccessToken(p.cfg.JWT, p.student, "student", 1)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	provider := notification.ProviderFunc(func(_ context.Context, m notification.Message) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.sent = append(p.sent, m)
		return nil
	})
	servers, scheduler, err := buildAsynq(p.cfg, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeNotify := wireNotifications(ctx, logger, servers, p.deps, p.cfg, provider)
	t.Cleanup(closeNotify)
	if err := servers.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Shutdown)
	t.Cleanup(servers.Shutdown)
	p.rl, _, _ = runEventsObserved(t, ctx, p.cfg, p.deps, p.obs)
	return p
}

// enroll sends POST /api/v1/enrollments for the student, optionally inside the
// trace named by traceparent, and returns the status code.
func (p *pipeline) enroll(traceparent string) int {
	p.t.Helper()
	body, _ := json.Marshal(map[string]string{"course_id": p.course.String()})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/enrollments", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")
	if traceparent != "" {
		req.Header.Set("traceparent", traceparent)
	}
	rec := httptest.NewRecorder()
	p.router.ServeHTTP(rec, req)
	if rec.Code >= 300 {
		p.t.Fatalf("enroll returned %d: %s", rec.Code, rec.Body)
	}
	return rec.Code
}

// waitSent waits for the enrollment confirmation to reach the provider and its
// delivery row to be marked sent.
func (p *pipeline) waitSent() {
	p.t.Helper()
	pollUntil(p.t, 60*time.Second, func() []string {
		if scalarOf(p.env.DB, `SELECT count(*) FROM notification.deliveries WHERE status = 'sent'`) < 1 {
			return []string{"no delivery is 'sent' yet"}
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if len(p.sent) < 1 {
			return []string{"the provider has seen no message"}
		}
		return nil
	})
}

func attr(s tracetest.SpanStub, key string) string {
	for _, a := range s.Attributes {
		if string(a.Key) == key {
			return a.Value.Emit()
		}
	}
	return ""
}

// spans returns the exported spans of trace tid.
func (p *pipeline) spans(tid string) []tracetest.SpanStub {
	p.t.Helper()
	if err := p.obs.ForceFlush(context.Background()); err != nil {
		p.t.Fatal(err)
	}
	var out []tracetest.SpanStub
	for _, s := range p.exp.GetSpans() {
		if s.SpanContext.TraceID().String() == tid {
			out = append(out, s)
		}
	}
	return out
}

// descendsFrom reports whether s has ancestor among spans (by walking parents).
func descendsFrom(s, ancestor tracetest.SpanStub, spans []tracetest.SpanStub) bool {
	byID := map[trace.SpanID]tracetest.SpanStub{}
	for _, x := range spans {
		byID[x.SpanContext.SpanID()] = x
	}
	for cur := s; cur.Parent.IsValid(); {
		if cur.Parent.SpanID() == ancestor.SpanContext.SpanID() {
			return true
		}
		next, ok := byID[cur.Parent.SpanID()]
		if !ok {
			return false
		}
		cur = next
	}
	return false
}

func first(spans []tracetest.SpanStub, match func(tracetest.SpanStub) bool) (tracetest.SpanStub, bool) {
	for _, s := range spans {
		if match(s) {
			return s, true
		}
	}
	return tracetest.SpanStub{}, false
}

// One request, one trace, across the whole event pipeline (RFC §12.2, ADR-0032,
// the E2 spec): the HTTP server span, the outbox insert, the relay span, in
// kafka mode the produce span and the consume span, the handler's database span
// under the consumer (or relay), and the delivery job, all under the trace id
// the request arrived with. The notification delivery is the proof that the
// trace crosses the sweeper and Asynq: that task is enqueued seconds later by a
// loop that is in no trace.
func TestWorker_OneTraceAcrossPipeline(t *testing.T) {
	for _, mode := range transportModes {
		t.Run(mode, func(t *testing.T) {
			p := startPipeline(t, mode, true)
			const tid, parent = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
			p.enroll("00-" + tid + "-" + parent + "-01")
			p.waitSent()
			spans := p.spans(tid)

			httpSpan, ok := first(spans, func(s tracetest.SpanStub) bool {
				return s.SpanKind == trace.SpanKindServer && s.Name == "POST /api/v1/enrollments"
			})
			if !ok {
				t.Fatalf("no HTTP server span in the trace; spans: %v", spanNames(spans))
			}
			if _, ok := first(spans, func(s tracetest.SpanStub) bool {
				return attr(s, "db.collection.name") == "outbox" && descendsFrom(s, httpSpan, spans)
			}); !ok {
				t.Errorf("no outbox insert span under the HTTP span; spans: %v", spanNames(spans))
			}

			wantMode := relay.ModeProcess
			if mode == config.TransportKafka {
				wantMode = relay.ModePublish
			}
			relaySpan, ok := first(spans, func(s tracetest.SpanStub) bool {
				return s.Name == "outbox relay "+wantMode && attr(s, "smartcourse.event.type") == "enrollment.student_enrolled"
			})
			if !ok {
				t.Fatalf("no relay span for the enrollment; spans: %v", spanNames(spans))
			}
			if relaySpan.Parent.SpanID() != httpSpan.SpanContext.SpanID() {
				t.Errorf("relay span parent = %s, want the HTTP server span %s (the stored span id)", relaySpan.Parent.SpanID(), httpSpan.SpanContext.SpanID())
			}

			// The span the handlers run under: the consume span in kafka mode,
			// the relay span in-process.
			handlerSpan := relaySpan
			if mode == config.TransportKafka {
				produce, ok := first(spans, func(s tracetest.SpanStub) bool {
					return s.SpanKind == trace.SpanKindProducer && attr(s, "messaging.system") == "kafka"
				})
				if !ok {
					t.Fatalf("no produce span; spans: %v", spanNames(spans))
				}
				if produce.Parent.SpanID() != relaySpan.SpanContext.SpanID() {
					t.Errorf("produce span parent = %s, want the relay span", produce.Parent.SpanID())
				}
				consumes := 0
				for _, s := range spans {
					if s.SpanKind == trace.SpanKindConsumer && attr(s, "messaging.system") == "kafka" {
						consumes++
						if s.Parent.SpanID() != produce.SpanContext.SpanID() {
							t.Errorf("consume span %s (group %s) parent = %s, want the produce span", s.Name, attr(s, "messaging.consumer.group.name"), s.Parent.SpanID())
						}
					}
				}
				if consumes == 0 {
					t.Fatalf("no consume span; spans: %v", spanNames(spans))
				}
				handlerSpan, ok = first(spans, func(s tracetest.SpanStub) bool {
					return s.SpanKind == trace.SpanKindConsumer && attr(s, "messaging.system") == "kafka" &&
						attr(s, "messaging.consumer.group.name") == "notification-dispatcher"
				})
				if !ok {
					t.Fatalf("no consume span for the notification group; spans: %v", spanNames(spans))
				}
			}
			if _, ok := first(spans, func(s tracetest.SpanStub) bool {
				return attr(s, "db.system.name") == "postgresql" && s.Parent.SpanID() == handlerSpan.SpanContext.SpanID()
			}); !ok {
				t.Errorf("no handler database span under %q; spans: %v", handlerSpan.Name, spanNames(spans))
			}

			job, ok := first(spans, func(s tracetest.SpanStub) bool {
				return attr(s, "smartcourse.job.type") == notification.TaskTypeDeliver && s.SpanKind == trace.SpanKindConsumer
			})
			if !ok {
				t.Fatalf("no delivery job span in the trace; spans: %v", spanNames(spans))
			}
			if !descendsFrom(job, handlerSpan, spans) || !descendsFrom(job, httpSpan, spans) {
				t.Errorf("the delivery job span is not a descendant of the handler span and the HTTP span; spans: %v", spanNames(spans))
			}
			if enq, ok := first(spans, func(s tracetest.SpanStub) bool {
				return s.SpanKind == trace.SpanKindProducer && attr(s, "messaging.system") == "asynq"
			}); !ok || job.Parent.SpanID() != enq.SpanContext.SpanID() {
				t.Errorf("the delivery job's parent is not the Asynq enqueue span (found=%v)", ok)
			}
			// Nothing of this request started a second trace: every span any
			// part of the pipeline exported for the enrollment is in tid, apart
			// from background work (the sweep's own queries run in no trace).
			for _, s := range p.exp.GetSpans() {
				if s.SpanContext.TraceID().String() != tid && (s.Name == "outbox relay "+wantMode || strings.Contains(s.Name, "notification:deliver")) {
					t.Errorf("span %q started its own trace %s", s.Name, s.SpanContext.TraceID())
				}
			}
		})
	}
}

func spanNames(spans []tracetest.SpanStub) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name
	}
	return out
}

// With tracing off nothing of the new code runs: no observer on the relay, no
// traceparent on the Kafka record, none stored on the delivery row, no spans,
// and the pipeline still delivers.
func TestWorker_TracingDisabled_NoSpansHeadersOrFailures(t *testing.T) {
	for _, mode := range transportModes {
		t.Run(mode, func(t *testing.T) {
			p := startPipeline(t, mode, false)
			if p.obs.TracingEnabled() {
				t.Fatal("tracing is on without an OTLP endpoint")
			}
			p.enroll("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01") // an incoming header must not matter
			p.waitSent()

			if n := scalarOf(p.env.DB, `SELECT count(*) FROM notification.deliveries WHERE traceparent IS NOT NULL`); n != 0 {
				t.Errorf("%d delivery rows stored a traceparent with tracing off", n)
			}
			if _, f, g := p.rl.Stats(); f != 0 || g != 0 {
				t.Errorf("relay failed=%d given_up=%d with tracing off", f, g)
			}
			if mode == config.TransportKafka {
				recs := readTopic(t, p.cfg.Kafka.Brokers, topics.Enrollment, 1)
				for _, r := range recs {
					for _, h := range r.Headers {
						if h.Key == transport.HeaderTraceparent {
							t.Errorf("record on %s has a traceparent header with tracing off: %s", r.Topic, h.Value)
						}
					}
				}
			}
		})
	}
}

// readTopic reads at least n records from the start of topic.
func readTopic(t *testing.T, brokers []string, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records from %s before the deadline", len(out), n, topic)
		}
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

// With tracing on the record carries the produce span's traceparent, so a
// consumer that has no envelope to read (or reads the header first) continues
// the right span.
func TestWorker_KafkaRecordCarriesTheProduceSpansTraceparent(t *testing.T) {
	p := startPipeline(t, config.TransportKafka, true)
	const tid, parent = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	p.enroll("00-" + tid + "-" + parent + "-01")
	p.waitSent()
	spans := p.spans(tid)
	produce, ok := first(spans, func(s tracetest.SpanStub) bool { return s.SpanKind == trace.SpanKindProducer && attr(s, "messaging.system") == "kafka" })
	if !ok {
		t.Fatalf("no produce span; spans: %v", spanNames(spans))
	}
	var header string
	for _, r := range readTopic(t, p.cfg.Kafka.Brokers, topics.Enrollment, 1) {
		for _, h := range r.Headers {
			if h.Key == transport.HeaderTraceparent {
				header = string(h.Value)
			}
		}
	}
	want := "00-" + tid + "-" + produce.SpanContext.SpanID().String() + "-01"
	if header != want {
		t.Errorf("record traceparent header = %q, want %q (trace id and the produce span id)", header, want)
	}
}
