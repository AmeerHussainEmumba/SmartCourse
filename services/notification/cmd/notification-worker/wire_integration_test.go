//go:build integration

package main

// The notification-worker end to end on real Postgres (as svc_notification),
// Redis, Kafka and, for one test, MongoDB: runtime.Run drives wire() exactly as
// main does, events are produced to Kafka (the source topic and the retry topic),
// and the effects are read back from the database, the fake SMTP server and the
// internal listener. Nothing here calls a handler or the sweeper directly.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus/transporttest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

const consumerGroup = "notification-dispatcher"

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// stack is the shared infrastructure; each subtest runs its own worker on it.
type stack struct {
	t       *testing.T
	env     *testenv.Env
	brokers []string
	regURL  string
	schemas eventbus.Schemas
	pub     *eventbus.Publisher
	kcfg    config.KafkaConfig
}

func newStack(t *testing.T) *stack {
	t.Helper()
	env := testenv.Setup(t)
	kb := testenv.SetupKafka(t)
	reg := transporttest.NewRegistry(t)
	k := config.KafkaConfig{
		Brokers: kb.Brokers, SchemaRegistryURL: reg.URL,
		ConsumerMaxInFlight: 4, ConsumerMaxAttempts: 2,
		ConsumerBackoffBase: 50 * time.Millisecond, ConsumerBackoffMax: 200 * time.Millisecond,
		ConsumerShutdownTimeout: 5 * time.Second, ProduceTimeout: 10 * time.Second,
	}
	schemas, err := eventbus.Prepare(context.Background(), k.Brokers, k.SchemaRegistryURL)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	pub, err := eventbus.NewPublisherFor(k, schemas, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	return &stack{t: t, env: env, brokers: kb.Brokers, regURL: reg.URL, schemas: schemas, pub: pub, kcfg: k}
}

func (s *stack) event(eventType string, payload map[string]any) relay.DecodedEvent {
	raw, _ := json.Marshal(payload)
	return relay.DecodedEvent{
		EventID: uuid.New(), AggregateType: "x", AggregateID: uuid.New(), EventType: eventType,
		SchemaVersion: int(s.schemas.PayloadVersions[eventType]), Payload: raw, OccurredAt: time.Now().UTC(),
	}
}

func (s *stack) publish(e relay.DecodedEvent) {
	s.t.Helper()
	if err := s.pub.Publish(context.Background(), e); err != nil {
		s.t.Fatalf("publish %s: %v", e.EventType, err)
	}
}

func (s *stack) publishRetry(e relay.DecodedEvent) {
	s.t.Helper()
	id := s.schemas.EnvelopeIDs[topics.Identity]
	if err := s.pub.PublishRetry(context.Background(), consumerGroup, id, e); err != nil {
		s.t.Fatalf("publish retry %s: %v", e.EventType, err)
	}
}

func (s *stack) count(q string, args ...any) int64 {
	s.t.Helper()
	var n int64
	if err := s.env.Owner.Raw(q, args...).Scan(&n).Error; err != nil {
		s.t.Fatalf("%s: %v", q, err)
	}
	return n
}

func (s *stack) eventually(d time.Duration, what string, cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out after %v waiting for: %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

type worker struct {
	t      *testing.T
	addr   string
	logs   *syncBuf
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// start runs the worker with cfg's SMTP and Mongo settings.
func (s *stack) start(smtp config.SMTPConfig, mongo config.MongoConfig) *worker {
	t := s.t
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	cfg := &config.Config{
		Env:       "test",
		Postgres:  config.PostgresConfig{DSN: s.env.ServiceDSN(t, "notification"), MaxOpenConns: 10, MaxIdleConns: 2, SearchPath: "notification,public"},
		Redis:     config.RedisConfig{Addr: s.env.RDB.Options().Addr},
		Telemetry: config.TelemetryConfig{MetricsAddr: addr, MetricsEnabled: true, SampleRatio: 1},
		Kafka:     s.kcfg,
		Asynq: config.AsynqConfig{
			CriticalConcurrency: 4, DefaultConcurrency: 1, LowConcurrency: 1, MaxRetry: 3,
			BackoffBase: time.Second, BackoffMax: 2 * time.Second, ShutdownTimeout: 5 * time.Second,
		},
		Jobs: config.JobsConfig{OutboxPruneInterval: time.Hour},
		SMTP: smtp, Mongo: mongo,
	}
	w := &worker{t: t, addr: addr, logs: &syncBuf{}, done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	logger := slog.New(slog.NewTextHandler(w.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	go func() {
		w.err = runtime.Run(ctx, "notification-worker", wire,
			runtime.WithConfig(func() (*config.Config, error) { return cfg, nil }), runtime.WithLogger(logger))
		close(w.done)
	}()
	t.Cleanup(w.stop)
	return w
}

func (w *worker) stop() {
	w.cancel()
	select {
	case <-w.done:
	case <-time.After(40 * time.Second):
		w.t.Error("worker did not shut down")
	}
}

func (w *worker) get(path string) (int, string) {
	resp, err := http.Get("http://" + w.addr + path)
	if err != nil {
		return 0, ""
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// ready waits for /health/ready to answer 200 and returns the per-check map.
func (w *worker) ready(s *stack) map[string]any {
	var checks map[string]any
	s.eventually(60*time.Second, "worker ready", func() bool {
		code, body := w.get("/health/ready")
		var m map[string]any
		_ = json.Unmarshal([]byte(body), &m)
		c, _ := m["checks"].(map[string]any)
		if code == 200 && c != nil {
			checks = c
			return true
		}
		return false
	})
	return checks
}

func smtpAt(f *fakeSMTP) config.SMTPConfig {
	h, p := f.hostPort()
	return config.SMTPConfig{Host: h, Port: p, From: "no-reply@smartcourse.local", ConnectTimeout: time.Second, SendTimeout: 5 * time.Second}
}

// poisonEvent encodes fine but its handler always fails: no student_id.
func poisonEvent(s *stack) relay.DecodedEvent {
	return s.event("progress.certificate_issued", map[string]any{"certificate_id": uuid.New(), "enrollment_id": uuid.New(), "course_id": uuid.New()})
}

func register(s *stack, email string) relay.DecodedEvent {
	return s.event("identity.user_registered", map[string]any{"user_id": uuid.New(), "email": email, "role": "student", "full_name": "Ada"})
}

func (f *fakeSMTP) mailTo(addr string) int {
	n := 0
	for _, m := range f.Messages() {
		for _, to := range m.to {
			if to == addr {
				n++
			}
		}
	}
	return n
}

// Components registered and working together: the internal listener serves
// /metrics and readiness, the consumer group reads the source topic AND its retry
// topic, the sweeper enqueues, the delivery task on the critical queue sends the
// mail, the marker lands in notification.processed_events. A poison record with
// no Mongo configured is logged and dropped, and the partition moves on.
func TestWorker_ConsumesSweepsAndSends(t *testing.T) {
	s := newStack(t)
	fake := newFakeSMTP(t)
	w := s.start(smtpAt(fake), config.MongoConfig{})
	checks := w.ready(s)
	for _, name := range []string{"postgres", "redis", "kafka", "smtp"} {
		if checks[name] != "ok" {
			t.Errorf("readiness check %q = %v, want ok (checks: %v)", name, checks[name], checks)
		}
	}
	if _, has := checks["mongodb"]; has {
		t.Error("a mongodb readiness check exists although MONGO_URI is unset")
	}
	if code, body := w.get("/metrics"); code != 200 || !strings.Contains(body, "db_connections_in_use") {
		t.Fatalf("internal listener /metrics = %d %.200q", code, body)
	}

	// A poison record first, then a good one behind it on the source topic: the
	// poison is dropped (no sink) and the good one is still processed.
	s.publish(poisonEvent(s))
	viaSource := register(s, "source@example.com")
	s.publish(viaSource)
	// The retry topic takes the same path.
	viaRetry := register(s, "retry@example.com")
	s.publishRetry(viaRetry)

	s.eventually(90*time.Second, "both welcome mails sent", func() bool {
		return fake.mailTo("source@example.com") == 1 && fake.mailTo("retry@example.com") == 1
	})
	if n := s.count(`SELECT count(*) FROM notification.processed_events WHERE event_id IN (?, ?)`, viaSource.EventID, viaRetry.EventID); n != 2 {
		t.Fatalf("notification.processed_events has %d of the 2 markers", n)
	}
	s.eventually(30*time.Second, "the poison record to be dropped and logged", func() bool {
		return strings.Contains(w.logs.String(), "no dead-letter sink is configured")
	})
	if !strings.Contains(w.logs.String(), "notification-dispatcher") {
		t.Error("the consumer group name notification-dispatcher is not in the log")
	}
	w.stop()
	if w.err != nil {
		t.Fatalf("Run: %v", w.err)
	}
}

// No provider configured: no delivery task, no sweeper, a warning, the row stays
// pending, and the worker is still ready.
func TestWorker_NoProviderLeavesDeliveriesPending(t *testing.T) {
	s := newStack(t)
	w := s.start(config.SMTPConfig{}, config.MongoConfig{})
	checks := w.ready(s)
	if _, has := checks["smtp"]; has {
		t.Error("an smtp readiness check exists without SMTP_HOST")
	}
	e := register(s, "pending@example.com")
	s.publish(e)
	s.eventually(60*time.Second, "the delivery row to be written", func() bool {
		return s.count(`SELECT count(*) FROM notification.deliveries`) >= 1
	})
	time.Sleep(4 * time.Second) // bounded absence: two sweep intervals would have enqueued and sent it
	if n := s.count(`SELECT count(*) FROM notification.deliveries WHERE status <> 'pending'`); n != 0 {
		t.Fatalf("%d deliveries left pending without a provider", n)
	}
	if !strings.Contains(w.logs.String(), "stay pending") {
		t.Errorf("no warning that deliveries stay pending:\n%s", w.logs.String())
	}
}

// A permanent provider failure is archived, not retried: one SMTP attempt, the
// row failed, one archived task on the critical queue, nothing waiting to retry.
func TestWorker_PermanentFailureIsArchivedNotRetried(t *testing.T) {
	s := newStack(t)
	fake := newFakeSMTP(t)
	fake.failRcpt = "550 no such mailbox"
	w := s.start(smtpAt(fake), config.MongoConfig{})
	w.ready(s)
	s.publish(register(s, "gone@example.com"))

	insp := asynqx.NewInspector(asynqx.RedisClient(s.env.RDB.Options().Addr, "", 0))
	t.Cleanup(func() { _ = insp.Close() })
	s.eventually(90*time.Second, "the task to be archived on the critical queue", func() bool {
		st, err := insp.Stats(asynqx.QueueCritical)
		return err == nil && st[0].Archived == 1
	})
	st, _ := insp.Stats(asynqx.QueueCritical)
	if st[0].Retry != 0 || st[0].Pending != 0 {
		t.Fatalf("critical queue = %+v, want nothing retrying or pending", st[0])
	}
	if n := s.count(`SELECT count(*) FROM notification.deliveries WHERE status = 'failed'`); n != 1 {
		t.Fatalf("%d failed deliveries, want 1", n)
	}
	rcpts := 0
	for _, c := range fake.Commands() {
		if strings.HasPrefix(strings.ToUpper(c), "RCPT") {
			rcpts++
		}
	}
	if rcpts != 1 {
		t.Fatalf("SMTP saw %d RCPT commands, want exactly 1 (no retry)", rcpts)
	}
}

// With MONGO_URI configured an event that exhausts its budget is parked in
// failed_events (with the consumer name, one history entry per attempt) before
// the offset advances.
func TestWorker_ExhaustedEventIsParkedInMongo(t *testing.T) {
	s := newStack(t)
	mg := testenv.SetupMongo(t)
	mcfg := config.MongoConfig{URI: mg.URI, Database: "smartcourse", ConnectTimeout: 5 * time.Second, ServerSelectionTimeout: 5 * time.Second, OpTimeout: 5 * time.Second}
	fake := newFakeSMTP(t)
	w := s.start(smtpAt(fake), mcfg)
	checks := w.ready(s)
	if checks["mongodb"] != "ok" {
		// The connection is made in the background; wait for it.
		s.eventually(30*time.Second, "mongodb ready", func() bool {
			_, body := w.get("/health/ready")
			return strings.Contains(body, `"mongodb":"ok"`)
		})
	}

	poison := poisonEvent(s)
	s.publish(poison)

	store, err := mongostore.Open(context.Background(), mongostore.Config{URI: mg.URI, Database: "smartcourse", ConnectTimeout: 5 * time.Second, ServerSelectionTimeout: 5 * time.Second, OpTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	var fe *mongostore.FailedEvent
	s.eventually(60*time.Second, "the event to be parked", func() bool {
		var err error
		fe, err = store.GetFailedEvent(context.Background(), poison.EventID.String(), consumerGroup)
		return err == nil && fe != nil
	})
	if fe.Consumer != "notification-dispatcher" || fe.EventType != "progress.certificate_issued" {
		t.Fatalf("parked record = %+v", fe)
	}
	if strings.Contains(w.logs.String(), "no dead-letter sink") {
		t.Error("the sink was not used although MONGO_URI is set")
	}
}
