//go:build integration

package eventbus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus/transporttest"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// stack is a real Postgres (outbox, processed_events, an effects table), a real
// Kafka broker with the RFC topics provisioned, and the registry stand-in.
// Schema ids come from the stand-in, so what is not exercised is the real
// Schema Registry (see transporttest.Registry).
type stack struct {
	t       *testing.T
	env     *testenv.Env // nil in a stream-only stack
	kafka   *testenv.KafkaEnv
	schemas eventbus.Schemas
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// newStack is newKafkaStack plus Postgres (outbox, processed_events, an effects
// table), for the transactional consumers and the relay.
func newStack(t *testing.T) *stack {
	t.Helper()
	env := testenv.Setup(t)
	s := newKafkaStack(t)
	s.env = env
	// The owner creates the effects table; the stack then acts as the course
	// service (its outbox and processed_events, search_path course,public), the
	// connection a real consumer has (testenv.Env.ServiceDB).
	if err := env.DB.Exec(`CREATE TABLE public.test_effects (
		consumer TEXT NOT NULL, event_id UUID NOT NULL, agg UUID NOT NULL, seq INT NOT NULL, n SERIAL, PRIMARY KEY (consumer, event_id))`).Error; err != nil {
		t.Fatalf("create test_effects: %v", err)
	}
	if err := env.DB.Exec(`GRANT ALL ON public.test_effects, public.test_effects_n_seq TO svc_course`).Error; err != nil {
		t.Fatalf("grant test_effects: %v", err)
	}
	env.DB = env.ServiceDB(t, "course")
	return s
}

// newKafkaStack is a stack with no database at all (env is nil): a broker with
// the topics and the registry stand-in, which is all a stream consumer needs.
func newKafkaStack(t *testing.T) *stack {
	t.Helper()
	kb := testenv.SetupKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := topics.Ensure(ctx, kb.Brokers); err != nil {
		t.Fatalf("topics.Ensure: %v", err)
	}
	reg := transporttest.NewRegistry(t)
	schemas, err := eventbus.LoadSchemas(ctx, registry.New(reg.URL))
	if err != nil {
		t.Fatalf("LoadSchemas: %v", err)
	}
	return &stack{t: t, kafka: kb, schemas: schemas}
}

func (s *stack) publisher(produceTimeout time.Duration) *eventbus.Publisher {
	s.t.Helper()
	p, err := eventbus.NewPublisher(eventbus.PublisherConfig{Brokers: s.kafka.Brokers, ProduceTimeout: produceTimeout, Schemas: s.schemas})
	if err != nil {
		s.t.Fatalf("NewPublisher: %v", err)
	}
	s.t.Cleanup(p.Close)
	return p
}

// startRelay runs a relay in publish mode and returns it with its stop func.
func (s *stack) startRelay(pub relay.Publisher, workers int, opts ...relay.Option) (*relay.Relay, func()) {
	s.t.Helper()
	base := []relay.Option{relay.WithWorkerCount(workers), relay.WithPollInterval(10 * time.Millisecond),
		relay.WithPublishBackoff(100 * time.Millisecond), relay.WithPublisher(pub)}
	rl := relay.New(s.env.DB, append(base, opts...)...)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); <-done }) }
	s.t.Cleanup(stop)
	return rl, stop
}

// runnerCfg is what a test tunes on the consumers; the rest is filled in by
// startRunner (brokers, a short retry budget, a quiet logger).
type runnerCfg struct {
	MaxInFlight     int
	ShutdownTimeout time.Duration
	Sink            relay.DeadLetterSink
	// Stream runs eventbus.NewStreamConsumers (no database) instead of NewConsumers.
	Stream  bool
	Tracing bool
	Metrics *telemetry.Metrics
}

// startRunner runs consumer groups for handlers and returns a stop func that
// waits for them to drain.
func (s *stack) startRunner(cfg runnerCfg, handlers ...relay.Handler) (*eventbus.Consumers, func()) {
	s.t.Helper()
	k := config.KafkaConfig{
		Brokers: s.kafka.Brokers, ConsumerMaxInFlight: cfg.MaxInFlight, ConsumerMaxAttempts: 3,
		ConsumerBackoffBase: 20 * time.Millisecond, ConsumerBackoffMax: 100 * time.Millisecond,
		ConsumerShutdownTimeout: cfg.ShutdownTimeout,
	}
	if k.ConsumerShutdownTimeout == 0 {
		k.ConsumerShutdownTimeout = 5 * time.Second
	}
	opts := []eventbus.ConsumerOption{eventbus.WithLogger(quiet)}
	if cfg.Sink != nil {
		opts = append(opts, eventbus.WithSink(cfg.Sink))
	}
	opts = append(opts, eventbus.WithTracing(cfg.Tracing), eventbus.WithMetrics(cfg.Metrics))
	var r *eventbus.Consumers
	var err error
	if cfg.Stream {
		r, err = eventbus.NewStreamConsumers(k, handlers, opts...)
	} else {
		r, err = eventbus.NewConsumers(k, handlers, s.env.DB, opts...)
	}
	if err != nil {
		s.t.Fatalf("new consumers (stream=%v): %v", cfg.Stream, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); <-done }) }
	s.t.Cleanup(stop)
	return r, stop
}

const effectType = "identity.user_profile_updated"

// write adds one outbox row for aggregate agg whose payload carries seq.
func (s *stack) write(agg uuid.UUID, seq int) {
	s.t.Helper()
	s.writeType(agg, effectType, map[string]any{"user_id": agg, "full_name": fmt.Sprint(seq)})
}

func (s *stack) writeType(agg uuid.UUID, eventType string, payload map[string]any) {
	s.t.Helper()
	err := s.env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "user", AggregateID: agg, EventType: eventType, SchemaVersion: 1, Payload: payload,
		})
	})
	if err != nil {
		s.t.Fatalf("write outbox: %v", err)
	}
}

// effectHandler records one row per event, in the handler's own (relay.Deliver)
// transaction, keyed (consumer, event_id): a second effect for one event is a
// primary-key violation, so "one effect" is enforced by the schema.
func effectHandler(consumer string, before func(ctx context.Context, evt relay.DecodedEvent) error) relay.Handler {
	return relay.NewHandler(consumer, []string{effectType}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		var p struct {
			UserID   uuid.UUID `json:"user_id"`
			FullName string    `json:"full_name"`
		}
		if err := json.Unmarshal(evt.Payload, &p); err != nil {
			return err
		}
		var seq int
		if _, err := fmt.Sscan(p.FullName, &seq); err != nil {
			return err
		}
		if err := tx.Exec(`INSERT INTO public.test_effects (consumer, event_id, agg, seq) VALUES (?, ?, ?, ?)`,
			consumer, evt.EventID, p.UserID, seq).Error; err != nil {
			return err
		}
		if before != nil {
			return before(ctx, evt)
		}
		return nil
	})
}

func (s *stack) count(q string, args ...any) int64 {
	s.t.Helper()
	var n int64
	if err := s.env.DB.Raw(q, args...).Row().Scan(&n); err != nil {
		s.t.Fatalf("count %q: %v", q, err)
	}
	return n
}

func (s *stack) eventually(d time.Duration, what string, cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out after %s waiting for: %s", d, what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// effectsFor returns the seq values of agg's effects in the order they were applied.
func (s *stack) effectsFor(consumer string, agg uuid.UUID) []int {
	s.t.Helper()
	var seqs []int
	if err := s.env.DB.Raw(`SELECT seq FROM public.test_effects WHERE consumer = ? AND agg = ? ORDER BY n`, consumer, agg).Scan(&seqs).Error; err != nil {
		s.t.Fatalf("read effects: %v", err)
	}
	return seqs
}

// sink is a DeadLetterSink that records what it is given and can be told to refuse.
type sink struct {
	mu      sync.Mutex
	got     []relay.DeadLetter
	sources []relay.Source // the relay.Source in each Park call's context
	refuse  bool
	calls   int
}

func (k *sink) Park(ctx context.Context, d relay.DeadLetter) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.calls++
	if src, ok := relay.SourceFrom(ctx); ok {
		k.sources = append(k.sources, src)
	}
	if k.refuse {
		return fmt.Errorf("sink unavailable")
	}
	k.got = append(k.got, d)
	return nil
}

func (k *sink) parked() []relay.DeadLetter {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]relay.DeadLetter(nil), k.got...)
}

func (k *sink) setRefuse(v bool) {
	k.mu.Lock()
	k.refuse = v
	k.mu.Unlock()
}

func (k *sink) attempts() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.calls
}
