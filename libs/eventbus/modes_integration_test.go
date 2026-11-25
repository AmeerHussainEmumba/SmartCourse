//go:build integration

// Both delivery modes of Consumers against a real broker: NewConsumers (the
// handler and its processed_events marker in one Postgres transaction) and
// NewStreamConsumers (no database: the handler is delivered plainly, for a
// service whose idempotency lives elsewhere). Every test here runs the same
// scenario in both and asserts the same outcome, because everything but the
// transaction is shared: the group per consumer name, the retry topic
// subscription, bounded in-flight, parking through the sink with one history
// entry per attempt, the relay.Source seam, tracing and metrics. The stream
// stack has no Postgres at all (newKafkaStack).
//
// Not covered: the real Schema Registry (a stand-in serves ids and versions),
// more than one broker.
package eventbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

type deliveryMode struct {
	name   string
	stream bool
}

var deliveryModes = []deliveryMode{{"transaction", false}, {"stream", true}}

// forEachMode runs fn once per delivery mode, each with its own stack.
func forEachMode(t *testing.T, fn func(t *testing.T, m *modeStack)) {
	t.Helper()
	for _, dm := range deliveryModes {
		t.Run(dm.name, func(t *testing.T) { fn(t, newModeStack(t, dm)) })
	}
}

// modeStack feeds events to the consumers the way each mode's service does: the
// transaction mode through the outbox and a publish-only relay (Postgres), the
// stream mode by producing to Kafka directly (nothing else exists).
type modeStack struct {
	*stack
	stream bool
	pub    *eventbus.Publisher
}

func newModeStack(t *testing.T, dm deliveryMode) *modeStack {
	t.Helper()
	m := &modeStack{stream: dm.stream}
	if dm.stream {
		m.stack = newKafkaStack(t)
	} else {
		m.stack = newStack(t)
	}
	m.pub = m.publisher(10 * time.Second)
	if !dm.stream {
		m.startRelay(m.pub, 2)
	}
	return m
}

// publish puts one effectType event for agg with payload seq on the identity topic.
func (m *modeStack) publish(agg uuid.UUID, seq int) {
	m.t.Helper()
	if !m.stream {
		m.write(agg, seq)
		return
	}
	evt, _ := m.storedEffectEvent(agg, seq)
	if err := m.pub.Publish(context.Background(), evt); err != nil {
		m.t.Fatalf("publish: %v", err)
	}
}

// garbage puts a record that is not an Envelope on the identity topic under agg's key.
func (m *modeStack) garbage(agg uuid.UUID) {
	m.t.Helper()
	p, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: m.kafka.Brokers})
	if err != nil {
		m.t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Produce(context.Background(), kafka.Message{Topic: topics.Identity, Key: []byte(agg.String()), Value: []byte("not an envelope")}); err != nil {
		m.t.Fatal(err)
	}
}

func (m *modeStack) run(cfg runnerCfg, handlers ...relay.Handler) (*eventbus.Consumers, func()) {
	cfg.Stream = m.stream
	return m.startRunner(cfg, handlers...)
}

// seen is one successful handler call as the handler observed it.
type seen struct {
	id     uuid.UUID
	agg    uuid.UUID
	seq    int
	txNil  bool
	src    relay.Source
	hasSrc bool
	span   bool
}

// recorder is a handler that uses no database, so it runs in both modes: it
// records what it saw, can fail or panic on chosen sequence numbers and holds
// each call for a while to measure concurrency.
type recorder struct {
	mu       sync.Mutex
	got      []seen
	attempts map[int]int
	cur, max int
}

func newRecorder() *recorder { return &recorder{attempts: map[int]int{}} }

func (r *recorder) handler(consumer string, hold time.Duration, behave func(seq int) error) relay.Handler {
	return relay.NewHandler(consumer, []string{effectType}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		var p struct {
			UserID   uuid.UUID `json:"user_id"`
			FullName string    `json:"full_name"`
		}
		var seq int
		if err := json.Unmarshal(evt.Payload, &p); err != nil {
			return err
		}
		if _, err := fmt.Sscan(p.FullName, &seq); err != nil {
			return err
		}
		r.mu.Lock()
		r.attempts[seq]++
		r.cur++
		r.max = max(r.max, r.cur)
		r.mu.Unlock()
		defer func() { r.mu.Lock(); r.cur--; r.mu.Unlock() }()
		if hold > 0 {
			select {
			case <-time.After(hold):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if behave != nil {
			if err := behave(seq); err != nil {
				return err
			}
		}
		src, hasSrc := relay.SourceFrom(ctx)
		r.mu.Lock()
		r.got = append(r.got, seen{id: evt.EventID, agg: p.UserID, seq: seq, txNil: tx == nil, src: src, hasSrc: hasSrc,
			span: trace.SpanFromContext(ctx).SpanContext().IsValid()})
		r.mu.Unlock()
		return nil
	})
}

func (r *recorder) all() []seen {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]seen(nil), r.got...)
}

func (r *recorder) seqsOf(agg uuid.UUID) []int {
	var out []int
	for _, s := range r.all() {
		if s.agg == agg {
			out = append(out, s.seq)
		}
	}
	return out
}

func (r *recorder) maxConcurrent() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.max
}

func (r *recorder) tried(seq int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts[seq]
}

// outcomeCount reads event_processed_total{consumer,outcome} from the registry.
func outcomeCount(t *testing.T, reg *prometheus.Registry, consumer string, outcome telemetry.Outcome) float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != telemetry.MetricEventProcessedTotal {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["consumer"] == consumer && labels["outcome"] == string(outcome) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func errString(err error, sub string) bool { return strings.Contains(err.Error(), sub) }

// Per-aggregate order, the record's source in the handler's context, and the
// mode's own contract for the transaction: a handler gets one in the
// transactional mode and nil in the stream mode.
func TestModes_OrderSourceAndTransaction(t *testing.T) {
	forEachMode(t, func(t *testing.T, m *modeStack) {
		const aggs, perAgg = 6, 10
		rec := newRecorder()
		ids := make([]uuid.UUID, aggs)
		for i := range ids {
			ids[i] = uuid.New()
			for n := 0; n < perAgg; n++ {
				m.publish(ids[i], n)
			}
		}
		m.run(runnerCfg{MaxInFlight: 4}, rec.handler("order-consumer", 0, nil))
		m.eventually(90*time.Second, "every event handled", func() bool { return len(rec.all()) == aggs*perAgg })
		for _, id := range ids {
			if got := rec.seqsOf(id); !equalInts(got, seqs(perAgg)) {
				t.Fatalf("aggregate %s handled out of order or incompletely: %v", id, got)
			}
		}
		for _, s := range rec.all() {
			if s.txNil != m.stream {
				t.Fatalf("handler saw a nil transaction = %v, want %v for this mode", s.txNil, m.stream)
			}
			if !s.hasSrc || s.src.Topic != topics.Identity || s.src.Key != s.agg.String() || s.src.Partition < 0 || s.src.Offset < 0 {
				t.Fatalf("handler's relay.Source = %+v (present %v), want the identity record's coordinates", s.src, s.hasSrc)
			}
			if want := uint32(m.schemas.EnvelopeIDs[topics.Identity]); s.src.SchemaID == 0 || s.src.SchemaID != want {
				t.Fatalf("schema id = %d, want the envelope's registry id %d", s.src.SchemaID, want)
			}
		}
	})
}

// A group subscribes to its handlers' topics and to retry.<consumer>, and a
// record replayed there is handled like any other, with the retry topic as its
// source.
func TestModes_SubscribeToTheRetryTopicAndHandleReplays(t *testing.T) {
	forEachMode(t, func(t *testing.T, m *modeStack) {
		rec := newRecorder()
		c, _ := m.run(runnerCfg{}, rec.handler("retry-consumer", 0, nil))
		want := []string{topics.Identity, topics.Retry("retry-consumer")}
		if got := c.Topics("retry-consumer"); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("subscribed to %v, want %v", got, want)
		}
		if g := c.Groups(); len(g) != 1 || g[0] != "retry-consumer" {
			t.Fatalf("groups = %v, want one group named after the consumer", g)
		}
		agg := uuid.New()
		evt, schemaID := m.storedEffectEvent(agg, 7)
		if err := m.pub.PublishRetry(context.Background(), "retry-consumer", schemaID, evt); err != nil {
			t.Fatalf("PublishRetry: %v", err)
		}
		m.eventually(60*time.Second, "the replayed record to be handled", func() bool { return len(rec.all()) == 1 })
		if got := rec.all()[0]; got.id != evt.EventID || got.src.Topic != topics.Retry("retry-consumer") {
			t.Fatalf("handled %+v, want the replayed event from the retry topic", got)
		}
	})
}

// Poison records do not stall the partition and are parked through the sink
// with one history entry per attempt and the record's source; metrics count the
// outcomes. In order on one key: garbage, healthy 1, failing 2, panicking 3,
// healthy 4.
func TestModes_ExhaustedRecordsAreParkedAndTheRestFlows(t *testing.T) {
	forEachMode(t, func(t *testing.T, m *modeStack) {
		agg := uuid.New()
		parked := &sink{}
		reg := telemetry.NewRegistry()
		metrics, err := telemetry.NewMetrics(reg)
		if err != nil {
			t.Fatal(err)
		}
		m.garbage(agg)
		for seq := 1; seq <= 4; seq++ {
			m.publish(agg, seq)
		}
		rec := newRecorder()
		runner, _ := m.run(runnerCfg{Sink: parked, Metrics: metrics}, rec.handler("poison-consumer", 0, func(seq int) error {
			switch seq {
			case 2:
				return errors.New("handler always fails on 2")
			case 3:
				panic("handler panics on 3")
			}
			return nil
		}))

		m.eventually(90*time.Second, "the healthy records behind the poison ones", func() bool { return equalInts(rec.seqsOf(agg), []int{1, 4}) })
		m.eventually(30*time.Second, "all three poison records parked", func() bool { return len(parked.parked()) == 3 })

		var garbage, failing, panicked bool
		for _, d := range parked.parked() {
			if d.Consumer != "poison-consumer" || d.Attempts != 3 || d.Err == nil || len(d.History) != 3 {
				t.Fatalf("dead letter = %+v; want consumer, 3 attempts and one history entry each", d)
			}
			switch {
			case d.Event.EventType != effectType:
				garbage = true
				if d.Event.EventID == uuid.Nil {
					t.Error("the undecodable record was parked without a stable id")
				}
			case errString(d.Err, "always fails"):
				failing = true
			case errString(d.Err, "handler panic"):
				panicked = true
			}
		}
		if !garbage || !failing || !panicked {
			t.Fatalf("parked = %+v; want the undecodable, the failing and the panicking record", parked.parked())
		}
		if len(parked.sources) != 3 {
			t.Fatalf("the sink saw %d relay.Source values, want one per parked record", len(parked.sources))
		}
		for _, src := range parked.sources {
			if src.Topic != topics.Identity || src.Offset < 0 {
				t.Fatalf("parked record source = %+v", src)
			}
		}
		if n := rec.tried(2); n != 3 {
			t.Errorf("the failing handler ran %d times, want the budget of 3", n)
		}
		if runner.ParkFailures() != 0 {
			t.Fatalf("ParkFailures = %d", runner.ParkFailures())
		}
		for outcome, want := range map[telemetry.Outcome]float64{
			telemetry.OutcomeProcessed: 2, telemetry.OutcomeDeadLetter: 3, telemetry.OutcomeFailed: 9,
		} {
			if got := outcomeCount(t, reg, "poison-consumer", outcome); got != want {
				t.Errorf("event_processed_total{outcome=%s} = %v, want %v", outcome, got, want)
			}
		}
	})
}

// A sink that refuses holds the record (retried with backoff, counted, the offset
// not advanced, the record behind it waiting) and parks it once the sink is back
// (B-06, B-07).
func TestModes_SinkThatRefusesHoldsTheRecordUntilItRecovers(t *testing.T) {
	forEachMode(t, func(t *testing.T, m *modeStack) {
		agg := uuid.New()
		parked := &sink{refuse: true}
		m.publish(agg, 1)
		m.publish(agg, 2)
		rec := newRecorder()
		runner, _ := m.run(runnerCfg{Sink: parked}, rec.handler("refused-consumer", 0, func(seq int) error {
			if seq == 1 {
				return errors.New("poison")
			}
			return nil
		}))
		m.eventually(60*time.Second, "the record to be held (sink refused it at least 3 times)", func() bool {
			return runner.HeldRecords() == 1 && parked.attempts() >= 3
		})
		if got := rec.seqsOf(agg); len(got) != 0 {
			t.Fatalf("handled %v while the poison record was unparkable: the offset advanced past it", got)
		}
		parked.setRefuse(false)
		m.eventually(60*time.Second, "the held record parked and the record behind it handled", func() bool {
			return len(parked.parked()) == 1 && equalInts(rec.seqsOf(agg), []int{2})
		})
		if runner.HeldRecords() != 0 || runner.ParkFailures() < 3 {
			t.Fatalf("HeldRecords=%d ParkFailures=%d after recovery", runner.HeldRecords(), runner.ParkFailures())
		}
	})
}

// MaxInFlight bounds the handlers running at once across the group.
func TestModes_InFlightIsBounded(t *testing.T) {
	forEachMode(t, func(t *testing.T, m *modeStack) {
		const events = 24
		rec := newRecorder()
		for i := 0; i < events; i++ {
			m.publish(uuid.New(), i)
		}
		m.run(runnerCfg{MaxInFlight: 2}, rec.handler("inflight-consumer", 150*time.Millisecond, nil))
		m.eventually(90*time.Second, "every event handled", func() bool { return len(rec.all()) == events })
		if got := rec.maxConcurrent(); got != 2 {
			t.Fatalf("max concurrent handlers = %d, want exactly the bound of 2 (it is reached with %d events over several partitions)", got, events)
		}
	})
}

// What the stream mode deliberately does not do: there is no processed_events
// marker, so a replay of an event the handler already handled runs the handler
// again. The handler's own idempotency (the archiver's first-write-wins by event
// id) is what makes that safe. The transactional counterpart, a replay absorbed
// by the marker, is TestConsumers_RetryOfAnAlreadyProcessedEventIsAbsorbed.
func TestStream_ReplayOfAHandledEventRunsTheHandlerAgain(t *testing.T) {
	m := newModeStack(t, deliveryMode{"stream", true})
	if m.env != nil {
		t.Fatal("the stream stack must have no database")
	}
	rec := newRecorder()
	m.run(runnerCfg{}, rec.handler("replay-consumer", 0, nil))
	agg := uuid.New()
	evt, schemaID := m.storedEffectEvent(agg, 1)
	for i := 0; i < 2; i++ {
		if err := m.pub.PublishRetry(context.Background(), "replay-consumer", schemaID, evt); err != nil {
			t.Fatalf("PublishRetry: %v", err)
		}
	}
	m.eventually(60*time.Second, "both deliveries to be handled", func() bool { return len(rec.all()) == 2 })
	if got := rec.all(); got[0].id != evt.EventID || got[1].id != evt.EventID {
		t.Fatalf("handled %+v, want the same event twice", got)
	}
}

// Tracing is opt-in per consumer: with it on each record is a CONSUMER span the
// handler runs under; with it off no span is started even though a provider is
// installed.
func TestModes_TracingIsOptIn(t *testing.T) {
	forEachMode(t, func(t *testing.T, m *modeStack) {
		exp := tracetest.NewInMemoryExporter()
		p, err := telemetry.SetupWithExporter(telemetry.Config{Enabled: true, ServiceName: "eventbus-test", SampleRatio: 1}, exp)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

		off, on := newRecorder(), newRecorder()
		agg := uuid.New()
		m.publish(agg, 1)
		m.run(runnerCfg{Tracing: false}, off.handler("trace-off", 0, nil))
		m.run(runnerCfg{Tracing: true}, on.handler("trace-on", 0, nil))
		m.eventually(60*time.Second, "both consumers handled the event", func() bool { return len(off.all()) == 1 && len(on.all()) == 1 })
		if off.all()[0].span {
			t.Error("a span reached the handler of a consumer with tracing off")
		}
		if !on.all()[0].span {
			t.Error("no span in the handler's context with tracing on")
		}
		m.eventually(30*time.Second, "the consumer span to be exported", func() bool {
			_ = p.ForceFlush(context.Background())
			for _, s := range exp.GetSpans() {
				if s.SpanKind == trace.SpanKindConsumer && s.Name == topics.Identity+" process" {
					return true
				}
			}
			return false
		})
	})
}
