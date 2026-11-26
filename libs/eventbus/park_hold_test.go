package eventbus

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// B-06 / B-07: when the dead-letter sink is down (MongoDB outage) a consumer
// that has exhausted a record's budget must not let the offset advance. These
// tests need no broker: exhausted is the hook libs/kafka calls, and its error
// return is what decides whether the offset advances.

type flakySink struct {
	mu       sync.Mutex
	failures int // refuse this many calls, then accept
	calls    int
	parked   []relay.DeadLetter
}

func (s *flakySink) Park(_ context.Context, d relay.DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failures {
		return errors.New("mongo down")
	}
	s.parked = append(s.parked, d)
	return nil
}

func (s *flakySink) callCount() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

func holdConsumers(sink relay.DeadLetterSink) *Consumers {
	return &Consumers{
		cfg: consumerSettings{Sink: sink, ParkBackoffBase: time.Millisecond, ParkBackoffMax: 5 * time.Millisecond},
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

var poisonRecord = kafka.Record{Topic: "enrollment.events", Partition: 2, Offset: 41, Key: []byte("k"), Value: []byte("not a frame")}

// The sink refuses far more often than the old three tries; the record is held
// the whole time and parked once the sink accepts it.
func TestExhausted_SinkDown_HoldsThenParksWhenItRecovers(t *testing.T) {
	sink := &flakySink{failures: 25}
	c := holdConsumers(sink)
	if err := c.exhausted(context.Background(), group{name: "g"}, poisonRecord, errors.New("handler failed")); err != nil {
		t.Fatalf("exhausted = %v, want nil once the sink accepted the record", err)
	}
	if len(sink.parked) != 1 {
		t.Fatalf("parked %d records, want 1", len(sink.parked))
	}
	if c.ParkFailures() != 25 {
		t.Fatalf("ParkFailures = %d, want 25 (every refused try counted)", c.ParkFailures())
	}
	if c.HeldRecords() != 0 {
		t.Fatalf("HeldRecords = %d after the record was parked", c.HeldRecords())
	}
}

// A sink that never comes back: the record is held (observable) until the
// consumer stops, and then exhausted reports an error so libs/kafka leaves the
// offset where it is. It never returns nil, which would drop the record.
func TestExhausted_SinkDown_NeverReturnsNilAndIsBoundedByShutdown(t *testing.T) {
	sink := &flakySink{failures: 1 << 30}
	c := holdConsumers(sink)
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- c.exhausted(ctx, group{name: "g"}, poisonRecord, errors.New("handler failed")) }()

	deadline := time.Now().Add(5 * time.Second)
	for c.HeldRecords() != 1 || sink.callCount() < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("record not held (held=%d, sink calls=%d)", c.HeldRecords(), sink.callCount())
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-res:
		t.Fatalf("exhausted returned %v while the sink was down and the consumer running; the offset would advance", err)
	default:
	}
	cancel()
	select {
	case err := <-res:
		if err == nil {
			t.Fatal("exhausted returned nil on shutdown with the record unparked; it would be committed and lost")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exhausted did not return after the context was cancelled")
	}
	if c.HeldRecords() != 0 {
		t.Fatalf("HeldRecords = %d after the hold ended", c.HeldRecords())
	}
}

// With no sink configured the documented gap is unchanged: log and drop.
func TestExhausted_NoSink_LogsAndDrops(t *testing.T) {
	c := holdConsumers(nil)
	if err := c.exhausted(context.Background(), group{name: "g"}, poisonRecord, errors.New("x")); err != nil {
		t.Fatalf("exhausted = %v, want nil (the record is dropped, loudly)", err)
	}
}

// The held-record gauge is non-zero while the sink refuses a record, counts each
// refusal, and returns to zero once the record is parked.
func TestExhausted_SinkDown_ExportsHeldGaugeAndParkFailures(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := telemetry.NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	sink := &flakySink{failures: 3}
	c := holdConsumers(sink)
	c.cfg.Metrics = m
	g := group{name: "g"}

	done := make(chan error, 1)
	go func() { done <- c.exhausted(context.Background(), g, poisonRecord, errors.New("handler failed")) }()
	if err := <-done; err != nil {
		t.Fatalf("exhausted = %v", err)
	}
	if got := testutil.ToFloat64(m.DeadLetterHeld.WithLabelValues("g")); got != 0 {
		t.Errorf("held gauge after the hold ended = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.DeadLetterParkFailures.WithLabelValues("g")); got != 3 {
		t.Errorf("park failures = %v, want 3", got)
	}
}

func TestExhausted_SinkDown_HeldGaugeIsOneWhileHolding(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, _ := telemetry.NewMetrics(reg)
	sink := &flakySink{failures: 1 << 30}
	c := holdConsumers(sink)
	c.cfg.Metrics = m
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.exhausted(ctx, group{name: "g"}, poisonRecord, errors.New("handler failed")) }()
	deadline := time.Now().Add(2 * time.Second)
	for testutil.ToFloat64(m.DeadLetterHeld.WithLabelValues("g")) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("held gauge never reached 1")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("exhausted returned nil while the sink was still refusing")
	}
	if got := testutil.ToFloat64(m.DeadLetterHeld.WithLabelValues("g")); got != 0 {
		t.Errorf("held gauge after stopping = %v, want 0", got)
	}
}
