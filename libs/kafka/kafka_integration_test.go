//go:build integration

// What this checks, against a real Kafka broker (RFC §18.2: no mocks for
// infrastructure; the image is the one docker-compose pins): a produce and
// consume round trip with key and headers; ordering per key across partitions;
// that a consumer cancelled in the middle of a handler leaves the record
// uncommitted so the next consumer of the group gets it; that a record which
// keeps failing exhausts its budget, fires OnExhausted, and does not stall a
// healthy record behind it on the same partition; that a graceful shutdown
// lets the in-flight handler finish and commit; that in-flight work is
// bounded; that a consumer joining the group mid-stream loses nothing; and
// that records produced while the broker is stopped and restarted are neither
// lost nor duplicated.
//
// One broker is shared by the subtests of TestKafka (a container costs seconds);
// every subtest uses its own topic and group. The restart tests get a broker of
// their own because they stop it. Nothing synchronises on a fixed sleep: tests
// block on channels or poll to a deadline. The only timed waits are bounded
// absence checks, labelled where used.
package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestKafka(t *testing.T) {
	env := testenv.SetupKafka(t)

	// sub runs fn as a subtest with a fresh topic and group name, and fails it
	// if goroutines started by the test outlive it.
	sub := func(name string, fn func(t *testing.T, env *testenv.KafkaEnv)) {
		t.Run(name, func(t *testing.T) {
			baseline := goleak.IgnoreCurrent()
			t.Cleanup(func() {
				if err := goleak.Find(baseline); err != nil {
					t.Errorf("goroutines leaked: %v", err)
				}
			})
			fn(t, env)
		})
	}

	sub("RoundTripKeyAndHeaders", testRoundTrip)
	sub("OrderingPerKey", testOrderingPerKey)
	sub("ConsumerKilledMidHandleIsRedelivered", testKilledMidHandle)
	sub("PoisonRecordDoesNotStallPartition", testPoison)
	sub("GracefulShutdownDrains", testGracefulShutdown)
	sub("InFlightIsBounded", testBoundedInFlight)
	sub("ConsumerJoiningMidStreamLosesNothing", testRebalance)
	sub("PingReportsReachability", testPing)
}

// uniq returns a name no other subtest or run uses.
func uniq(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func createTopic(t *testing.T, brokers []string, name string, partitions int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, partitions, 1, nil, name); err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
}

func newProducer(t *testing.T, brokers []string, cfg kafka.ProducerConfig) *kafka.Producer {
	t.Helper()
	cfg.Brokers = brokers
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func produce(t *testing.T, p *kafka.Producer, topic, key, value string) kafka.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := p.Produce(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(value)})
	if err != nil {
		t.Fatalf("produce %s/%s: %v", key, value, err)
	}
	return res
}

// runner is a consumer running in the background.
type runner struct {
	cancel context.CancelFunc
	errc   chan error
	c      *kafka.Consumer
}

func startConsumer(t *testing.T, cfg kafka.ConsumerConfig) *runner {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = quiet
	}
	if cfg.Backoff == (kafka.BackoffConfig{}) {
		cfg.Backoff = kafka.BackoffConfig{Base: 10 * time.Millisecond, Max: 20 * time.Millisecond}
	}
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{cancel: cancel, errc: make(chan error, 1), c: c}
	go func() { r.errc <- c.Run(ctx) }()
	t.Cleanup(func() { r.cancel(); <-r.errc })
	return r
}

// stop cancels the consumer and returns Run's error. Safe to call twice.
func (r *runner) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.errc:
		r.errc <- err // for the cleanup's receive
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not stop within 30s")
		return nil
	}
}

// sink collects handled values.
type sink struct {
	mu   sync.Mutex
	vals []string
}

func (s *sink) add(v string) {
	s.mu.Lock()
	s.vals = append(s.vals, v)
	s.mu.Unlock()
}

func (s *sink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.vals...)
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func testRoundTrip(t *testing.T, env *testenv.KafkaEnv) {
	topic := uniq("roundtrip")
	createTopic(t, env.Brokers, topic, 3)
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{ClientID: "test-producer"})

	res, err := p.Produce(context.Background(), kafka.Message{
		Topic:   topic,
		Key:     []byte("course-1"),
		Value:   []byte("hello"),
		Headers: []kafka.Header{{Key: "event_id", Value: []byte("e-1")}, {Key: "trace", Value: []byte("t-1")}},
	})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	got := make(chan kafka.Record, 1)
	r := startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: uniq("g"), Topics: []string{topic},
		Handler: func(_ context.Context, rec kafka.Record) error { got <- rec; return nil },
	})
	rec := recv(t, got, "the record")

	if string(rec.Key) != "course-1" || string(rec.Value) != "hello" {
		t.Errorf("key/value = %q/%q", rec.Key, rec.Value)
	}
	if rec.Topic != topic || rec.Partition != res.Partition || rec.Offset != res.Offset {
		t.Errorf("position = %s/%d/%d, producer acked %d/%d", rec.Topic, rec.Partition, rec.Offset, res.Partition, res.Offset)
	}
	if v, ok := rec.Header("event_id"); !ok || string(v) != "e-1" {
		t.Errorf("header event_id = %q, %v", v, ok)
	}
	if v, ok := rec.Header("trace"); !ok || string(v) != "t-1" {
		t.Errorf("header trace = %q, %v", v, ok)
	}
	if rec.Timestamp == 0 {
		t.Error("timestamp not set")
	}
	if err := r.stop(t); err != nil {
		t.Errorf("Run = %v, want nil after a clean drain", err)
	}
	if s := r.c.Stats(); s.Handled != 1 || s.Failed != 0 || s.Exhausted != 0 {
		t.Errorf("stats = %+v", s)
	}
}

func testOrderingPerKey(t *testing.T, env *testenv.KafkaEnv) {
	const keys, perKey = 8, 40
	topic := uniq("ordering")
	createTopic(t, env.Brokers, topic, 4)
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{})

	// Interleave the keys, so ordering per key is not an accident of produce order.
	partOf := map[string]int32{}
	for i := range perKey {
		for k := range keys {
			key := "k" + strconv.Itoa(k)
			res := produce(t, p, topic, key, key+":"+strconv.Itoa(i))
			if prev, ok := partOf[key]; ok && prev != res.Partition {
				t.Fatalf("key %s moved from partition %d to %d", key, prev, res.Partition)
			}
			partOf[key] = res.Partition
		}
	}

	var s sink
	r := startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: uniq("g"), Topics: []string{topic}, MaxInFlight: 4,
		Handler: func(_ context.Context, rec kafka.Record) error { s.add(string(rec.Value)); return nil },
	})
	waitFor(t, 30*time.Second, "all records", func() bool { return len(s.snapshot()) >= keys*perKey })
	_ = r.stop(t)

	next := map[string]int{}
	for _, v := range s.snapshot() {
		var key string
		var n int
		if _, err := fmt.Sscanf(v, "%2s:%d", &key, &n); err != nil {
			t.Fatalf("bad value %q: %v", v, err)
		}
		if n != next[key] {
			t.Fatalf("key %s: got sequence %d, want %d (order broken or duplicate)", key, n, next[key])
		}
		next[key]++
	}
	for k := range keys {
		if next["k"+strconv.Itoa(k)] != perKey {
			t.Errorf("key k%d received %d records, want %d", k, next["k"+strconv.Itoa(k)], perKey)
		}
	}
	if len(distinct(partOf)) < 2 {
		t.Errorf("all keys landed on one partition (%v): the test proves nothing about ordering across partitions", partOf)
	}
}

func distinct(m map[string]int32) map[int32]bool {
	out := map[int32]bool{}
	for _, v := range m {
		out[v] = true
	}
	return out
}

func testKilledMidHandle(t *testing.T, env *testenv.KafkaEnv) {
	topic, group := uniq("killed"), uniq("g")
	createTopic(t, env.Brokers, topic, 1)
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{})
	for _, v := range []string{"a", "b", "c"} {
		produce(t, p, topic, "k", v)
	}

	started := make(chan struct{}, 1)
	a := startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: group, Topics: []string{topic},
		ShutdownTimeout: 300 * time.Millisecond,
		Handler: func(ctx context.Context, rec kafka.Record) error {
			started <- struct{}{}
			<-ctx.Done() // the consumer is killed before the handler returns
			return ctx.Err()
		},
	})
	recv(t, started, "A's handler to start")
	if err := a.stop(t); !errors.Is(err, kafka.ErrDrainTimeout) {
		t.Fatalf("A's Run = %v, want ErrDrainTimeout (its handler was cancelled)", err)
	}

	var s sink
	b := startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: group, Topics: []string{topic},
		Handler: func(_ context.Context, rec kafka.Record) error { s.add(string(rec.Value)); return nil },
	})
	waitFor(t, 30*time.Second, "B to receive the records", func() bool { return len(s.snapshot()) >= 3 })
	_ = b.stop(t)
	if got := s.snapshot(); fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("B received %v, want [a b c]: the interrupted record must be redelivered first and nothing lost", got)
	}
	if st := a.c.Stats(); st.Failed != 0 {
		t.Errorf("A counted %d failures; a cancelled handler is an abandonment, not a failure", st.Failed)
	}
}

func testPoison(t *testing.T, env *testenv.KafkaEnv) {
	topic, group := uniq("poison"), uniq("g")
	createTopic(t, env.Brokers, topic, 1) // one partition: the healthy records are behind the poison ones
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{})
	for _, v := range []string{"poison", "boom", "ok1", "ok2"} {
		produce(t, p, topic, "k", v)
	}

	type exhausted struct {
		rec kafka.Record
		err error
	}
	var (
		s       sink
		calls   sync.Map // value -> *atomic.Int32
		hook    = make(chan exhausted, 4)
		handler = func(_ context.Context, rec kafka.Record) error {
			n, _ := calls.LoadOrStore(string(rec.Value), new(atomic.Int32))
			n.(*atomic.Int32).Add(1)
			switch string(rec.Value) {
			case "poison":
				return errors.New("cannot process")
			case "boom":
				panic("handler bug")
			}
			s.add(string(rec.Value))
			return nil
		}
	)
	cfg := kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: group, Topics: []string{topic}, MaxAttempts: 3,
		Handler:     handler,
		OnExhausted: func(_ context.Context, rec kafka.Record, err error) { hook <- exhausted{rec, err} },
	}
	r := startConsumer(t, cfg)

	for _, want := range []string{"poison", "boom"} {
		ex := recv(t, hook, "OnExhausted for "+want)
		if string(ex.rec.Value) != want || ex.err == nil {
			t.Fatalf("OnExhausted got %q, %v; want %q with an error", ex.rec.Value, ex.err, want)
		}
		if want == "boom" && !errors.Is(ex.err, kafka.ErrPanic) {
			t.Errorf("a panic must reach the hook as ErrPanic, got %v", ex.err)
		}
	}
	waitFor(t, 30*time.Second, "the healthy records behind the poison ones", func() bool { return len(s.snapshot()) == 2 })
	if got := s.snapshot(); fmt.Sprint(got) != "[ok1 ok2]" {
		t.Fatalf("healthy records = %v, want [ok1 ok2] in order", got)
	}
	for _, v := range []string{"poison", "boom"} {
		n, _ := calls.Load(v)
		if got := n.(*atomic.Int32).Load(); got != 3 {
			t.Errorf("%s handled %d times, want exactly the budget of 3", v, got)
		}
	}
	if st := r.c.Stats(); st.Exhausted != 2 || st.Panics != 3 {
		t.Errorf("stats = %+v, want 2 exhausted and 3 panics", st)
	}
	if err := r.stop(t); err != nil {
		t.Fatalf("Run = %v", err)
	}
	select {
	case ex := <-hook:
		t.Fatalf("OnExhausted fired again for %q", ex.rec.Value)
	default:
	}

	// The offset advanced past the poison records: a new consumer of the group
	// sees only what was produced after them.
	produce(t, p, topic, "k", "ok3")
	var s2 sink
	cfg.Handler = func(_ context.Context, rec kafka.Record) error { s2.add(string(rec.Value)); return nil }
	r2 := startConsumer(t, cfg)
	waitFor(t, 30*time.Second, "ok3", func() bool { return len(s2.snapshot()) >= 1 })
	_ = r2.stop(t)
	if got := s2.snapshot(); fmt.Sprint(got) != "[ok3]" {
		t.Fatalf("second consumer saw %v, want [ok3]: the poison offsets must have been committed", got)
	}
}

func testGracefulShutdown(t *testing.T, env *testenv.KafkaEnv) {
	topic, group := uniq("drain"), uniq("g")
	createTopic(t, env.Brokers, topic, 1)
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{})
	produce(t, p, topic, "k", "r1")
	produce(t, p, topic, "k", "r2")

	started, release := make(chan struct{}, 1), make(chan struct{})
	var finished atomic.Bool
	a := startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: group, Topics: []string{topic},
		ShutdownTimeout: 20 * time.Second,
		Handler: func(ctx context.Context, rec kafka.Record) error {
			started <- struct{}{}
			select {
			case <-release:
				finished.Store(true)
				return nil
			case <-ctx.Done():
				return ctx.Err() // would mean the drain did not wait
			}
		},
	})
	recv(t, started, "r1 to start")
	a.cancel()

	// Bounded absence check: Run must still be waiting for the handler.
	select {
	case err := <-a.errc:
		t.Fatalf("Run returned (%v) while a handler was still running", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-a.errc:
		a.errc <- err
		if err != nil {
			t.Fatalf("Run = %v, want nil: the handler finished inside the timeout", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after the handler finished")
	}
	if !finished.Load() {
		t.Fatal("handler was cut off")
	}

	// r1 was committed by the drain; r2 was never started by A.
	var s sink
	b := startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: group, Topics: []string{topic},
		Handler: func(_ context.Context, rec kafka.Record) error { s.add(string(rec.Value)); return nil },
	})
	waitFor(t, 30*time.Second, "r2", func() bool { return len(s.snapshot()) >= 1 })
	_ = b.stop(t)
	if got := s.snapshot(); fmt.Sprint(got) != "[r2]" {
		t.Fatalf("B saw %v, want [r2]: r1 must be committed, r2 delivered once", got)
	}
}

func testBoundedInFlight(t *testing.T, env *testenv.KafkaEnv) {
	const limit, total = 3, 48
	topic := uniq("bounded")
	createTopic(t, env.Brokers, topic, 8)
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{})
	for i := range total {
		produce(t, p, topic, "k"+strconv.Itoa(i), strconv.Itoa(i))
	}

	var running, peak, done atomic.Int32
	r := startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: uniq("g"), Topics: []string{topic}, MaxInFlight: limit,
		Handler: func(_ context.Context, rec kafka.Record) error {
			n := running.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond) // work, so handlers overlap
			running.Add(-1)
			done.Add(1)
			return nil
		},
	})
	waitFor(t, 60*time.Second, "all records", func() bool { return done.Load() >= total })
	_ = r.stop(t)
	if got := peak.Load(); got > limit {
		t.Fatalf("peak concurrent handlers = %d, want at most %d", got, limit)
	}
	if got := peak.Load(); got < 2 {
		t.Errorf("peak concurrent handlers = %d: no parallelism observed across 8 partitions", got)
	}
}

func testRebalance(t *testing.T, env *testenv.KafkaEnv) {
	const total = 120
	topic, group := uniq("rebalance"), uniq("g")
	createTopic(t, env.Brokers, topic, 4)
	p := newProducer(t, env.Brokers, kafka.ProducerConfig{})

	var s sink
	handler := func(_ context.Context, rec kafka.Record) error { s.add(string(rec.Value)); return nil }
	cfg := kafka.ConsumerConfig{Brokers: env.Brokers, GroupID: group, Topics: []string{topic}, Handler: handler}
	startConsumer(t, cfg)

	for i := range total {
		produce(t, p, topic, "k"+strconv.Itoa(i%16), strconv.Itoa(i))
		if i == total/2 {
			startConsumer(t, cfg) // joins mid-stream; partitions move
		}
	}
	waitFor(t, 60*time.Second, "every record at least once", func() bool {
		return len(uniqueSet(s.snapshot())) >= total
	})
	got := s.snapshot()
	t.Logf("handled %d times for %d records (duplicates are allowed: at-least-once)", len(got), total)
	if len(uniqueSet(got)) != total {
		t.Fatalf("lost records: %d distinct of %d", len(uniqueSet(got)), total)
	}
}

func uniqueSet(vals []string) map[string]int {
	out := map[string]int{}
	for _, v := range vals {
		out[v]++
	}
	return out
}

func testPing(t *testing.T, env *testenv.KafkaEnv) {
	ctx := context.Background()
	if err := kafka.Ping(ctx, env.Brokers); err != nil {
		t.Fatalf("Ping against a live broker: %v", err)
	}
	if err := kafka.HealthCheck(env.Brokers)(ctx); err != nil {
		t.Fatalf("HealthCheck against a live broker: %v", err)
	}
	start := time.Now()
	if err := kafka.Ping(ctx, []string{"127.0.0.1:1"}); err == nil {
		t.Fatal("Ping against a closed port succeeded")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("Ping against a closed port took %v; a readiness probe must fail fast", d)
	}
}

// TestBrokerRestart stops the broker while producers are mid-flight, restarts
// it, and checks that what was acknowledged is exactly what is on the topic:
// nothing lost, nothing stored twice, order kept per key. The producers use no
// retry logic of their own: the survival is the client's retries plus the
// idempotent producer.
func TestBrokerRestart(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(*testenv.KafkaEnv, *testing.T)
	}{
		{"GracefulStop", (*testenv.KafkaEnv).Stop},
		{"KillWithoutGrace", (*testenv.KafkaEnv).Kill},
	} {
		t.Run(tc.name, func(t *testing.T) { testBrokerRestart(t, tc.stop) })
	}
}

func testBrokerRestart(t *testing.T, stop func(*testenv.KafkaEnv, *testing.T)) {
	const producers, perProducer = 4, 25
	env := testenv.SetupKafka(t)
	topic := uniq("restart")
	createTopic(t, env.Brokers, topic, 3)

	p := newProducer(t, env.Brokers, kafka.ProducerConfig{
		MaxRetries:      1000,
		Backoff:         kafka.BackoffConfig{Base: 100 * time.Millisecond, Max: time.Second},
		DeliveryTimeout: 90 * time.Second,
	})
	if err := kafka.Ping(context.Background(), env.Brokers); err != nil {
		t.Fatalf("Ping before the outage: %v", err)
	}

	var acked atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, producers)
	for k := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "key" + strconv.Itoa(k)
			for i := range perProducer {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				_, err := p.Produce(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(key + ":" + strconv.Itoa(i))})
				cancel()
				if err != nil {
					errs <- fmt.Errorf("%s record %d: %w", key, i, err)
					return
				}
				acked.Add(1)
				time.Sleep(40 * time.Millisecond) // spread the records over the outage
			}
		}()
	}

	waitFor(t, 60*time.Second, "some records acked before the outage", func() bool { return acked.Load() >= 10 })
	stop(env, t)
	beforeOutage := acked.Load()
	if err := kafka.Ping(context.Background(), env.Brokers); err == nil {
		t.Error("Ping succeeded while the broker was stopped")
	}
	// Bounded wait: let producers hit the dead broker and start retrying.
	time.Sleep(3 * time.Second)
	t.Logf("acked before outage: %d; during the 3s outage: %d more", beforeOutage, acked.Load()-beforeOutage)
	env.Start(t)

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("a produce failed instead of riding out the restart: %v", err)
	}
	waitFor(t, 30*time.Second, "Ping after the restart", func() bool {
		return kafka.Ping(context.Background(), env.Brokers) == nil
	})

	// Read the topic back with a fresh group, to the end.
	var s sink
	r := startConsumer(t, kafka.ConsumerConfig{
		Brokers: env.Brokers, GroupID: uniq("g"), Topics: []string{topic}, MaxInFlight: 1,
		Handler: func(_ context.Context, rec kafka.Record) error { s.add(string(rec.Value)); return nil },
	})
	want := producers * perProducer
	waitFor(t, 60*time.Second, "the topic's records", func() bool { return len(s.snapshot()) >= want })
	// Bounded absence check: give a duplicate a moment to show up.
	time.Sleep(time.Second)
	_ = r.stop(t)

	got := s.snapshot()
	if len(got) != want {
		counts := uniqueSet(got)
		var dups []string
		for v, n := range counts {
			if n > 1 {
				dups = append(dups, fmt.Sprintf("%s x%d", v, n))
			}
		}
		sort.Strings(dups)
		t.Fatalf("topic holds %d records, want %d (%d distinct); duplicates: %v", len(got), want, len(counts), dups)
	}
	// Per partition the consumer sees offset order, so per key the sequence
	// must be 0..perProducer-1 with no gaps and no repeats.
	next := map[string]int{}
	for _, v := range got {
		key, seq, _ := strings.Cut(v, ":")
		n, _ := strconv.Atoi(seq)
		if n != next[key] {
			t.Fatalf("key %s: sequence %d where %d was expected (lost, duplicated or reordered)", key, n, next[key])
		}
		next[key]++
	}
}
