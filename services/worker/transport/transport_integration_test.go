//go:build integration

// The Kafka transport end to end (ADR-0029), against real Postgres and a real
// broker (the apache/kafka image docker-compose pins): outbox row -> relay ->
// encoded Envelope on a topic -> consumer group -> the registered handler in a
// relay.Deliver transaction -> offset commit. Each test builds its own broker,
// because the topics are fixed by RFC §6.5 and a second test's group would
// otherwise read the first test's records.
//
// Not covered: the real Schema Registry (a stand-in serves ids and versions),
// a Confluent-format consumer reading the bytes, more than one broker, TLS or
// SASL, throughput.
package transport_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport"
)

func seqs(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// (a) Per-aggregate order holds end to end: four relay workers and four
// in-flight handlers, rows written aggregate by aggregate (the case where
// unordered claiming would let two workers overtake each other), and every
// aggregate's effects are applied in the order its events were written.
func TestTransport_PerAggregateOrderEndToEnd(t *testing.T) {
	s := newStack(t)
	const aggs, perAgg = 8, 15
	ids := make([]uuid.UUID, aggs)
	for i := range ids {
		ids[i] = uuid.New()
		for n := 0; n < perAgg; n++ {
			s.write(ids[i], n)
		}
	}
	rl, _ := s.startRelay(s.publisher(10*time.Second), 4)
	s.startRunner(transport.RunnerConfig{MaxInFlight: 4}, effectHandler("order-consumer", nil))

	s.eventually(90*time.Second, "all effects applied", func() bool {
		return s.count(`SELECT count(*) FROM public.test_effects`) == aggs*perAgg
	})
	for _, id := range ids {
		if got := s.effectsFor("order-consumer", id); !equalInts(got, seqs(perAgg)) {
			t.Fatalf("aggregate %s applied out of order or incompletely: %v", id, got)
		}
	}
	if n := s.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("%d rows still unpublished", n)
	}
	if _, _, g := rl.Stats(); g != 0 {
		t.Fatalf("relay gave up on %d rows", g)
	}
}

// (b) Duplicate delivery produces one effect: the relay re-publishes a row
// (as it does after a crash between the broker's ack and its commit), and the
// same record is also produced directly a third time; the handler's effect and
// processed_events each exist once.
func TestTransport_DuplicateDeliveryProducesOneEffect(t *testing.T) {
	s := newStack(t)
	agg := uuid.New()
	s.write(agg, 0)
	_, stopRelay := s.startRelay(s.publisher(10*time.Second), 1)
	s.startRunner(transport.RunnerConfig{}, effectHandler("dup-consumer", nil))
	s.eventually(60*time.Second, "first delivery", func() bool { return s.count(`SELECT count(*) FROM public.test_effects`) == 1 })
	stopRelay()

	// Re-publish the same row through the real relay path.
	if err := s.env.DB.Exec(`UPDATE outbox SET published_at = NULL`).Error; err != nil {
		t.Fatal(err)
	}
	_, stopRelay2 := s.startRelay(s.publisher(10*time.Second), 1)
	s.eventually(30*time.Second, "row re-published", func() bool { return s.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0 })
	stopRelay2()

	// And once more, straight onto the topic, with the same bytes.
	var row relay.DecodedEvent
	var raw []byte
	if err := s.env.DB.Raw(`SELECT id AS outbox_id, event_id, aggregate_type, aggregate_id, event_type, payload, occurred_at FROM outbox`).Row().Scan(
		&row.OutboxID, &row.EventID, &row.AggregateType, &row.AggregateID, &row.EventType, &raw, &row.OccurredAt); err != nil {
		t.Fatal(err)
	}
	row.Payload = raw
	msg, err := transport.NewCodec(s.schemas).Encode(row)
	if err != nil {
		t.Fatal(err)
	}
	p, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: s.kafka.Brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Produce(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	// A marker event behind the duplicates (same key, so the same partition):
	// once it is applied, the duplicates ahead of it have been handled.
	s.write(agg, 1)
	_, stopRelay3 := s.startRelay(s.publisher(10*time.Second), 1)
	defer stopRelay3()
	s.eventually(60*time.Second, "marker applied after the duplicates", func() bool {
		return s.count(`SELECT count(*) FROM public.test_effects`) == 2
	})
	if got := s.effectsFor("dup-consumer", agg); !equalInts(got, []int{0, 1}) {
		t.Fatalf("effects = %v, want one each of 0 and 1", got)
	}
	if n := s.count(`SELECT count(*) FROM processed_events WHERE consumer_name = 'dup-consumer'`); n != 2 {
		t.Fatalf("processed_events rows = %d, want 2", n)
	}
}

// (c) A consumer cancelled in the middle of a handler loses nothing: the
// handler had written its effect but not committed, the shutdown deadline
// cancels it, the transaction rolls back, the offset stays uncommitted, and the
// next consumer of the group handles that record and everything after it, once.
func TestTransport_ConsumerKilledMidHandleResumesWithoutLoss(t *testing.T) {
	s := newStack(t)
	agg := uuid.New()
	const total = 12
	for n := 0; n < total; n++ {
		s.write(agg, n)
	}
	s.startRelay(s.publisher(10*time.Second), 2)

	inHandler := make(chan struct{})
	var once sync.Once
	blocking := effectHandler("kill-consumer", func(ctx context.Context, evt relay.DecodedEvent) error {
		if strings.Contains(string(evt.Payload), `"full_name":"5"`) || strings.Contains(string(evt.Payload), `"full_name": "5"`) {
			once.Do(func() { close(inHandler) })
			<-ctx.Done() // stuck mid-handle, after the effect insert, until the runner gives up on it
			return ctx.Err()
		}
		return nil
	})
	_, stop1 := s.startRunner(transport.RunnerConfig{ShutdownTimeout: time.Second}, blocking)
	select {
	case <-inHandler:
	case <-time.After(90 * time.Second):
		t.Fatal("the handler never reached event 5")
	}
	if n := s.count(`SELECT count(*) FROM public.test_effects`); n != 5 {
		t.Fatalf("before the kill %d effects are committed, want 5 (0..4)", n)
	}
	stop1() // cancels the stuck handler at the shutdown deadline

	if n := s.count(`SELECT count(*) FROM public.test_effects WHERE seq = 5`); n != 0 {
		t.Fatal("the killed handler's effect was committed")
	}
	s.startRunner(transport.RunnerConfig{}, effectHandler("kill-consumer", nil))
	s.eventually(90*time.Second, "the second consumer finishes the stream", func() bool {
		return s.count(`SELECT count(*) FROM public.test_effects`) == total
	})
	if got := s.effectsFor("kill-consumer", agg); !equalInts(got, seqs(total)) {
		t.Fatalf("effects = %v, want 0..%d once each and in order", got, total-1)
	}
}

// (d) The broker is stopped while the relay has a backlog and events keep being
// written, then started again. The backlog drains: no row is dropped or parked,
// no effect is lost, and every effect exists once (a duplicate would be a
// primary-key violation, which the handler's idempotency absorbs).
func TestTransport_BrokerStoppedThenStarted_BacklogDrains(t *testing.T) {
	s := newStack(t)
	const aggs, perAgg = 4, 20
	ids := make([]uuid.UUID, aggs)
	for i := range ids {
		ids[i] = uuid.New()
	}
	write := func(from, to int) {
		for _, id := range ids {
			for n := from; n < to; n++ {
				s.write(id, n)
			}
		}
	}
	write(0, 8)
	parked := &sink{}
	rl, _ := s.startRelay(s.publisher(3*time.Second), 3, relay.WithDeadLetterSink(parked), relay.WithMaxAttempts(3))
	s.startRunner(transport.RunnerConfig{Sink: parked}, effectHandler("outage-consumer", nil))
	s.eventually(90*time.Second, "some events delivered before the outage", func() bool {
		return s.count(`SELECT count(*) FROM public.test_effects`) >= 8
	})

	s.kafka.Stop(t)
	write(8, 20) // the backlog grows while the broker is down
	s.eventually(60*time.Second, "the relay to fail against the dead broker", func() bool {
		p, f, _ := rl.Stats()
		_ = p
		return f > 0
	})
	if n := s.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`); n == 0 {
		t.Fatal("rows were marked published while the broker was down")
	}
	s.kafka.Start(t)

	s.eventually(180*time.Second, "the backlog to drain", func() bool {
		return s.count(`SELECT count(*) FROM public.test_effects`) == aggs*perAgg
	})
	for _, id := range ids {
		got := s.effectsFor("outage-consumer", id)
		sort.Ints(got)
		if !equalInts(got, seqs(perAgg)) {
			t.Fatalf("aggregate %s: effects %v, want 0..%d once each", id, got, perAgg-1)
		}
	}
	if n := s.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("%d rows still unpublished", n)
	}
	if _, _, g := rl.Stats(); g != 0 || len(parked.parked()) != 0 {
		t.Fatalf("the outage dead-lettered events: givenUp=%d parked=%d", g, len(parked.parked()))
	}
	if a := s.count(`SELECT coalesce(max(attempts), 0) FROM outbox`); a != 0 {
		t.Fatalf("an outage consumed %d attempts of an event's budget", a)
	}
}

// (e) A poison message does not stall a healthy one behind it, and reaches the
// sink. Three shapes: a record that cannot be decoded, a record whose handler
// always fails, and (relay side) a row that cannot be encoded.
func TestTransport_PoisonMessagesDoNotStallThePartition(t *testing.T) {
	t.Run("undecodable and failing records on the consumer side", func(t *testing.T) {
		s := newStack(t)
		agg := uuid.New()
		parked := &sink{}
		p, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: s.kafka.Brokers})
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()

		// Same key => same partition, in this order: garbage, healthy 1, failing 2, healthy 3.
		if _, err := p.Produce(context.Background(), kafka.Message{Topic: topics.Identity, Key: []byte(agg.String()), Value: []byte("not an envelope")}); err != nil {
			t.Fatal(err)
		}
		s.write(agg, 1)
		s.write(agg, 2)
		s.write(agg, 3)
		s.startRelay(s.publisher(10*time.Second), 1)

		h := effectHandler("poison-consumer", func(_ context.Context, evt relay.DecodedEvent) error {
			if strings.Contains(string(evt.Payload), `"2"`) {
				return errors.New("handler always fails on 2")
			}
			return nil
		})
		runner, _ := s.startRunner(transport.RunnerConfig{Sink: parked}, h)

		s.eventually(90*time.Second, "the healthy records behind the poison ones", func() bool {
			got := s.effectsFor("poison-consumer", agg)
			return equalInts(got, []int{1, 3})
		})
		s.eventually(30*time.Second, "both poison records parked", func() bool { return len(parked.parked()) == 2 })

		var sawGarbage, sawFailing bool
		for _, d := range parked.parked() {
			if d.Consumer != "poison-consumer" || d.Attempts != 3 || d.Err == nil {
				t.Fatalf("dead letter = %+v", d)
			}
			switch {
			case strings.Contains(string(d.Event.Payload), "undecodable_record"):
				sawGarbage = true
				if d.Event.EventID == uuid.Nil {
					t.Error("the undecodable record was parked without a stable id")
				}
			case d.Event.EventType == effectType && strings.Contains(d.Err.Error(), "always fails"):
				sawFailing = true
			}
		}
		if !sawGarbage || !sawFailing {
			t.Fatalf("parked = %+v; want the undecodable and the failing record", parked.parked())
		}
		if runner.ParkFailures() != 0 {
			t.Fatalf("ParkFailures = %d", runner.ParkFailures())
		}
		// The failing record was not committed as processed: no effect, no marker.
		if n := s.count(`SELECT count(*) FROM public.test_effects WHERE seq = 2`); n != 0 {
			t.Fatal("the failing handler's effect was committed")
		}
	})

	t.Run("a sink that refuses is counted and the partition still moves", func(t *testing.T) {
		s := newStack(t)
		agg := uuid.New()
		parked := &sink{refuse: true}
		s.write(agg, 1)
		s.write(agg, 2)
		s.startRelay(s.publisher(10*time.Second), 1)
		h := effectHandler("refused-consumer", func(_ context.Context, evt relay.DecodedEvent) error {
			if strings.Contains(string(evt.Payload), `"1"`) {
				return errors.New("poison")
			}
			return nil
		})
		runner, _ := s.startRunner(transport.RunnerConfig{Sink: parked}, h)
		s.eventually(90*time.Second, "the record behind the unparkable one", func() bool {
			return equalInts(s.effectsFor("refused-consumer", agg), []int{2})
		})
		s.eventually(30*time.Second, "the failed park to be counted", func() bool { return runner.ParkFailures() == 1 })
		if parked.attempts() < 2 {
			t.Fatalf("the sink was tried %d times, want a retry", parked.attempts())
		}
	})

	t.Run("an unencodable row on the relay side is parked and blocks nothing", func(t *testing.T) {
		s := newStack(t)
		badAgg, goodAgg := uuid.New(), uuid.New()
		parked := &sink{}
		s.writeType(badAgg, "course.course_created", map[string]any{"course_id": badAgg}) // no schema for this type
		s.write(badAgg, 1)                                                                // same aggregate, behind it
		for n := 0; n < 5; n++ {
			s.write(goodAgg, n)
		}
		rl, _ := s.startRelay(s.publisher(10*time.Second), 2, relay.WithDeadLetterSink(parked), relay.WithMaxAttempts(3))
		s.startRunner(transport.RunnerConfig{}, effectHandler("relay-poison-consumer", nil))

		s.eventually(90*time.Second, "everything but the bad row delivered", func() bool {
			return s.count(`SELECT count(*) FROM public.test_effects`) == 6
		})
		s.eventually(30*time.Second, "the bad row parked and its row released", func() bool {
			return s.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0 && len(parked.parked()) == 1
		})
		d := parked.parked()[0]
		if d.Consumer != relay.RelayConsumer || d.Event.EventType != "course.course_created" || d.Attempts != 3 ||
			!errors.Is(d.Err, transport.ErrUnencodable) {
			t.Fatalf("dead letter = %+v", d)
		}
		if _, _, g := rl.Stats(); g != 1 {
			t.Fatalf("givenUp = %d, want 1", g)
		}
	})
}

// A killed process between the broker's ack and the relay's commit is the one
// duplicate source the relay itself creates; the consumer test above covers
// what happens to it. This pins the other half: the relay never marks a row
// published for a record the broker did not accept.
func TestTransport_RowStaysUnpublishedWhileTheBrokerIsUnreachable(t *testing.T) {
	s := newStack(t)
	s.kafka.Stop(t)
	agg := uuid.New()
	s.write(agg, 0)
	rl, stop := s.startRelay(s.publisher(2*time.Second), 1)
	s.eventually(60*time.Second, "a failed publish", func() bool { _, f, _ := rl.Stats(); return f >= 1 })
	if n := s.count(`SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`); n != 0 {
		t.Fatal("a row was marked published with no broker acknowledgement")
	}
	stop()
}
