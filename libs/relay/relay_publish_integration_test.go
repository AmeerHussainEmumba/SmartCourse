//go:build integration

// Publish mode and the dead-letter sink (ADR-0029, ADR-0031), against real
// Postgres with a fake Publisher and a fake sink, so each claim is checked
// without a broker: the row is marked published only after the Publisher
// returned nil; per-aggregate order holds with several workers; an outage does
// not use up an event's budget; a poison row does not hold other aggregates up
// and is parked before it is dropped; a sink that refuses keeps the row. The
// real Kafka versions are in libs/eventbus.
package relay_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

type publisherFunc func(ctx context.Context, evt relay.DecodedEvent) error

func (f publisherFunc) Publish(ctx context.Context, evt relay.DecodedEvent) error { return f(ctx, evt) }

type sinkFunc func(ctx context.Context, d relay.DeadLetter) error

func (f sinkFunc) Park(ctx context.Context, d relay.DeadLetter) error { return f(ctx, d) }

func writeFor(t *testing.T, db *gorm.DB, agg uuid.UUID, eventType string, i int) {
	t.Helper()
	err := db.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "test", AggregateID: agg, EventType: eventType, SchemaVersion: 1, Payload: map[string]any{"i": i},
		})
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
}

func unpublished(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	return countRows(t, db, `SELECT count(*) FROM outbox WHERE published_at IS NULL`)
}

// The row must stay unpublished while the Publisher has not returned (the
// broker has not acknowledged), and stay unpublished when it returns an error.
func TestRelayPublish_MarkedPublishedOnlyAfterPublisherReturnsNil(t *testing.T) {
	env := setupCourse(t)
	writeFor(t, env.DB, uuid.New(), "t.ack", 0)

	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	pub := publisherFunc(func(ctx context.Context, evt relay.DecodedEvent) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return errors.New("broker said no") // the ack never came
		}
		return nil
	})
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond),
		relay.WithPublisher(pub), relay.WithMaxAttempts(5))
	stop := startRelay(rl)
	defer stop()

	<-entered
	if n := unpublished(t, env.DB); n != 1 {
		t.Fatalf("row was marked published before the publisher returned (%d unpublished)", n)
	}
	close(release)
	eventually(t, 10*time.Second, "row published after the second, successful publish", func() bool { return unpublished(t, env.DB) == 0 })
	if c := calls.Load(); c != 2 {
		t.Fatalf("publisher calls = %d, want 2 (one failure, one success)", c)
	}
	if a := countRows(t, env.DB, `SELECT attempts FROM outbox`); a != 1 {
		t.Fatalf("attempts = %d, want 1 (the failed publish)", a)
	}
}

func TestRelayPublish_PerAggregateOrderHoldsWithManyWorkers(t *testing.T) {
	env := setupCourse(t)
	const aggs, perAgg = 6, 12
	ids := make([]uuid.UUID, aggs)
	for i := range ids {
		ids[i] = uuid.New()
	}
	// Contiguous per aggregate: with several workers, consecutive rows are
	// then rows of one aggregate, the case where unordered claiming overtakes.
	for _, a := range ids {
		for n := 0; n < perAgg; n++ {
			writeFor(t, env.DB, a, "t.order", n)
		}
	}

	var mu sync.Mutex
	last := map[uuid.UUID]int64{}
	var violations []string
	inFlight := map[uuid.UUID]int{}
	var total atomic.Int32
	pub := publisherFunc(func(ctx context.Context, evt relay.DecodedEvent) error {
		mu.Lock()
		inFlight[evt.AggregateID]++
		if inFlight[evt.AggregateID] > 1 {
			violations = append(violations, "two publishes of one aggregate at once")
		}
		if evt.OutboxID < last[evt.AggregateID] {
			violations = append(violations, fmt.Sprintf("outbox %d after %d", evt.OutboxID, last[evt.AggregateID]))
		}
		last[evt.AggregateID] = evt.OutboxID
		mu.Unlock()
		time.Sleep(5 * time.Millisecond) // widen the window for a second worker to overtake
		mu.Lock()
		inFlight[evt.AggregateID]--
		mu.Unlock()
		total.Add(1)
		return nil
	})
	rl := relay.New(env.DB, relay.WithWorkerCount(4), relay.WithPollInterval(5*time.Millisecond), relay.WithPublisher(pub))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 30*time.Second, "all rows published", func() bool { return unpublished(t, env.DB) == 0 })
	stop()

	mu.Lock()
	defer mu.Unlock()
	if len(violations) > 0 {
		t.Fatalf("ordering violated: %v", violations[:min(5, len(violations))])
	}
	if total.Load() != aggs*perAgg {
		t.Fatalf("published %d, want %d", total.Load(), aggs*perAgg)
	}
}

// An outage (Transient errors) must not count toward the budget: with a budget
// of 2 the row survives many failed publishes, is never parked, and goes out
// once the transport is back.
func TestRelayPublish_TransientFailuresDoNotConsumeTheBudget(t *testing.T) {
	env := setupCourse(t)
	writeFor(t, env.DB, uuid.New(), "t.outage", 0)

	var down atomic.Bool
	down.Store(true)
	var calls atomic.Int32
	var parked atomic.Int32
	pub := publisherFunc(func(ctx context.Context, evt relay.DecodedEvent) error {
		calls.Add(1)
		if down.Load() {
			return relay.Transient(errors.New("broker unreachable"))
		}
		return nil
	})
	sink := sinkFunc(func(context.Context, relay.DeadLetter) error { parked.Add(1); return nil })
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(5*time.Millisecond), relay.WithPublishBackoff(10*time.Millisecond),
		relay.WithPublisher(pub), relay.WithMaxAttempts(2), relay.WithDeadLetterSink(sink))
	stop := startRelay(rl)
	defer stop()

	eventually(t, 10*time.Second, "several failed publishes", func() bool { return calls.Load() >= 6 })
	if unpublished(t, env.DB) != 1 || parked.Load() != 0 {
		t.Fatalf("an outage dropped or parked the event (unpublished=%d parked=%d)", unpublished(t, env.DB), parked.Load())
	}
	if a := countRows(t, env.DB, `SELECT attempts FROM outbox`); a != 0 {
		t.Fatalf("attempts = %d after transient failures, want 0", a)
	}
	down.Store(false)
	eventually(t, 10*time.Second, "row published once the transport is back", func() bool { return unpublished(t, env.DB) == 0 })
	if parked.Load() != 0 {
		t.Fatalf("event was parked although the publish eventually succeeded")
	}
}

// A row that can never be published (not transient) is retried up to the
// budget, parked with the Publisher's error, then marked published. Rows of
// other aggregates are not held up while it burns its budget; later rows of the
// same aggregate wait for it, then go out in order.
func TestRelayPublish_PoisonRowIsParkedAndDoesNotBlockOthers(t *testing.T) {
	env := setupCourse(t)
	poisonAgg, otherAgg := uuid.New(), uuid.New()
	writeFor(t, env.DB, poisonAgg, "t.bad", 0)
	writeFor(t, env.DB, poisonAgg, "t.good", 1)
	for i := 0; i < 5; i++ {
		writeFor(t, env.DB, otherAgg, "t.good", i)
	}

	var mu sync.Mutex
	var order []string
	pub := publisherFunc(func(ctx context.Context, evt relay.DecodedEvent) error {
		if evt.EventType == "t.bad" {
			return errors.New("cannot encode")
		}
		mu.Lock()
		order = append(order, evt.AggregateID.String()[:4]+fmt.Sprint(evt.OutboxID))
		mu.Unlock()
		return nil
	})
	var dl []relay.DeadLetter
	sink := sinkFunc(func(_ context.Context, d relay.DeadLetter) error {
		mu.Lock()
		dl = append(dl, d)
		mu.Unlock()
		return nil
	})
	rl := relay.New(env.DB, relay.WithWorkerCount(2), relay.WithPollInterval(5*time.Millisecond),
		relay.WithPublisher(pub), relay.WithMaxAttempts(3), relay.WithDeadLetterSink(sink))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 15*time.Second, "everything drained", func() bool { return unpublished(t, env.DB) == 0 })
	stop()

	mu.Lock()
	defer mu.Unlock()
	if len(dl) != 1 {
		t.Fatalf("parked %d events, want 1", len(dl))
	}
	d := dl[0]
	if d.Event.EventType != "t.bad" || d.Consumer != relay.RelayConsumer || d.Attempts != 3 || d.Err == nil || d.Err.Error() != "cannot encode" {
		t.Fatalf("dead letter = %+v", d)
	}
	if len(order) != 6 {
		t.Fatalf("published %d good rows, want 6: %v", len(order), order)
	}
}

// ADR-0031's "park, never drop": the sink is called before the row is marked
// published, and a sink that refuses leaves the row unpublished (counted, and
// retried) until it accepts.
func TestRelayDeadLetter_ParkedBeforeDrop_RefusedParkKeepsTheRow(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "t.dl", 1)

	var accept atomic.Bool
	var parkCalls atomic.Int32
	var publishedAtPark atomic.Int64 = atomic.Int64{}
	publishedAtPark.Store(-1)
	sink := sinkFunc(func(ctx context.Context, d relay.DeadLetter) error {
		parkCalls.Add(1)
		// The relay's claim transaction still holds the row (uncommitted); what
		// a sink can observe is that the row is not yet published.
		var n int64
		if err := env.DB.Raw(`SELECT count(*) FROM outbox WHERE id = ? AND published_at IS NOT NULL`, d.Event.OutboxID).Row().Scan(&n); err == nil {
			publishedAtPark.Store(n)
		}
		if !accept.Load() {
			return errors.New("mongo down")
		}
		return nil
	})
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(5*time.Millisecond), relay.WithPublishBackoff(10*time.Millisecond),
		relay.WithMaxAttempts(2), relay.WithDeadLetterSink(sink))
	rl.Register(relay.NewHandler("dl-consumer", []string{"t.dl"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
		return errFlaky
	}))
	stop := startRelay(rl)
	defer stop()

	eventually(t, 10*time.Second, "sink refused at least twice", func() bool { return parkCalls.Load() >= 2 })
	if unpublished(t, env.DB) != 1 {
		t.Fatal("row was dropped while the sink refused it")
	}
	if rl.ParkFailures() < 2 {
		t.Fatalf("ParkFailures = %d, want >= 2", rl.ParkFailures())
	}
	if _, _, g := rl.Stats(); g != 0 {
		t.Fatalf("givenUp = %d while the park kept failing", g)
	}
	accept.Store(true)
	eventually(t, 10*time.Second, "row published after the sink accepted", func() bool { return unpublished(t, env.DB) == 0 })
	if v := publishedAtPark.Load(); v != 0 {
		t.Fatalf("row was already published when Park ran (saw %d)", v)
	}
	if _, _, g := rl.Stats(); g != 1 {
		t.Fatalf("givenUp = %d, want 1", g)
	}
}

// With no sink the relay keeps today's behaviour: log, keep last_error, drop.
func TestRelayDeadLetter_NoSinkStillDropsAfterBudget(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "t.nosink", 1)
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(5*time.Millisecond), relay.WithMaxAttempts(2))
	rl.Register(relay.NewHandler("c", []string{"t.nosink"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error { return errFlaky }))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 10*time.Second, "event given up", func() bool { return unpublished(t, env.DB) == 0 })
	if _, _, g := rl.Stats(); g != 1 {
		t.Fatalf("givenUp = %d, want 1", g)
	}
}

// In-process mode is untouched by the transport options: handlers run in the
// relay, one effect per event, nothing is published anywhere.
func TestRelayDeliver_IsIdempotentAndWritesProcessedEvents(t *testing.T) {
	env := setupCourse(t)
	createEffectsTable(t, env.Owner)
	h := effectHandler("deliver-test", "t.deliver")
	evt := relay.DecodedEvent{EventID: uuid.New(), EventType: "t.deliver", OutboxID: 1}
	for i := 0; i < 3; i++ {
		if err := relay.Deliver(context.Background(), env.DB, h, evt); err != nil {
			t.Fatalf("Deliver #%d: %v", i, err)
		}
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM test_effects`); n != 1 {
		t.Fatalf("effects = %d, want 1", n)
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM processed_events WHERE consumer_name = 'deliver-test'`); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1", n)
	}
	panicky := relay.NewHandler("panicky", []string{"t.deliver"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error { panic("boom") })
	if err := relay.Deliver(context.Background(), env.DB, panicky, evt); err == nil {
		t.Fatal("a handler panic must come back as an error")
	}
}
