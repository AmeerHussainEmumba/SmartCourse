//go:build integration

// Publish-only operation (docs/plans/microservices-split.md §4): each service's
// relay publishes its own outbox and runs no handler, because the consumers
// live in other processes (libs/eventbus). relay.New(db, WithPublisher(p)) with
// nothing registered must therefore be a complete, quiet mode of its own.
package relay_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestRelayPublishOnly_NoHandlersRegisteredPublishesEverythingInOrder(t *testing.T) {
	env := testenv.Setup(t)
	agg := uuid.New()
	for i := 0; i < 6; i++ {
		writeFor(t, env.DB, agg, "t.nobody_handles_this", i) // no handler exists for the type, here or anywhere in this process
	}

	var published []int64
	var hooks atomic.Int32
	pub := publisherFunc(func(_ context.Context, evt relay.DecodedEvent) error {
		published = append(published, evt.OutboxID) // one aggregate: publishes are serial
		return nil
	})
	rl := relay.New(env.DB, relay.WithWorkerCount(3), relay.WithPollInterval(5*time.Millisecond), relay.WithPublisher(pub),
		relay.WithHandlerHook(func(string, bool, error) { hooks.Add(1) }))
	if h := rl.Handlers(); len(h) != 0 {
		t.Fatalf("a publish-only relay has handlers: %v", h)
	}
	if !rl.Publishing() {
		t.Fatal("Publishing() is false with a publisher set")
	}
	stop := startRelay(rl)
	defer stop()
	eventually(t, 15*time.Second, "all rows published", func() bool { return unpublished(t, env.DB) == 0 })
	stop()

	if len(published) != 6 {
		t.Fatalf("published %d rows, want 6", len(published))
	}
	for i := 1; i < len(published); i++ {
		if published[i] < published[i-1] {
			t.Fatalf("published out of order: %v", published)
		}
	}
	if n := hooks.Load(); n != 0 {
		t.Fatalf("the handler hook fired %d times in publish-only mode", n)
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM processed_events`); n != 0 {
		t.Fatalf("publish-only relay wrote %d processed_events rows", n)
	}
	if p, f, g := rl.Stats(); p != 6 || f != 0 || g != 0 {
		t.Fatalf("stats processed=%d failed=%d givenUp=%d, want 6/0/0", p, f, g)
	}
}

// Giving up on a row needs no handler either: the dead letter is attributed to
// the relay itself (relay.RelayConsumer) and the row stays until it is parked.
func TestRelayPublishOnly_PoisonRowIsParkedAsTheRelaysOwn(t *testing.T) {
	env := testenv.Setup(t)
	writeFor(t, env.DB, uuid.New(), "t.poison", 0)

	var parked atomic.Pointer[relay.DeadLetter]
	sink := sinkFunc(func(_ context.Context, d relay.DeadLetter) error { parked.Store(&d); return nil })
	pub := publisherFunc(func(context.Context, relay.DecodedEvent) error { return errors.New("cannot encode this") })
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(5*time.Millisecond), relay.WithPublisher(pub),
		relay.WithDeadLetterSink(sink), relay.WithMaxAttempts(2))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 15*time.Second, "poison row parked and released", func() bool { return unpublished(t, env.DB) == 0 })

	d := parked.Load()
	if d == nil || d.Consumer != relay.RelayConsumer || d.Event.EventType != "t.poison" {
		t.Fatalf("parked = %+v, want a dead letter for consumer %q", d, relay.RelayConsumer)
	}
}
