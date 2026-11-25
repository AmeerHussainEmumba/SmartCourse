//go:build integration

// Publish-only operation (docs/plans/microservices-split.md §4): each service's
// relay publishes its own outbox and runs no handler, because the consumers
// live in other processes (libs/eventbus). relay.New(db, p) takes no handler and
// must be a complete, quiet relay of its own.
package relay_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

func TestRelayPublishOnly_NoHandlersRegisteredPublishesEverythingInOrder(t *testing.T) {
	env := setupCourse(t)
	agg := uuid.New()
	for i := 0; i < 6; i++ {
		writeFor(t, env.DB, agg, "t.nobody_handles_this", i) // no handler exists for the type, here or anywhere in this process
	}

	var published []int64
	pub := publisherFunc(func(_ context.Context, evt relay.DecodedEvent) error {
		published = append(published, evt.OutboxID) // one aggregate: publishes are serial
		return nil
	})
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(3), relay.WithPollInterval(5*time.Millisecond))
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
	env := setupCourse(t)
	writeFor(t, env.DB, uuid.New(), "t.poison", 0)

	var parked atomic.Pointer[relay.DeadLetter]
	sink := sinkFunc(func(_ context.Context, d relay.DeadLetter) error { parked.Store(&d); return nil })
	pub := publisherFunc(func(context.Context, relay.DecodedEvent) error { return errors.New("cannot encode this") })
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(5*time.Millisecond),
		relay.WithDeadLetterSink(sink), relay.WithMaxAttempts(2))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 15*time.Second, "poison row parked and released", func() bool { return unpublished(t, env.DB) == 0 })

	d := parked.Load()
	if d == nil || d.Consumer != relay.RelayConsumer || d.Event.EventType != "t.poison" {
		t.Fatalf("parked = %+v, want a dead letter for consumer %q", d, relay.RelayConsumer)
	}
}
