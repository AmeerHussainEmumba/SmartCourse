//go:build integration

// The retry topic (docs/plans/microservices-split.md §4.1) against real Kafka and
// Postgres: a consumer group also reads retry.<its name>, and a record from
// there goes through the same idempotent relay.Deliver path as any other. A
// service builds its consumers from its own handlers (no *relay.Relay) and its
// relay publishes with no handler registered.
package eventbus_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// storedEffectEvent is a parked-and-replayed event as the audit service would
// hold it: decoded payload, registry versions from the schemas.
func (s *stack) storedEffectEvent(agg uuid.UUID, seq int) (relay.DecodedEvent, uint32) {
	s.t.Helper()
	return relay.DecodedEvent{
		EventID: uuid.New(), AggregateType: "user", AggregateID: agg, EventType: effectType,
		SchemaVersion: int(s.schemas.PayloadVersions[effectType]),
		Payload:       []byte(fmt.Sprintf(`{"user_id":%q,"full_name":"%d"}`, agg, seq)),
		OccurredAt:    time.Now().UTC(),
	}, s.schemas.EnvelopeIDs[topics.Identity]
}

func TestConsumers_SubscribeToTheirRetryTopic(t *testing.T) {
	s := newStack(t)
	c, _ := s.startRunner(runnerCfg{}, effectHandler("retry-consumer", nil))
	got := c.Topics("retry-consumer")
	want := []string{topics.Identity, topics.Retry("retry-consumer")}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("subscribed to %v, want %v", got, want)
	}

	agg := uuid.New()
	evt, schemaID := s.storedEffectEvent(agg, 7)
	if err := s.publisher(10*time.Second).PublishRetry(context.Background(), "retry-consumer", schemaID, evt); err != nil {
		t.Fatalf("PublishRetry: %v", err)
	}
	s.eventually(60*time.Second, "the retry record to be handled", func() bool {
		return s.count(`SELECT count(*) FROM public.test_effects WHERE event_id = ?`, evt.EventID) == 1
	})
	// Through the idempotent path: the marker is written with the effect.
	if n := s.count(`SELECT count(*) FROM processed_events WHERE consumer_name = 'retry-consumer' AND event_id = ?`, evt.EventID); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1", n)
	}
}

// Replaying an event the consumer already processed (the second replay of the
// design's replay-after-partial-failure case, or a replay of something that was
// delivered normally) is absorbed: one effect.
func TestConsumers_RetryOfAnAlreadyProcessedEventIsAbsorbed(t *testing.T) {
	s := newStack(t)
	agg := uuid.New()
	s.write(agg, 0)
	s.startRelay(s.publisher(10*time.Second), 1)
	s.startRunner(runnerCfg{}, effectHandler("absorb-consumer", nil))
	s.eventually(60*time.Second, "normal delivery", func() bool { return s.count(`SELECT count(*) FROM public.test_effects`) == 1 })

	var row struct {
		EventID uuid.UUID
	}
	if err := s.env.DB.Raw(`SELECT event_id FROM outbox LIMIT 1`).Scan(&row).Error; err != nil {
		t.Fatal(err)
	}
	// Replay the same event twice through the retry topic, then a different one
	// behind them on the same key: once that is applied the replays have been
	// handled (one partition, in order).
	pub := s.publisher(10 * time.Second)
	replay, schemaID := s.storedEffectEvent(agg, 0)
	replay.EventID = row.EventID
	for i := 0; i < 2; i++ {
		if err := pub.PublishRetry(context.Background(), "absorb-consumer", schemaID, replay); err != nil {
			t.Fatalf("PublishRetry: %v", err)
		}
	}
	marker, _ := s.storedEffectEvent(agg, 1)
	if err := pub.PublishRetry(context.Background(), "absorb-consumer", schemaID, marker); err != nil {
		t.Fatalf("PublishRetry: %v", err)
	}
	s.eventually(60*time.Second, "the marker event to be applied", func() bool {
		return s.count(`SELECT count(*) FROM public.test_effects WHERE event_id = ?`, marker.EventID) == 1
	})
	if n := s.count(`SELECT count(*) FROM public.test_effects WHERE event_id = ?`, row.EventID); n != 1 {
		t.Fatalf("the replayed event has %d effects, want 1", n)
	}
}

// A service's relay only publishes: no handler registered, the rows reach Kafka
// and are marked published after the ack; its consumers are built from its own
// handlers, with no relay value involved.
func TestPublishOnlyRelayFeedsConsumersBuiltFromHandlers(t *testing.T) {
	s := newStack(t)
	agg := uuid.New()
	for n := 0; n < 5; n++ {
		s.write(agg, n)
	}
	s.startRelay(s.publisher(10*time.Second), 2)
	s.startRunner(runnerCfg{}, effectHandler("publish-only-consumer", nil))
	s.eventually(60*time.Second, "all events consumed", func() bool {
		return s.count(`SELECT count(*) FROM public.test_effects`) == 5
	})
	if got := s.effectsFor("publish-only-consumer", agg); !equalInts(got, seqs(5)) {
		t.Fatalf("effects %v, want 0..4 in order", got)
	}
	if n := s.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("%d rows unpublished", n)
	}
}
