//go:build integration

// Regression guards from the second adversarial pass: attacks that HELD
// (docs/plans/week-3-findings-behaviour.md, "What held").
//
//	cd services/worker && go test -tags=integration -run TestAdversarial ./transport/
package eventbus_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

type collectSink struct {
	mu     sync.Mutex
	parked []relay.DeadLetter
}

func (c *collectSink) Park(_ context.Context, d relay.DeadLetter) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.parked = append(c.parked, d)
	return nil
}

func (c *collectSink) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.parked)
}

// The hypothesis was that a row whose encoded record is larger than the broker
// accepts (here a 3 MiB name against the 1 MiB default) escapes
// classifyProduceError, is treated as a transient broker problem, never uses its
// attempt budget and blocks its aggregate forever. It does not: the record is
// parked within the budget and the later event of the same aggregate flows.
func TestAdversarial_OversizeRecord_IsParkedAndDoesNotBlockItsAggregate(t *testing.T) {
	s := newStack(t)
	agg := uuid.New()
	s.writeType(agg, effectType, map[string]any{"user_id": agg, "full_name": strings.Repeat("x", 3<<20)})
	s.write(agg, 1) // a small, healthy event of the same aggregate behind it
	sink := &collectSink{}
	rl, _ := s.startRelay(s.publisher(5*time.Second), 2, relay.WithMaxAttempts(3), relay.WithDeadLetterSink(sink))

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) && s.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`) > 0 {
		time.Sleep(250 * time.Millisecond)
	}
	unpublished := s.count(`SELECT count(*) FROM outbox WHERE published_at IS NULL`)
	_, failed, given := rl.Stats()
	if unpublished != 0 || sink.n() != 1 {
		t.Fatalf("after 45s: %d rows still unpublished, %d parked, relay failed=%d given_up=%d; want the unfixable record parked and the healthy event behind it published",
			unpublished, sink.n(), failed, given)
	}
}
