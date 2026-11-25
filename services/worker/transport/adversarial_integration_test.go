//go:build integration && bugrepro

// Reproducers for behaviour defects found by the second adversarial pass
// (docs/plans/week-3-findings-behaviour.md). They fail today by design.
//
//	cd services/worker && go test -tags='integration bugrepro' -run Bugrepro ./transport/
package transport_test

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

// B-08: a row whose encoded record is larger than the broker accepts (here a
// 3 MiB name against the 1 MiB default) is classified by classifyProduceError.
// The client-side "record too large" error is not a *kerr.Error the function
// recognises, so the row is treated as a transient broker problem: it does not
// consume its attempt budget, is retried forever and, because publish mode
// holds back every later row of the same aggregate, blocks that aggregate for
// good. Expected: a record no retry can fix is parked within the budget and the
// aggregate's later events flow.
func TestBugrepro_OversizeRecord_BlocksItsAggregateForever(t *testing.T) {
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
