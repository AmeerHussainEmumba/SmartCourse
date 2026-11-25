//go:build integration && bugrepro

// Reproducers for behaviour defects found by the second adversarial pass
// (docs/plans/week-3-findings-behaviour.md). They fail today by design.
//
//	cd libs/relay && go test -tags='integration bugrepro' -run Bugrepro .
package relay_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// B-09: a transient publish failure (the broker is down) records last_error on
// the row and the later successful publish does not clear it. outbox.Prune
// keeps every published row that has a last_error ("rare", its comment says),
// so after a broker outage every row that was tried while it lasted stays in
// the outbox forever: the table grows without bound and the 'outbox prune' job
// reports success. Expected: a row that was eventually published is pruned
// once it is old enough.
func TestBugrepro_RowsPublishedAfterATransientFailure_AreNeverPruned(t *testing.T) {
	env := testenv.Setup(t)
	const rows = 5
	for i := 0; i < rows; i++ {
		writeFor(t, env.DB, uuid.New(), "t.outage", i)
	}
	var mu sync.Mutex
	failedOnce := map[uuid.UUID]bool{}
	pub := publisherFunc(func(_ context.Context, evt relay.DecodedEvent) error {
		mu.Lock()
		defer mu.Unlock()
		if !failedOnce[evt.EventID] {
			failedOnce[evt.EventID] = true
			return relay.Transient(errors.New("broker unreachable"))
		}
		return nil
	})
	rl := relay.New(env.DB, relay.WithWorkerCount(2), relay.WithPollInterval(10*time.Millisecond),
		relay.WithPublishBackoff(50*time.Millisecond), relay.WithPublisher(pub))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 15*time.Second, "every row published", func() bool { return unpublished(t, env.DB) == 0 })
	stop()

	time.Sleep(50 * time.Millisecond)
	n, err := outbox.Prune(context.Background(), env.DB, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Fatalf("Prune removed %d of %d rows that were published (all after one transient failure); the rest keep a stale last_error and stay forever", n, rows)
	}
}
