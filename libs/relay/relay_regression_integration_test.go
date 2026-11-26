//go:build integration

// Regression tests for defects in libs/relay found by adversarial testing and
// since fixed (F-04 claim lock held through delivery, F-05 panics recovered,
// F-28 last_error persisted). They began as bugrepro reproducers against the
// removed in-process path and now run on the publish path; test names are
// unchanged.
package relay_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// ADR-0006 / relay.go claimAndProcess(): "FOR UPDATE SKIP LOCKED lets multiple
// relay REPLICAS drain the outbox concurrently without double-processing a
// row". The claim transaction must keep the row lock until the row is marked
// published, or a second replica claims and publishes the SAME event while the
// first is still inside its Publisher.
//
// The publisher blocks the first invocation until a concurrent second
// invocation for the same event shows up (or 2s pass). If SKIP LOCKED works as
// documented the second invocation can never happen.
func TestBugrepro_TwoRelays_SameEventRunsConcurrently(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "t.dup", 1)

	var inPublish, maxConcurrent atomic.Int32
	secondSeen := make(chan struct{})
	var once sync.Once
	pub := publisherFunc(func(ctx context.Context, evt relay.DecodedEvent) error {
		n := inPublish.Add(1)
		defer inPublish.Add(-1)
		for {
			m := maxConcurrent.Load()
			if n <= m || maxConcurrent.CompareAndSwap(m, n) {
				break
			}
		}
		if n >= 2 {
			once.Do(func() { close(secondSeen) })
			return nil
		}
		select {
		case <-secondSeen:
		case <-time.After(2 * time.Second):
		}
		return nil
	})
	mk := func() *relay.Relay {
		return relay.New(env.DB, pub, relay.WithPollInterval(20*time.Millisecond), relay.WithWorkerCount(1))
	}
	r1, r2 := mk(), mk()
	stop1, stop2 := startRelay(r1), startRelay(r2)
	eventually(t, 15*time.Second, "event published", func() bool {
		return countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`) == 1
	})
	stop1()
	stop2()

	if maxConcurrent.Load() > 1 {
		t.Errorf("one outbox row was published by %d relay replicas at the same time; SKIP LOCKED does not protect delivery because the claim transaction released the row before the publish finished", maxConcurrent.Load())
	}
	_, f1, g1 := r1.Stats()
	_, f2, g2 := r2.Stats()
	if f1+f2 != 0 || g1+g2 != 0 {
		t.Errorf("spurious failures from the duplicate delivery: failed=%d givenUp=%d", f1+f2, g1+g2)
	}
}

// A panic inside the Publisher (nil map write, bad type assertion, ...) must not
// take down the whole worker process: it is recovered and counted as a failed
// attempt, and the row is given up on like any other poison row, so it cannot
// crash-loop the process on restart.
func TestBugrepro_PublisherPanicCrashesWholeProcess(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "t.panic", 1)
	pub := publisherFunc(func(context.Context, relay.DecodedEvent) error {
		var m map[string]int
		m["boom"] = 1 // nil map write
		return nil
	})
	rl := relay.New(env.DB, pub, relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(1))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 10*time.Second, "panicking event given up", func() bool {
		_, _, g := rl.Stats()
		return g == 1
	})
	stop()
	var lastError *string
	if err := env.DB.Raw(`SELECT last_error FROM outbox`).Row().Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if lastError == nil {
		t.Error("the panic was recovered but last_error does not record it")
	}
}

// outbox.last_error is "for the relay's concern": an event the relay gives up on
// is marked published, and the row must keep why (the data needed to diagnose or
// replay it must not live only in a log line).
func TestBugrepro_GivenUpEventRecordsNoLastError(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "t.gaveup", 1)
	pub := publisherFunc(func(context.Context, relay.DecodedEvent) error { return errFlaky })
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(2))
	stop := startRelay(rl)
	eventually(t, 10*time.Second, "give up", func() bool { _, _, g := rl.Stats(); return g == 1 })
	stop()

	var lastError *string
	var attempts int
	if err := env.DB.Raw(`SELECT last_error, attempts FROM outbox`).Row().Scan(&lastError, &attempts); err != nil {
		t.Fatal(err)
	}
	if lastError == nil {
		t.Errorf("event was given up (and marked published) but outbox.last_error is NULL; persisted attempts=%d (want 2)", attempts)
	}
}
