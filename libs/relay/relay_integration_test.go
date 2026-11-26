//go:build integration

// What this file checks, against real Postgres (RFC §18.2/§18.3) with a fake
// Publisher: that the worker pool genuinely bounds concurrency rather than just
// having a "workerCount" field that is never enforced; that a publish that
// fails twice before succeeding is retried, marks the row published once and
// leaves no stale last_error; that giving up after max attempts actually stops a
// poison row from being retried forever; and that shutdown is genuinely
// graceful: an in-flight publish finishes and its result is persisted. Each of
// these is a claim relay.go's comments make. Run with `make test-integration`.
package relay_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// writeTestEvents writes n events, each on its own aggregate (so none holds
// another back under the per-aggregate order rule).
func writeTestEvents(t *testing.T, db *gorm.DB, eventType string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		err := db.Transaction(func(tx *gorm.DB) error {
			return outbox.Write(context.Background(), tx, outbox.Event{
				AggregateType: "test", AggregateID: uuid.New(), EventType: eventType,
				SchemaVersion: 1, Payload: map[string]any{"i": i},
			})
		})
		if err != nil {
			t.Fatalf("write test event %d: %v", i, err)
		}
	}
}

// nopPublisher accepts everything.
var nopPublisher = publisherFunc(func(context.Context, relay.DecodedEvent) error { return nil })

// TestRelay_BoundedConcurrency_NeverExceedsWorkerCount is the test for
// SmartCourse.md §7's "Fixed-size worker pools" requirement: not that a worker
// count is configured, but that it is actually enforced. 20 events, 3 workers,
// a publisher that holds its slot for 80ms: the observed concurrent count should
// reach 3 but never exceed it.
func TestRelay_BoundedConcurrency_NeverExceedsWorkerCount(t *testing.T) {
	env := setupCourse(t)
	const workerCount = 3
	const eventCount = 20

	writeTestEvents(t, env.DB, "test.concurrency", eventCount)

	var current, maxObserved, publishedCount atomic.Int32
	pub := publisherFunc(func(ctx context.Context, evt relay.DecodedEvent) error {
		n := current.Add(1)
		for {
			m := maxObserved.Load()
			if n <= m || maxObserved.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		current.Add(-1)
		publishedCount.Add(1)
		return nil
	})
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(workerCount), relay.WithPollInterval(50*time.Millisecond))
	stop := startRelay(rl)
	defer stop()

	eventually(t, 10*time.Second, "every event published", func() bool { return publishedCount.Load() >= eventCount })
	stop()

	if got := maxObserved.Load(); got != workerCount {
		t.Fatalf("expected max concurrent publishes to reach exactly the worker count (%d), got %d: either the pool isn't bounding correctly, or isn't achieving real parallelism", workerCount, got)
	}
}

// TestRelay_RetriesAcrossFailures_PublishesOnceAndClearsLastError: a publish
// that fails twice (not transient, so each failure counts an attempt) and then
// succeeds leaves the row published, with the failed attempts counted, exactly
// one successful publish, and last_error cleared (B-09).
func TestRelay_RetriesAcrossFailures_PublishesOnceAndClearsLastError(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "test.flaky", 1)

	var calls, successes atomic.Int32
	pub := publisherFunc(func(ctx context.Context, evt relay.DecodedEvent) error {
		if calls.Add(1) < 3 {
			return errFlaky
		}
		successes.Add(1)
		return nil
	})
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(20*time.Millisecond), relay.WithMaxAttempts(10))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 10*time.Second, "row published", func() bool { return unpublished(t, env.DB) == 0 })
	stop()

	if calls.Load() != 3 || successes.Load() != 1 {
		t.Fatalf("publisher calls=%d successes=%d, want 3 and 1", calls.Load(), successes.Load())
	}
	var attempts int
	var lastErr *string
	if err := env.DB.Raw(`SELECT attempts, last_error FROM outbox`).Row().Scan(&attempts, &lastErr); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (the two failures)", attempts)
	}
	if lastErr != nil {
		t.Fatalf("last_error = %q on a row that was published; it must be cleared", *lastErr)
	}
}

// TestRelay_GivesUpAfterMaxAttempts proves the retry mechanism actually
// terminates for a poison row rather than consuming a worker slot on every
// poll forever (SmartCourse.md §7: bounded retries, not infinite ones).
func TestRelay_GivesUpAfterMaxAttempts(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "test.poison", 1)

	var attempts atomic.Int32
	pub := publisherFunc(func(context.Context, relay.DecodedEvent) error { attempts.Add(1); return errFlaky })
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(20*time.Millisecond), relay.WithMaxAttempts(3))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 10*time.Second, "relay gives up", func() bool { _, _, g := rl.Stats(); return g > 0 })
	stop()

	if attempts.Load() != 3 {
		t.Fatalf("publisher called %d times, want exactly maxAttempts=3", attempts.Load())
	}
	if n := unpublished(t, env.DB); n != 0 {
		t.Fatal("expected the poison row to be marked published (given up) rather than retried forever")
	}
}

// TestRelay_GracefulShutdown_DrainsInFlightWork proves SmartCourse.md §7's
// "Graceful worker shutdown": cancelling the context must not abandon a publish
// mid-flight. The publisher sleeps past the cancellation point; Run() must not
// return until it is finished and the row is persisted as published.
func TestRelay_GracefulShutdown_DrainsInFlightWork(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "test.slow", 1)

	entered := make(chan struct{})
	var finished atomic.Bool
	pub := publisherFunc(func(context.Context, relay.DecodedEvent) error {
		close(entered)
		time.Sleep(400 * time.Millisecond)
		finished.Store(true)
		return nil
	})
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(20*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the publisher was never called")
	}
	cancel() // while the publish is still sleeping

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation: shutdown is not draining in-flight work within a reasonable time")
	}
	if !finished.Load() {
		t.Fatal("Run returned before the in-flight publish finished: shutdown abandoned in-flight work instead of draining it")
	}
	// The stronger check: the publish finishing is not the same as its result
	// being persisted. If the claim transaction were handed the already-cancelled
	// shutdown context, the commit would fail with "context canceled" at the last
	// moment and the row would be published twice after the restart (see worker()
	// in relay.go, which detaches the context for this reason).
	if n := unpublished(t, env.DB); n != 0 {
		t.Fatalf("publish finished but the outbox row was not marked published (%d unpublished)", n)
	}
}

var errFlaky = &flakyError{}

type flakyError struct{}

func (*flakyError) Error() string { return "intentional test failure" }
