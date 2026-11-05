//go:build integration

// What this file checks, against real Postgres (RFC §18.2/§18.3): that the
// worker pool genuinely bounds concurrency rather than just having a
// "workerCount" field that's never actually enforced; that idempotency
// holds across retries — a handler that fails twice then succeeds still
// leaves exactly one processed_events row, not three; that giving up after
// max attempts actually stops a poison event from being retried forever;
// and that shutdown is genuinely graceful — in-flight work finishes rather
// than being abandoned mid-handler. Each of these is a claim relay.go's
// comments make; this file is what makes them claims-with-evidence rather
// than claims-with-vibes. Run with `make test-integration`.
package relay_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

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

// TestRelay_BoundedConcurrency_NeverExceedsWorkerCount is the test for
// SmartCourse.md §7's "Fixed-size worker pools" requirement: not that a
// worker count is configured, but that it's actually enforced. 20 events,
// 3 workers, a handler that holds its slot for 80ms — with real
// parallelism bounded correctly, the observed concurrent count should
// reach 3 but never exceed it.
func TestRelay_BoundedConcurrency_NeverExceedsWorkerCount(t *testing.T) {
	env := testenv.Setup(t)
	const workerCount = 3
	const eventCount = 20

	writeTestEvents(t, env.DB, "test.concurrency", eventCount)

	var current atomic.Int32
	var maxObserved atomic.Int32
	var processedCount atomic.Int32

	rl := relay.New(env.DB, relay.WithWorkerCount(workerCount), relay.WithPollInterval(50*time.Millisecond))
	rl.Register(relay.NewHandler("concurrency-test", []string{"test.concurrency"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		n := current.Add(1)
		for {
			m := maxObserved.Load()
			if n <= m || maxObserved.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		current.Add(-1)
		processedCount.Add(1)
		return nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()

	deadline := time.After(4 * time.Second)
	for processedCount.Load() < eventCount {
		select {
		case <-deadline:
			t.Fatalf("timed out: only %d/%d events processed", processedCount.Load(), eventCount)
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	<-done

	if got := maxObserved.Load(); got != workerCount {
		t.Fatalf("expected max concurrent handler executions to reach exactly the worker count (%d), got %d — either the pool isn't bounding correctly, or isn't achieving real parallelism", workerCount, got)
	}
}

// TestRelay_IdempotencyAcrossRetries proves RFC §6.6's rule end to end: a
// handler that fails twice before succeeding must still leave exactly ONE
// processed_events row, not one per attempt — the check-then-insert inside
// one transaction (runOne in relay.go) is what makes retries safe.
func TestRelay_IdempotencyAcrossRetries(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "test.flaky", 1)

	var attempts atomic.Int32
	var mu sync.Mutex
	succeeded := false

	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond), relay.WithMaxAttempts(10))
	rl.Register(relay.NewHandler("flaky-test", []string{"test.flaky"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		n := attempts.Add(1)
		if n < 3 {
			return errFlaky
		}
		mu.Lock()
		succeeded = true
		mu.Unlock()
		return nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	go rl.Run(ctx)

	deadline := time.After(2500 * time.Millisecond)
	for {
		mu.Lock()
		ok := succeeded
		mu.Unlock()
		if ok {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("handler never succeeded after %d attempts", attempts.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	time.Sleep(200 * time.Millisecond) // let the successful attempt's transaction land

	if attempts.Load() < 3 {
		t.Fatalf("expected at least 3 attempts (2 failures + 1 success), got %d", attempts.Load())
	}

	var processedEventsCount int64
	env.DB.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = 'flaky-test'`).Row().Scan(&processedEventsCount)
	if processedEventsCount != 1 {
		t.Fatalf("expected exactly 1 processed_events row despite %d attempts, got %d — idempotency check is not preventing duplicate effects across retries", attempts.Load(), processedEventsCount)
	}
}

// TestRelay_GivesUpAfterMaxAttempts proves the retry mechanism actually
// terminates for a poison event rather than consuming a worker slot on
// every poll forever (SmartCourse.md §7 — "Retry mechanisms" implies
// bounded retries, not infinite ones).
func TestRelay_GivesUpAfterMaxAttempts(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "test.poison", 1)

	var attempts atomic.Int32
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond), relay.WithMaxAttempts(3))
	rl.Register(relay.NewHandler("poison-test", []string{"test.poison"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		attempts.Add(1)
		return errFlaky
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	go rl.Run(ctx)

	deadline := time.After(2500 * time.Millisecond)
	for {
		_, _, givenUp := rl.Stats()
		if givenUp > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("relay never gave up on the poison event; attempts so far: %d", attempts.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	time.Sleep(150 * time.Millisecond)

	var unpublished int64
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = 'test.poison' AND published_at IS NULL`).Row().Scan(&unpublished)
	if unpublished != 0 {
		t.Fatal("expected the poison event to be marked published (given up) rather than retried forever")
	}
}

// TestRelay_GracefulShutdown_DrainsInFlightWork proves SmartCourse.md §7's
// "Graceful worker shutdown": cancelling the context must not abandon a
// handler mid-execution. The handler sleeps past the cancellation point;
// Run() must not return until it's genuinely finished.
func TestRelay_GracefulShutdown_DrainsInFlightWork(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "test.slow", 1)

	var finished atomic.Bool
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond))
	rl.Register(relay.NewHandler("slow-test", []string{"test.slow"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		time.Sleep(400 * time.Millisecond)
		finished.Store(true)
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()

	// Give the poller one tick to claim and dispatch the event, then
	// cancel while the handler is still sleeping.
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation — shutdown is not draining in-flight work within a reasonable time")
	}

	if !finished.Load() {
		t.Fatal("Run returned before the in-flight handler finished — shutdown abandoned in-flight work instead of draining it")
	}

	// The stronger check: the handler's Go code finishing is not the same
	// as its result being persisted. If the in-flight work's database
	// transaction is handed the already-cancelled shutdown context, the
	// handler runs to completion but its commit fails with "context
	// canceled" at the last moment — which looks graceful (finished ==
	// true) while silently losing the result. This is the check that
	// catches that: it caught a real bug during development, where
	// worker() passed the outer (cancelled) ctx straight through instead
	// of detaching it — see the comment on worker() in relay.go.
	var processedEventsCount int64
	env.DB.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = 'slow-test'`).Row().Scan(&processedEventsCount)
	if processedEventsCount != 1 {
		t.Fatalf("handler finished but its result wasn't persisted: expected 1 processed_events row, got %d", processedEventsCount)
	}
	var publishedAtIsNull bool
	env.DB.Raw(`SELECT published_at IS NULL FROM outbox WHERE event_type = 'test.slow'`).Row().Scan(&publishedAtIsNull)
	if publishedAtIsNull {
		t.Fatal("handler finished but the outbox row was never marked published")
	}
}

var errFlaky = &flakyError{}

type flakyError struct{}

func (*flakyError) Error() string { return "intentional test failure" }
