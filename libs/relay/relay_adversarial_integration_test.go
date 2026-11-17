//go:build integration

// Adversarial relay tests (ADR-0006 / ADR-0021). Each test attacks one
// claim in relay.go's comments: no loss, no double effect, retries bounded,
// shutdown clean, uncommitted events invisible. Everything synchronises on
// polling-with-a-deadline against real Postgres, never on fixed sleeps.
package relay_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// eventually polls cond until it is true or the deadline passes.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", d, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func countRows(t *testing.T, db *gorm.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.Raw(query, args...).Row().Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// startRelay runs rl until the returned stop func is called; stop blocks
// until Run has returned.
func startRelay(rl *relay.Relay) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

func createEffectsTable(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := db.Exec(`CREATE TABLE public.test_effects (
		consumer TEXT NOT NULL, event_id UUID NOT NULL, n SERIAL, PRIMARY KEY (consumer, event_id))`).Error; err != nil {
		t.Fatalf("create test_effects: %v", err)
	}
}

// effectHandler writes one row into test_effects inside the relay's own
// transaction: a duplicate effect would violate the primary key and make the
// handler fail, so "no double effect" is enforced by the schema, not by a
// counter the test could get wrong.
func effectHandler(consumer string, types ...string) relay.Handler {
	return relay.NewHandler(consumer, types, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		return tx.Exec(`INSERT INTO public.test_effects (consumer, event_id) VALUES (?, ?)`, consumer, evt.EventID).Error
	})
}

func TestRelayAdv_RolledBackAndUncommittedEventsAreNeverDelivered(t *testing.T) {
	env := testenv.Setup(t)

	var mu sync.Mutex
	var seen []string
	rl := relay.New(env.DB, relay.WithPollInterval(20*time.Millisecond))
	rl.Register(relay.NewHandler("rb", []string{"t.rb"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		var p struct{ Tag string }
		_ = jsonUnmarshal(evt.Payload, &p)
		mu.Lock()
		seen = append(seen, p.Tag)
		mu.Unlock()
		return nil
	}))
	got := func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), seen...) }

	write := func(tx *gorm.DB, tag string) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "t", AggregateID: uuid.New(), EventType: "t.rb", SchemaVersion: 1, Payload: map[string]any{"Tag": tag},
		})
	}

	// An open transaction holds an outbox row (lowest id) that is not yet committed.
	open := env.DB.Begin()
	if open.Error != nil {
		t.Fatal(open.Error)
	}
	if err := write(open, "uncommitted"); err != nil {
		t.Fatal(err)
	}
	// A transaction that writes then fails must leave nothing behind.
	_ = env.DB.Transaction(func(tx *gorm.DB) error {
		if err := write(tx, "rolled-back"); err != nil {
			return err
		}
		return fmt.Errorf("business rule failed after the event was written")
	})
	if err := env.DB.Transaction(func(tx *gorm.DB) error { return write(tx, "marker1") }); err != nil {
		t.Fatal(err)
	}

	stop := startRelay(rl)
	defer stop()

	eventually(t, 5*time.Second, "marker1 delivered", func() bool { return len(got()) >= 1 })
	for _, s := range got() {
		if s != "marker1" {
			t.Fatalf("relay delivered %q before/without commit", s)
		}
	}

	// Roll back the open transaction; its event must still never appear.
	if err := open.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Transaction(func(tx *gorm.DB) error { return write(tx, "marker2") }); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "marker2 delivered", func() bool { return len(got()) >= 2 })
	stop()

	if g := got(); len(g) != 2 || g[0] != "marker1" || g[1] != "marker2" {
		t.Fatalf("expected exactly [marker1 marker2], got %v", g)
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM outbox`); n != 2 {
		t.Fatalf("expected 2 outbox rows (rolled back / aborted writes must not persist), got %d", n)
	}
}

func TestRelayAdv_TwoRelaysOneDB_NoEventLostNoDoubleEffect(t *testing.T) {
	env := testenv.Setup(t)
	createEffectsTable(t, env.DB)
	const events = 200
	writeTestEvents(t, env.DB, "t.two", events)

	mk := func() *relay.Relay {
		rl := relay.New(env.DB, relay.WithPollInterval(15*time.Millisecond), relay.WithWorkerCount(4), relay.WithBatchSize(25))
		rl.Register(effectHandler("c1", "t.two"))
		rl.Register(effectHandler("c2", "t.two"))
		return rl
	}
	r1, r2 := mk(), mk()
	stop1, stop2 := startRelay(r1), startRelay(r2)

	eventually(t, 30*time.Second, "all outbox rows published", func() bool {
		return countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	stop1()
	stop2()

	// Each consumer applied each event exactly once, across both relays.
	for _, c := range []string{"c1", "c2"} {
		if n := countRows(t, env.DB, `SELECT count(DISTINCT event_id) FROM public.test_effects WHERE consumer = ?`, c); n != events {
			t.Fatalf("consumer %s: expected %d distinct effects, got %d (event lost)", c, events, n)
		}
		if n := countRows(t, env.DB, `SELECT count(*) FROM processed_events WHERE consumer_name = ?`, c); n != events {
			t.Fatalf("consumer %s: expected %d processed_events rows, got %d", c, events, n)
		}
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM public.test_effects`); n != 2*events {
		t.Fatalf("expected %d total effects, got %d", 2*events, n)
	}
	if _, _, g1 := r1.Stats(); g1 != 0 {
		t.Fatalf("relay 1 gave up on %d events", g1)
	}
	if _, _, g2 := r2.Stats(); g2 != 0 {
		t.Fatalf("relay 2 gave up on %d events", g2)
	}
}

func TestRelayAdv_PartialFailure_OtherConsumerNotReRun_AttemptsCounted(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "t.partial", 1)

	var aCalls, bCalls atomic.Int32
	var mu sync.Mutex
	var bAttemptsSeen []int
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(15*time.Millisecond), relay.WithMaxAttempts(10))
	rl.Register(relay.NewHandler("A", []string{"t.partial"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		aCalls.Add(1)
		return nil
	}))
	rl.Register(relay.NewHandler("B", []string{"t.partial"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		n := bCalls.Add(1)
		mu.Lock()
		bAttemptsSeen = append(bAttemptsSeen, evt.Attempts)
		mu.Unlock()
		if n <= 2 {
			return errFlaky
		}
		return nil
	}))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 10*time.Second, "event published after B recovers", func() bool {
		return countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`) == 1
	})
	stop()

	if aCalls.Load() != 1 {
		t.Fatalf("consumer A succeeded first time and must not be re-run on B's retries; A ran %d times", aCalls.Load())
	}
	if bCalls.Load() != 3 {
		t.Fatalf("B should run 3 times (fail, fail, succeed), ran %d", bCalls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(bAttemptsSeen) != "[0 1 2]" {
		t.Fatalf("DecodedEvent.Attempts should count prior failures [0 1 2], got %v", bAttemptsSeen)
	}
	if n := countRows(t, env.DB, `SELECT attempts FROM outbox`); n != 2 {
		t.Fatalf("persisted attempts should be 2, got %d", n)
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM processed_events`); n != 2 {
		t.Fatalf("expected one processed_events row per consumer (2), got %d", n)
	}
}

func TestRelayAdv_PoisonEventDoesNotBlockQueue_AndIsTriedExactlyMaxAttempts(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "t.poison2", 1) // lowest id: head of the queue
	writeTestEvents(t, env.DB, "t.good", 8)

	var poisonCalls, goodCalls atomic.Int32
	var mu sync.Mutex
	var attemptsSeen []int
	// batch size 1 + one worker is the worst case for head-of-line blocking.
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithBatchSize(1), relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(3))
	rl.Register(relay.NewHandler("poison", []string{"t.poison2"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		poisonCalls.Add(1)
		mu.Lock()
		attemptsSeen = append(attemptsSeen, evt.Attempts)
		mu.Unlock()
		return errFlaky
	}))
	rl.Register(relay.NewHandler("good", []string{"t.good"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		goodCalls.Add(1)
		return nil
	}))
	stop := startRelay(rl)
	defer stop()
	// Wait for the whole queue to drain, not just the good events: a failed
	// row cools down briefly before it is retried, so the good events can
	// finish before the poison event has used all of its attempts.
	eventually(t, 15*time.Second, "all good events processed and the poison event given up", func() bool {
		return goodCalls.Load() == 8 && countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	stop()

	if poisonCalls.Load() != 3 {
		t.Fatalf("poison event should be attempted exactly maxAttempts=3 times, got %d", poisonCalls.Load())
	}
	mu.Lock()
	if fmt.Sprint(attemptsSeen) != "[0 1 2]" {
		t.Fatalf("attempt numbers seen by handler: %v", attemptsSeen)
	}
	mu.Unlock()
	if _, _, g := rl.Stats(); g != 1 {
		t.Fatalf("givenUp should be 1, got %d", g)
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("expected the queue fully drained, %d rows still unpublished", n)
	}
}

// TestRelayAdv_CancelMidBatchThenRestart simulates two failure shapes at
// once: relay 1 is cancelled while a large batch is in progress, and its
// handlers' transactions roll back on a business error after the effect was
// written. Relay 2 then finishes the job. Every event must have exactly one
// committed effect.
func TestRelayAdv_CancelMidBatchThenRestart_ExactlyOneEffectPerEvent(t *testing.T) {
	env := testenv.Setup(t)
	createEffectsTable(t, env.DB)
	const events = 80
	writeTestEvents(t, env.DB, "t.crash", events)

	var started atomic.Int32
	flaky := relay.NewHandler("crashy", []string{"t.crash"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		n := started.Add(1)
		if err := tx.Exec(`INSERT INTO public.test_effects (consumer, event_id) VALUES ('crashy', ?)`, evt.EventID).Error; err != nil {
			return err
		}
		if n%3 == 0 {
			return errFlaky // effect written, then the handler "crashes": the tx must roll the effect back
		}
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	r1 := relay.New(env.DB, relay.WithPollInterval(10*time.Millisecond), relay.WithBatchSize(40), relay.WithWorkerCount(4), relay.WithMaxAttempts(100))
	r1.Register(flaky)
	stop1 := startRelay(r1)
	eventually(t, 10*time.Second, "relay 1 to be mid-batch", func() bool { return started.Load() >= 10 })
	stop1() // cancel mid-batch; Run drains in-flight work and returns

	published := countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`)
	if published >= events {
		t.Skip("relay 1 finished everything before being cancelled; nothing to resume")
	}

	r2 := relay.New(env.DB, relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(100))
	r2.Register(effectHandler("crashy", "t.crash"))
	stop2 := startRelay(r2)
	defer stop2()
	eventually(t, 20*time.Second, "restarted relay drains the queue", func() bool {
		return countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	stop2()

	if n := countRows(t, env.DB, `SELECT count(DISTINCT event_id) FROM public.test_effects WHERE consumer = 'crashy'`); n != events {
		t.Fatalf("expected %d events with an effect, got %d (lost events)", events, n)
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM public.test_effects WHERE consumer = 'crashy'`); n != events {
		t.Fatalf("expected exactly %d committed effects, got %d (duplicated effect)", events, n)
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM processed_events WHERE consumer_name = 'crashy'`); n != events {
		t.Fatalf("processed_events rows: want %d got %d", events, n)
	}
}

// Characterisation test: an event type nobody handles is marked published
// immediately. That means a consumer registered AFTER such an event was
// relayed will never see it (no replay). Documented as a limitation; this
// test pins the behaviour so a change is noticed.
func TestRelayAdv_UnhandledEventTypeIsMarkedPublished_NoReplayForLateConsumers(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "nobody.handles.this", 3)

	rl := relay.New(env.DB, relay.WithPollInterval(10*time.Millisecond))
	rl.Register(effectNoop("other", "something.else"))
	stop := startRelay(rl)
	eventually(t, 5*time.Second, "unhandled events marked published", func() bool {
		return countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	stop()

	var late atomic.Int32
	rl2 := relay.New(env.DB, relay.WithPollInterval(10*time.Millisecond))
	rl2.Register(relay.NewHandler("late", []string{"nobody.handles.this"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
		late.Add(1)
		return nil
	}))
	stop2 := startRelay(rl2)
	// Prove rl2 is polling by delivering a fresh event.
	writeTestEvents(t, env.DB, "nobody.handles.this", 1)
	eventually(t, 5*time.Second, "fresh event delivered to the late consumer", func() bool { return late.Load() >= 1 })
	stop2()
	if late.Load() != 1 {
		t.Fatalf("late consumer should only see the event written after it started (1), saw %d", late.Load())
	}
}

func effectNoop(name string, types ...string) relay.Handler {
	return relay.NewHandler(name, types, func(context.Context, *gorm.DB, relay.DecodedEvent) error { return nil })
}

func TestRelayAdv_LargeAndBinaryPayloadsSurviveIntact(t *testing.T) {
	env := testenv.Setup(t)

	big := bytes.Repeat([]byte("0123456789abcdef"), 512*1024) // 8 MiB
	bigHash := sha256.Sum256(big)
	// outbox.Write marshals Payload as JSON; a string of 8 MiB round-trips
	// through json.Marshal, which escapes nothing here.
	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: "t", AggregateID: uuid.New(), EventType: "t.big", SchemaVersion: 1, Payload: string(big),
		})
	}); err != nil {
		t.Fatalf("write 8 MiB payload: %v", err)
	}
	// A raw, non-JSON, NUL-containing payload inserted below the writer API.
	raw := []byte{0x00, 0xff, 0xfe, 0x00, 'x'}
	if err := env.DB.Exec(`INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload)
		VALUES (?, 't', ?, 't.raw', 1, ?)`, uuid.New(), uuid.New(), raw).Error; err != nil {
		t.Fatalf("insert raw payload: %v", err)
	}

	var mu sync.Mutex
	got := map[string][]byte{}
	rl := relay.New(env.DB, relay.WithPollInterval(10*time.Millisecond))
	capture := func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		mu.Lock()
		got[evt.EventType] = append([]byte(nil), evt.Payload...)
		mu.Unlock()
		return nil
	}
	rl.Register(relay.NewHandler("cap", []string{"t.big", "t.raw"}, capture))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 20*time.Second, "both payloads delivered", func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 2 })
	stop()

	mu.Lock()
	defer mu.Unlock()
	var decoded string
	if err := jsonUnmarshal(got["t.big"], &decoded); err != nil {
		t.Fatalf("decode big payload: %v", err)
	}
	if sha256.Sum256([]byte(decoded)) != bigHash {
		t.Fatal("8 MiB payload was corrupted in transit")
	}
	if !bytes.Equal(got["t.raw"], raw) {
		t.Fatalf("raw bytes corrupted: %v", got["t.raw"])
	}
}

func TestRelayAdv_RunAndShutdownLeaveNoGoroutinesBehind(t *testing.T) {
	env := testenv.Setup(t)
	createEffectsTable(t, env.DB)
	// Warm the pool and anything lazily started so the baseline is honest.
	_ = countRows(t, env.DB, `SELECT 1`)
	baseline := goleak.IgnoreCurrent()

	for round := 0; round < 4; round++ {
		writeTestEvents(t, env.DB, "t.leak", 20)
		var done atomic.Int32
		rl := relay.New(env.DB, relay.WithPollInterval(5*time.Millisecond), relay.WithWorkerCount(6), relay.WithBatchSize(8))
		rl.Register(relay.NewHandler("leak", []string{"t.leak"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
			done.Add(1)
			return tx.Exec(`INSERT INTO public.test_effects (consumer, event_id) VALUES ('leak', ?)`, evt.EventID).Error
		}))
		stop := startRelay(rl)
		eventually(t, 10*time.Second, "round events processed", func() bool { return done.Load() >= 20 })
		stop()
	}
	if err := goleak.Find(baseline, goleak.IgnoreTopFunction("database/sql.(*DB).connectionCleaner")); err != nil {
		t.Fatalf("goroutines leaked after relay shutdown: %v", err)
	}
}

func TestRelayAdv_CancelledBeforeFirstTickReturnsPromptly(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "t.never", 5)
	rl := relay.New(env.DB, relay.WithPollInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run with an already-cancelled context did not return")
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 5 {
		t.Fatalf("nothing should have been processed, %d unpublished of 5", n)
	}
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
