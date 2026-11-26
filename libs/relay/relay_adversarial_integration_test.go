//go:build integration

// Adversarial relay tests (ADR-0006 / ADR-0029). Each test attacks one claim in
// relay.go's comments: no loss, no double publish, retries bounded, shutdown
// clean, uncommitted events invisible. They run the publish path with a fake
// Publisher. Everything synchronises on polling-with-a-deadline against real
// Postgres, never on fixed sleeps.
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
// until Run has returned and may be called more than once.
func startRelay(rl *relay.Relay) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

// recorder is a Publisher that records every successful publish and can fail
// chosen calls.
type recorder struct {
	mu   sync.Mutex
	got  []relay.DecodedEvent
	fail func(call int, evt relay.DecodedEvent) error // nil = never fail
	n    int
}

func (r *recorder) Publish(_ context.Context, evt relay.DecodedEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n++
	if r.fail != nil {
		if err := r.fail(r.n, evt); err != nil {
			return err
		}
	}
	r.got = append(r.got, evt)
	return nil
}

func (r *recorder) published() []relay.DecodedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]relay.DecodedEvent(nil), r.got...)
}

func TestRelayAdv_RolledBackAndUncommittedEventsAreNeverPublished(t *testing.T) {
	env := setupCourse(t)

	rec := &recorder{}
	rl := relay.New(env.DB, rec, relay.WithPollInterval(20*time.Millisecond))
	tags := func() []string {
		var out []string
		for _, e := range rec.published() {
			var p struct{ Tag string }
			_ = json.Unmarshal(e.Payload, &p)
			out = append(out, p.Tag)
		}
		return out
	}
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

	eventually(t, 5*time.Second, "marker1 published", func() bool { return len(tags()) >= 1 })
	for _, s := range tags() {
		if s != "marker1" {
			t.Fatalf("relay published %q before/without commit", s)
		}
	}

	// Roll back the open transaction; its event must still never appear.
	if err := open.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Transaction(func(tx *gorm.DB) error { return write(tx, "marker2") }); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "marker2 published", func() bool { return len(tags()) >= 2 })
	stop()

	if g := tags(); len(g) != 2 || g[0] != "marker1" || g[1] != "marker2" {
		t.Fatalf("expected exactly [marker1 marker2], got %v", g)
	}
	if n := countRows(t, env.DB, `SELECT count(*) FROM outbox`); n != 2 {
		t.Fatalf("expected 2 outbox rows (rolled back / aborted writes must not persist), got %d", n)
	}
}

func TestRelayAdv_TwoRelaysOneDB_NoEventLostNoDoublePublish(t *testing.T) {
	env := setupCourse(t)
	const events = 200
	writeTestEvents(t, env.DB, "t.two", events)

	rec := &recorder{}
	mk := func() *relay.Relay {
		return relay.New(env.DB, rec, relay.WithPollInterval(15*time.Millisecond), relay.WithWorkerCount(4), relay.WithBatchSize(25))
	}
	r1, r2 := mk(), mk()
	stop1, stop2 := startRelay(r1), startRelay(r2)

	eventually(t, 30*time.Second, "all outbox rows published", func() bool { return unpublished(t, env.DB) == 0 })
	stop1()
	stop2()

	seen := map[uuid.UUID]int{}
	for _, e := range rec.published() {
		seen[e.EventID]++
	}
	if len(seen) != events {
		t.Fatalf("%d distinct events published, want %d (event lost)", len(seen), events)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s published %d times by two relays on one outbox", id, n)
		}
	}
	if _, _, g1 := r1.Stats(); g1 != 0 {
		t.Fatalf("relay 1 gave up on %d events", g1)
	}
	if _, _, g2 := r2.Stats(); g2 != 0 {
		t.Fatalf("relay 2 gave up on %d events", g2)
	}
}

func TestRelayAdv_PoisonRowDoesNotBlockQueue_AndIsTriedExactlyMaxAttempts(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "t.poison2", 1) // lowest id: head of the queue
	writeTestEvents(t, env.DB, "t.good", 8)

	var mu sync.Mutex
	var attemptsSeen []int
	var good atomic.Int32
	pub := publisherFunc(func(_ context.Context, evt relay.DecodedEvent) error {
		if evt.EventType == "t.poison2" {
			mu.Lock()
			attemptsSeen = append(attemptsSeen, evt.Attempts)
			mu.Unlock()
			return errFlaky
		}
		good.Add(1)
		return nil
	})
	// batch size 1 + one worker is the worst case for head-of-line blocking.
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithBatchSize(1), relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(3))
	stop := startRelay(rl)
	defer stop()
	// Wait for the whole queue to drain, not just the good events: a failed row
	// cools down briefly before it is retried, so the good events can finish
	// before the poison row has used all of its attempts.
	eventually(t, 15*time.Second, "all good events published and the poison row given up", func() bool {
		return good.Load() == 8 && unpublished(t, env.DB) == 0
	})
	stop()

	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(attemptsSeen) != "[0 1 2]" {
		t.Fatalf("poison row should be attempted exactly maxAttempts=3 times with Attempts [0 1 2], got %v", attemptsSeen)
	}
	if _, _, g := rl.Stats(); g != 1 {
		t.Fatalf("givenUp should be 1, got %d", g)
	}
}

// TestRelayAdv_CancelMidBatchThenRestart simulates two failure shapes at once:
// relay 1 is cancelled while a large batch is in progress and some of its
// publishes fail; relay 2 then finishes the job. Every event must have exactly
// one successful publish.
func TestRelayAdv_CancelMidBatchThenRestart_ExactlyOnePublishPerEvent(t *testing.T) {
	env := setupCourse(t)
	const events = 80
	writeTestEvents(t, env.DB, "t.crash", events)

	rec := &recorder{fail: func(call int, _ relay.DecodedEvent) error {
		if call%3 == 0 {
			return errFlaky
		}
		time.Sleep(2 * time.Millisecond)
		return nil
	}}
	r1 := relay.New(env.DB, rec, relay.WithPollInterval(10*time.Millisecond), relay.WithBatchSize(40), relay.WithWorkerCount(4), relay.WithMaxAttempts(100))
	stop1 := startRelay(r1)
	eventually(t, 10*time.Second, "relay 1 to be mid-batch", func() bool { return len(rec.published()) >= 10 })
	stop1() // cancel mid-batch; Run drains in-flight work and returns

	if unpublished(t, env.DB) == 0 {
		t.Skip("relay 1 finished everything before being cancelled; nothing to resume")
	}
	r2 := relay.New(env.DB, rec, relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(100))
	stop2 := startRelay(r2)
	defer stop2()
	eventually(t, 20*time.Second, "restarted relay drains the queue", func() bool { return unpublished(t, env.DB) == 0 })
	stop2()

	seen := map[uuid.UUID]int{}
	for _, e := range rec.published() {
		seen[e.EventID]++
	}
	if len(seen) != events {
		t.Fatalf("%d events published, want %d (lost events)", len(seen), events)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s published %d times", id, n)
		}
	}
}

func TestRelayAdv_LargeAndBinaryPayloadsSurviveIntact(t *testing.T) {
	env := setupCourse(t)

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

	rec := &recorder{}
	rl := relay.New(env.DB, rec, relay.WithPollInterval(10*time.Millisecond))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 20*time.Second, "both payloads published", func() bool { return len(rec.published()) == 2 })
	stop()

	got := map[string][]byte{}
	for _, e := range rec.published() {
		got[e.EventType] = e.Payload
	}
	var decoded string
	if err := json.Unmarshal(got["t.big"], &decoded); err != nil {
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
	env := setupCourse(t)
	// Warm the pool and anything lazily started so the baseline is honest.
	_ = countRows(t, env.DB, `SELECT 1`)
	baseline := goleak.IgnoreCurrent()

	for round := 0; round < 4; round++ {
		writeTestEvents(t, env.DB, "t.leak", 20)
		var done atomic.Int32
		pub := publisherFunc(func(context.Context, relay.DecodedEvent) error { done.Add(1); return nil })
		rl := relay.New(env.DB, pub, relay.WithPollInterval(5*time.Millisecond), relay.WithWorkerCount(6), relay.WithBatchSize(8))
		stop := startRelay(rl)
		eventually(t, 10*time.Second, "round events published", func() bool { return done.Load() >= 20 })
		stop()
	}
	if err := goleak.Find(baseline, goleak.IgnoreTopFunction("database/sql.(*DB).connectionCleaner")); err != nil {
		t.Fatalf("goroutines leaked after relay shutdown: %v", err)
	}
}

func TestRelayAdv_CancelledBeforeFirstTickReturnsPromptly(t *testing.T) {
	env := setupCourse(t)
	writeTestEvents(t, env.DB, "t.never", 5)
	rl := relay.New(env.DB, nopPublisher, relay.WithPollInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run with an already-cancelled context did not return")
	}
	if n := unpublished(t, env.DB); n != 5 {
		t.Fatalf("nothing should have been published, %d unpublished of 5", n)
	}
}
