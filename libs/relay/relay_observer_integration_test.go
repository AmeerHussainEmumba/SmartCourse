//go:build integration

// The relay's observability seams (Observer, HandlerHook) and the event_id on
// its log lines, against real Postgres: the observer is told each delivery in
// the right mode and its context reaches the Publisher and the handlers; its
// finish function sees the outcome; a panicking observer does not stop the
// delivery; the hook separates processed from duplicate and failed; and the
// relay's logs carry event_id.
package relay_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

type obsKey struct{}

type finished struct{ err error }

// recObserver is an Observer that tags the context and records what it saw.
type recObserver struct {
	mu   sync.Mutex
	seen []relay.DecodedEvent
	done []finished
}

func (o *recObserver) observe(ctx context.Context, evt relay.DecodedEvent, mode string) (context.Context, func(error)) {
	o.mu.Lock()
	o.seen = append(o.seen, evt)
	o.mu.Unlock()
	return context.WithValue(ctx, obsKey{}, mode), func(err error) {
		o.mu.Lock()
		o.done = append(o.done, finished{err})
		o.mu.Unlock()
	}
}

func (o *recObserver) snapshot() ([]relay.DecodedEvent, []finished) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]relay.DecodedEvent(nil), o.seen...), append([]finished(nil), o.done...)
}

func TestRelayObserver_PublishModeContextReachesPublisher(t *testing.T) {
	env := setupCourse(t)
	writeFor(t, env.DB, uuid.New(), "t.obs", 0)

	obs := &recObserver{}
	got := make(chan any, 4)
	pub := publisherFunc(func(ctx context.Context, evt relay.DecodedEvent) error {
		got <- ctx.Value(obsKey{})
		return nil
	})
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond),
		relay.WithPublisher(pub), relay.WithObserver(obs.observe))
	stop := startRelay(rl)
	defer stop()

	eventually(t, 10*time.Second, "row published", func() bool { return unpublished(t, env.DB) == 0 })
	if v := <-got; v != relay.ModePublish {
		t.Errorf("the Publisher's context carried %v, want the observer's value for mode %q", v, relay.ModePublish)
	}
	seen, done := obs.snapshot()
	if len(seen) != 1 || seen[0].EventType != "t.obs" || len(done) != 1 || done[0].err != nil {
		t.Errorf("observer saw %v and finished %v; want one delivery of t.obs finishing with no error", seen, done)
	}
}

func TestRelayObserver_ProcessModeAndFailureOutcome(t *testing.T) {
	env := setupCourse(t)
	writeFor(t, env.DB, uuid.New(), "t.proc", 0)

	obs := &recObserver{}
	boom := errors.New("handler boom")
	ctxModes := make(chan any, 8)
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond),
		relay.WithMaxAttempts(2), relay.WithObserver(obs.observe))
	rl.Register(relay.NewHandler("c", []string{"t.proc"}, func(ctx context.Context, _ *gorm.DB, _ relay.DecodedEvent) error {
		ctxModes <- ctx.Value(obsKey{})
		return boom
	}))
	stop := startRelay(rl)
	defer stop()

	eventually(t, 10*time.Second, "row given up", func() bool { _, _, g := rl.Stats(); return g == 1 })
	if v := <-ctxModes; v != relay.ModeProcess {
		t.Errorf("the handler's context carried %v, want mode %q", v, relay.ModeProcess)
	}
	_, done := obs.snapshot()
	if len(done) != 2 {
		t.Fatalf("want one finish per attempt (2), got %d", len(done))
	}
	for _, d := range done {
		if !errors.Is(d.err, boom) || !strings.Contains(d.err.Error(), "c:") {
			t.Errorf("finish error = %v, want the handler's error naming consumer c", d.err)
		}
	}
}

func TestRelayObserver_PanickingObserverDoesNotStopDelivery(t *testing.T) {
	env := setupCourse(t)
	writeFor(t, env.DB, uuid.New(), "t.panic", 0)
	writeFor(t, env.DB, uuid.New(), "t.panic", 1)

	n := 0
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond),
		relay.WithObserver(func(ctx context.Context, _ relay.DecodedEvent, _ string) (context.Context, func(error)) {
			n++
			if n == 1 {
				panic("observer start")
			}
			return ctx, func(error) { panic("observer finish") }
		}))
	var handled sync.WaitGroup
	handled.Add(2)
	rl.Register(relay.NewHandler("c", []string{"t.panic"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
		handled.Done()
		return nil
	}))
	stop := startRelay(rl)
	defer stop()
	handled.Wait()
	eventually(t, 10*time.Second, "both rows published", func() bool { return unpublished(t, env.DB) == 0 })
}

func TestRelayHandlerHook_SeparatesProcessedDuplicateFailed(t *testing.T) {
	env := setupCourse(t)
	agg := uuid.New()
	writeFor(t, env.DB, agg, "t.hook", 0)

	type call struct {
		consumer  string
		duplicate bool
		failed    bool
	}
	var mu sync.Mutex
	var calls []call
	var failOnce sync.Once
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(5),
		relay.WithHandlerHook(func(consumer string, duplicate bool, err error) {
			mu.Lock()
			calls = append(calls, call{consumer, duplicate, err != nil})
			mu.Unlock()
		}))
	rl.Register(relay.NewHandler("good", []string{"t.hook"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error { return nil }))
	rl.Register(relay.NewHandler("flaky", []string{"t.hook"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
		var err error
		failOnce.Do(func() { err = errors.New("first attempt fails") })
		return err
	}))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 10*time.Second, "row published", func() bool { return unpublished(t, env.DB) == 0 })

	mu.Lock()
	defer mu.Unlock()
	// Attempt 1: good processed, flaky failed. Attempt 2: good is now a duplicate
	// (its marker committed), flaky processed.
	want := map[call]int{
		{"good", false, false}:  1,
		{"flaky", false, true}:  1,
		{"good", true, false}:   1,
		{"flaky", false, false}: 1,
	}
	got := map[call]int{}
	for _, c := range calls {
		got[c]++
	}
	for c, n := range want {
		if got[c] != n {
			t.Errorf("hook calls %+v = %d, want %d (all: %+v)", c, got[c], n, calls)
		}
	}
}

// The relay's own log lines about a row carry event_id (RFC §12.4).
func TestRelayLogs_CarryEventID(t *testing.T) {
	env := setupCourse(t)
	var buf syncLog
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	agg := uuid.New()
	writeFor(t, env.DB, agg, "t.log", 0)
	var eventID string
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(1))
	rl.Register(relay.NewHandler("c", []string{"t.log"}, func(_ context.Context, _ *gorm.DB, evt relay.DecodedEvent) error {
		eventID = evt.EventID.String()
		return errors.New("always fails")
	}))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 10*time.Second, "row given up", func() bool { _, _, g := rl.Stats(); return g == 1 })

	for _, msg := range []string{"relay handler failed", "relay: giving up on event after max attempts"} {
		line := buf.lineWith(msg)
		if line == "" {
			t.Fatalf("no log line %q in:\n%s", msg, buf.String())
		}
		if !strings.Contains(line, `"event_id":"`+eventID+`"`) {
			t.Errorf("log line %q lacks event_id %s: %s", msg, eventID, line)
		}
	}
}

type syncLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncLog) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncLog) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }
func (s *syncLog) lineWith(sub string) string {
	for _, l := range strings.Split(s.String(), "\n") {
		if strings.Contains(l, sub) {
			return l
		}
	}
	return ""
}
