//go:build integration

// The relay's observability seam (Observer) and the event_id on its log lines,
// against real Postgres: the observer is told each delivery in publish mode and
// its context reaches the Publisher; its finish function sees the outcome; a
// panicking observer does not stop the delivery; and the relay's logs carry
// event_id.
package relay_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/google/uuid"
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
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond),
		relay.WithObserver(obs.observe))
	stop := startRelay(rl)
	defer stop()

	eventually(t, 10*time.Second, "row published", func() bool { return unpublished(t, env.DB) == 0 })
	if v := <-got; v != relay.ModePublish {
		t.Errorf("the Publisher's context carried %v, want the observer's value for mode %q", v, relay.ModePublish)
	}
	eventually(t, 5*time.Second, "observer finished", func() bool { _, done := obs.snapshot(); return len(done) == 1 })
	seen, done := obs.snapshot()
	if len(seen) != 1 || seen[0].EventType != "t.obs" || done[0].err != nil {
		t.Errorf("observer saw %v and finished %v; want one delivery of t.obs finishing with no error", seen, done)
	}
}

func TestRelayObserver_FailureOutcomeIsReportedPerAttempt(t *testing.T) {
	env := setupCourse(t)
	writeFor(t, env.DB, uuid.New(), "t.proc", 0)

	obs := &recObserver{}
	boom := errors.New("publish boom")
	pub := publisherFunc(func(context.Context, relay.DecodedEvent) error { return boom })
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond),
		relay.WithMaxAttempts(2), relay.WithObserver(obs.observe))
	stop := startRelay(rl)
	defer stop()

	eventually(t, 10*time.Second, "row given up", func() bool { _, _, g := rl.Stats(); return g == 1 })
	stop()
	_, done := obs.snapshot()
	if len(done) != 2 {
		t.Fatalf("want one finish per attempt (2), got %d", len(done))
	}
	for _, d := range done {
		if !errors.Is(d.err, boom) || !strings.Contains(d.err.Error(), relay.RelayConsumer+":") {
			t.Errorf("finish error = %v, want the publisher's error attributed to %q", d.err, relay.RelayConsumer)
		}
	}
}

func TestRelayObserver_PanickingObserverDoesNotStopDelivery(t *testing.T) {
	env := setupCourse(t)
	writeFor(t, env.DB, uuid.New(), "t.panic", 0)
	writeFor(t, env.DB, uuid.New(), "t.panic", 1)

	n := 0
	var published sync.WaitGroup
	published.Add(2)
	pub := publisherFunc(func(context.Context, relay.DecodedEvent) error { published.Done(); return nil })
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond),
		relay.WithObserver(func(ctx context.Context, _ relay.DecodedEvent, _ string) (context.Context, func(error)) {
			n++
			if n == 1 {
				panic("observer start")
			}
			return ctx, func(error) { panic("observer finish") }
		}))
	stop := startRelay(rl)
	defer stop()
	published.Wait()
	eventually(t, 10*time.Second, "both rows published", func() bool { return unpublished(t, env.DB) == 0 })
}

// The relay's own log lines about a row carry event_id (RFC §12.4).
func TestRelayLogs_CarryEventID(t *testing.T) {
	env := setupCourse(t)
	var buf syncLog
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	writeFor(t, env.DB, uuid.New(), "t.log", 0)
	var eventID atomic.Value
	pub := publisherFunc(func(_ context.Context, evt relay.DecodedEvent) error {
		eventID.Store(evt.EventID.String())
		return errors.New("always fails")
	})
	rl := relay.New(env.DB, pub, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(1))
	stop := startRelay(rl)
	defer stop()
	eventually(t, 10*time.Second, "row given up", func() bool { _, _, g := rl.Stats(); return g == 1 })
	stop()

	id, _ := eventID.Load().(string)
	for _, msg := range []string{"relay publish failed", "relay: giving up on event after max attempts"} {
		line := buf.lineWith(msg)
		if line == "" {
			t.Fatalf("no log line %q in:\n%s", msg, buf.String())
		}
		if !strings.Contains(line, `"event_id":"`+id+`"`) {
			t.Errorf("log line %q lacks event_id %s: %s", msg, id, line)
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
