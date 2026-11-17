//go:build integration

// Reproducers for REAL defects in libs/relay found by adversarial testing.
// Regression tests (originally reproducers; now fixed). Run
// with `go test -tags='integration bugrepro' -run Bugrepro ./...`.
package relay_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// ADR-0006 / relay.go poll(): "FOR UPDATE SKIP LOCKED lets multiple relay
// REPLICAS drain the outbox concurrently without double-processing a row".
// poll() runs the SELECT ... FOR UPDATE SKIP LOCKED inside a transaction that
// commits immediately, so the row lock is gone before any handler runs. A
// second relay replica therefore claims and runs the SAME event while the
// first is still inside its handler.
//
// The handler blocks the first invocation until a concurrent second
// invocation for the same event shows up (or 2s pass). If SKIP LOCKED worked
// as documented the second invocation can never happen.
func TestBugrepro_TwoRelays_SameEventRunsConcurrently(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "t.dup", 1)

	var inHandler atomic.Int32
	var maxConcurrent atomic.Int32
	secondSeen := make(chan struct{})
	var once sync.Once
	h := relay.NewHandler("dup", []string{"t.dup"}, func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		n := inHandler.Add(1)
		defer inHandler.Add(-1)
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
		rl := relay.New(env.DB, relay.WithPollInterval(20*time.Millisecond), relay.WithWorkerCount(1))
		rl.Register(h)
		return rl
	}
	r1, r2 := mk(), mk()
	stop1, stop2 := startRelay(r1), startRelay(r2)
	eventually(t, 15*time.Second, "event published", func() bool {
		return countRows(t, env.DB, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`) == 1
	})
	stop1()
	stop2()

	if maxConcurrent.Load() > 1 {
		t.Errorf("one outbox row was handled by %d relay replicas at the same time; SKIP LOCKED does not protect processing because the claim transaction commits before the handler runs", maxConcurrent.Load())
	}
	_, f1, g1 := r1.Stats()
	_, f2, g2 := r2.Stats()
	if f1+f2 != 0 || g1+g2 != 0 {
		t.Errorf("spurious failures from the duplicate delivery (processed_events unique violation): failed=%d givenUp=%d", f1+f2, g1+g2)
	}
}

// relay.go worker(): there is no recover() around process(). A panicking
// handler (nil map write, bad type assertion, ...) takes down the whole
// worker process - and with it every other consumer and the Temporal
// worker - and, because the row stays unpublished, crash-loops on restart.
// The test re-executes itself as a child process and requires the child to
// SURVIVE a panicking handler.
func TestBugrepro_HandlerPanicCrashesWholeProcess(t *testing.T) {
	if os.Getenv("RELAY_PANIC_CHILD") == "1" {
		env := testenv.Setup(t)
		writeTestEvents(t, env.DB, "t.panic", 1)
		rl := relay.New(env.DB, relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(1))
		rl.Register(relay.NewHandler("panicky", []string{"t.panic"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error {
			var m map[string]int
			m["boom"] = 1 // nil map write
			return nil
		}))
		stop := startRelay(rl)
		eventually(t, 10*time.Second, "panicking event given up", func() bool {
			_, _, g := rl.Stats()
			return g == 1
		})
		stop()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestBugrepro_HandlerPanicCrashesWholeProcess$", "-test.v", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), "RELAY_PANIC_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil && strings.Contains(string(out), "panic: assignment to entry in nil map") {
		t.Fatalf("a panic inside a relay handler killed the whole process instead of being recovered and counted as a failed delivery:\n%s", tail(string(out), 1500))
	}
	if err != nil {
		t.Fatalf("child failed for another reason: %v\n%s", err, tail(string(out), 1500))
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// outbox.last_error exists "for the relay's concern" but is never written.
// An event the relay gives up on is marked published with no trace of why in
// the row (no dead-letter store exists; ADR/relay.go admit that). The data
// needed to diagnose or manually replay it is only in a log line.
func TestBugrepro_GivenUpEventRecordsNoLastError(t *testing.T) {
	env := testenv.Setup(t)
	writeTestEvents(t, env.DB, "t.gaveup", 1)
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(10*time.Millisecond), relay.WithMaxAttempts(2))
	rl.Register(relay.NewHandler("gu", []string{"t.gaveup"}, func(context.Context, *gorm.DB, relay.DecodedEvent) error { return errFlaky }))
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
