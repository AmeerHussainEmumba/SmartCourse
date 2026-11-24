package harness

import (
	"fmt"
	"testing"
	"time"
)

// Eventually polls cond until it returns nil or timeout passes. A scenario
// never sleeps for a fixed time as its only wait: it names the condition. On
// timeout the test fails with what was being waited for and the last error
// cond returned, which should state the value it saw ("outbox backlog 3, want
// 0"), so the failure reads as an observation, not as "timed out".
func Eventually(t testing.TB, timeout time.Duration, what string, cond func() error) {
	t.Helper()
	if err := WaitFor(timeout, cond); err != nil {
		t.Fatalf("timed out after %s waiting for %s: %v", timeout, what, err)
	}
}

// WaitFor is Eventually without the test: it returns the last error when the
// deadline passes. Use it where the caller wants to decide what failure means.
func WaitFor(timeout time.Duration, cond func() error) error {
	deadline := time.Now().Add(timeout)
	interval := 100 * time.Millisecond
	var last error
	for {
		if last = cond(); last == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(interval)
		if interval < time.Second {
			interval += interval / 2
		}
	}
}

// Never asserts that holds keeps returning nil for the whole window; the first
// error fails the test. It is a bounded negative check ("no second mail
// arrives") and the one place a scenario waits out a fixed window: pair it
// with an Eventually that proved the positive case first, so the window
// starts from a settled state.
func Never(t testing.TB, window time.Duration, what string, holds func() error) {
	t.Helper()
	deadline := time.Now().Add(window)
	for {
		if err := holds(); err != nil {
			t.Fatalf("%s did not hold: %v", what, err)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// Errf is fmt.Errorf under a name that reads well in condition functions.
func Errf(format string, args ...any) error { return fmt.Errorf(format, args...) }
