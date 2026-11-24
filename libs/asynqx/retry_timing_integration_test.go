//go:build integration

// Retry timing as the worker configures it (E07 finding, Week 3): Servers built
// exactly as services/worker builds them, with NO DelayedTaskCheckInterval,
// must retry a failing task at the configured backoff (Base, doubling), to
// within the one-second resolution Asynq stores retry times at. Asynq only
// moves a due retry back to pending when its delayed-task check runs, every 5
// seconds by default, so before the fix every gap collapsed to about 5 seconds
// whatever the backoff said (1s, 2s and 4s all became ~5s).
package asynqx_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestServers_RetryGapsFollowTheBackoffWithDefaultCheckInterval(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)

	// The production shape: services/worker/cmd/worker/wire.go.
	ss, err := asynqx.NewServers(asynqx.ServersConfig{
		Redis: redis,
		Pools: map[string]int{asynqx.QueueCritical: 2},
		Backoff: asynqx.BackoffConfig{
			Base: time.Second, Max: 8 * time.Second, Jitter: asynqx.DefaultBackoff.Jitter,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var attempts []time.Time
	mustRegister(t, ss, "test:retry-timing", asynqx.QueueCritical, func(context.Context, []byte) error {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, time.Now())
		return errors.New("provider down")
	})
	c := newClient(t, redis)
	mustEnqueue(t, c, asynqx.Job{Type: "test:retry-timing", ID: "retry-timing", Queue: asynqx.QueueCritical, MaxRetry: 5})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ss.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	eventually(t, 40*time.Second, "four attempts", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 4
	})
	mu.Lock()
	defer mu.Unlock()
	// Requested delays 1s, 2s, 4s, shortened by up to 20% jitter, truncated to
	// whole seconds by Asynq, and noticed at the next check (1s at most once the
	// interval is tight). So each gap lies in [delay*0.8 - 1s, delay + 1.5s].
	for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		gap := attempts[i+1].Sub(attempts[i])
		lo, hi := time.Duration(float64(delay)*0.8)-time.Second, delay+1500*time.Millisecond
		t.Logf("gap before retry %d: %v (requested %v, allowed %v..%v)", i+1, gap.Round(10*time.Millisecond), delay, max(lo, 0), hi)
		if gap < lo || gap > hi {
			t.Errorf("gap before retry %d = %v, want within [%v, %v] of the %v backoff: the retry was not scheduled when the backoff says", i+1, gap, max(lo, 0), hi, delay)
		}
	}
}
