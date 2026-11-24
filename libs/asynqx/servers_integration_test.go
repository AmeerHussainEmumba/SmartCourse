//go:build integration

// Servers against a real Redis: each queue's pool is bounded on its own, a
// failing task retries with backoff, a poison task is archived by Asynq (and
// not retried again), and shutdown lets in-flight tasks on every queue finish.
// Shares the helpers and the one-Redis-per-test layout of asynqx_integration_test.go.
package asynqx_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestServers(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)

	sub := func(name string, fn func(t *testing.T, redis asynqx.RedisOpt)) {
		t.Run(name, func(t *testing.T) {
			if err := env.RDB.FlushAll(context.Background()).Err(); err != nil {
				t.Fatalf("flush redis: %v", err)
			}
			baseline := goleak.IgnoreCurrent()
			t.Cleanup(func() {
				if err := goleak.Find(baseline); err != nil {
					t.Errorf("goroutines leaked: %v", err)
				}
			})
			fn(t, redis)
		})
	}
	sub("PoolsAreBoundedPerQueue", testServersPools)
	sub("RetryWithBackoffThenPoisonArchived", testServersPoison)
	sub("GracefulShutdownDrainsEveryQueue", testServersShutdown)
}

func newServers(t *testing.T, cfg asynqx.ServersConfig) *asynqx.Servers {
	t.Helper()
	if cfg.DelayedTaskCheckInterval == 0 {
		cfg.DelayedTaskCheckInterval = 50 * time.Millisecond
	}
	ss, err := asynqx.NewServers(cfg)
	if err != nil {
		t.Fatalf("NewServers: %v", err)
	}
	return ss
}

func mustRegister(t *testing.T, ss *asynqx.Servers, typ, queue string, fn func(context.Context, []byte) error) {
	t.Helper()
	if err := ss.Register(typ, queue, fn); err != nil {
		t.Fatalf("Register(%s): %v", typ, err)
	}
}

// runServers is runServer for Servers.
func runServers(t *testing.T, ss *asynqx.Servers) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- ss.Run(ctx) }()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() { cancel(); result = <-ch })
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// Critical gets a pool of 2 and low a pool of 1. Both are saturated with
// blocking tasks at the same moment: a single server of concurrency 3 would
// not tell them apart, and a shared pool would let one queue take the other's
// slots.
func testServersPools(t *testing.T, redis asynqx.RedisOpt) {
	const critPool, lowPool = 2, 1
	c := newClient(t, redis)
	insp := newInspector(t, redis)

	var critIn, lowIn, critMax, lowMax atomic.Int32
	track := func(in, max *atomic.Int32, release <-chan struct{}) func(context.Context, []byte) error {
		return func(ctx context.Context, _ []byte) error {
			n := in.Add(1)
			for {
				m := max.Load()
				if n <= m || max.CompareAndSwap(m, n) {
					break
				}
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			in.Add(-1)
			return nil
		}
	}
	release := make(chan struct{})
	ss := newServers(t, asynqx.ServersConfig{Redis: redis, Pools: map[string]int{asynqx.QueueCritical: critPool, asynqx.QueueLow: lowPool}})
	mustRegister(t, ss, "pool:crit", asynqx.QueueCritical, track(&critIn, &critMax, release))
	mustRegister(t, ss, "pool:low", asynqx.QueueLow, track(&lowIn, &lowMax, release))
	for i := range 6 {
		mustEnqueue(t, c, asynqx.Job{Type: "pool:crit", ID: fmt.Sprintf("crit-%d", i), Queue: asynqx.QueueCritical})
		mustEnqueue(t, c, asynqx.Job{Type: "pool:low", ID: fmt.Sprintf("low-%d", i), Queue: asynqx.QueueLow})
	}
	runServers(t, ss)

	eventually(t, 15*time.Second, "both pools saturated, rest pending", func() bool {
		cs, ls := stats(t, insp, asynqx.QueueCritical), stats(t, insp, asynqx.QueueLow)
		return cs.Active == critPool && cs.Pending == 6-critPool && ls.Active == lowPool && ls.Pending == 6-lowPool
	})
	close(release)
	eventually(t, 15*time.Second, "all tasks drained", func() bool {
		return stats(t, insp, asynqx.QueueCritical).Pending == 0 && stats(t, insp, asynqx.QueueLow).Pending == 0 &&
			critIn.Load() == 0 && lowIn.Load() == 0
	})
	if critMax.Load() != critPool || lowMax.Load() != lowPool {
		t.Fatalf("max in flight critical=%d low=%d, want exactly %d and %d", critMax.Load(), lowMax.Load(), critPool, lowPool)
	}
}

// A task that fails once then succeeds is retried after the configured delay.
// A poison task that always fails is attempted MaxRetry+1 times, reported to
// OnExhausted once, archived by Asynq, and then left alone.
func testServersPoison(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	insp := newInspector(t, redis)

	var mu sync.Mutex
	var flakyAt []time.Time
	var poison atomic.Int32
	var exhausted []asynqx.FailedTask
	ss := newServers(t, asynqx.ServersConfig{
		Redis:   redis,
		Backoff: asynqx.BackoffConfig{Base: time.Second, Max: time.Second},
		OnExhausted: func(_ context.Context, f asynqx.FailedTask) {
			mu.Lock()
			exhausted = append(exhausted, f)
			mu.Unlock()
		},
	})
	mustRegister(t, ss, "poison:flaky", asynqx.QueueDefault, func(context.Context, []byte) error {
		mu.Lock()
		defer mu.Unlock()
		flakyAt = append(flakyAt, time.Now())
		if len(flakyAt) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	mustRegister(t, ss, "poison:bad", asynqx.QueueLow, func(context.Context, []byte) error {
		poison.Add(1)
		return errors.New("always fails")
	})
	mustEnqueue(t, c, asynqx.Job{Type: "poison:flaky", ID: "flaky", Queue: asynqx.QueueDefault, MaxRetry: 3, Retention: time.Minute})
	mustEnqueue(t, c, asynqx.Job{Type: "poison:bad", ID: "bad", Queue: asynqx.QueueLow, MaxRetry: 2, Payload: []byte("p")})
	runServers(t, ss)

	eventually(t, 30*time.Second, "flaky task retried and completed", func() bool {
		return state(t, insp, asynqx.QueueDefault, "flaky") == "completed"
	})
	mu.Lock()
	if len(flakyAt) != 2 {
		t.Errorf("flaky attempts = %d, want 2", len(flakyAt))
	} else if g := flakyAt[1].Sub(flakyAt[0]); g < 500*time.Millisecond {
		// 1s backoff, stored with one-second resolution.
		t.Errorf("retry came %v after the failure, want it delayed by the backoff", g)
	}
	mu.Unlock()

	eventually(t, 30*time.Second, "poison task archived", func() bool {
		return state(t, insp, asynqx.QueueLow, "bad") == "archived"
	})
	// Bounded absence check: an archived task must not be attempted again.
	time.Sleep(3 * time.Second)
	if got := poison.Load(); got != 3 {
		t.Fatalf("poison attempts = %d, want 3 (first run + 2 retries)", got)
	}
	if s := stats(t, insp, asynqx.QueueLow); s.Archived != 1 || s.Retry != 0 || s.Pending != 0 {
		t.Fatalf("low queue = %+v, want one archived task and nothing pending or retrying", s)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(exhausted) != 1 || exhausted[0].Type != "poison:bad" || exhausted[0].Retried != 2 || string(exhausted[0].Payload) != "p" {
		t.Fatalf("OnExhausted calls = %+v, want exactly one for poison:bad after 2 retries", exhausted)
	}
	if ss.ExhaustedCount() != 1 {
		t.Fatalf("ExhaustedCount = %d", ss.ExhaustedCount())
	}
}

// Shutdown with a task in flight on critical and one on low: Run must not
// return until both have finished, and both tasks must end up completed.
func testServersShutdown(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	insp := newInspector(t, redis)

	started := make(chan string, 2)
	release := make(chan struct{})
	var finished atomic.Int32
	ss := newServers(t, asynqx.ServersConfig{Redis: redis, ShutdownTimeout: 20 * time.Second})
	for _, q := range []string{asynqx.QueueCritical, asynqx.QueueLow} {
		mustRegister(t, ss, "drain:"+q, q, func(context.Context, []byte) error {
			started <- q
			<-release
			finished.Add(1)
			return nil
		})
		mustEnqueue(t, c, asynqx.Job{Type: "drain:" + q, ID: "drain-" + q, Queue: q, Retention: time.Minute})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- ss.Run(ctx) }()

	for range 2 {
		select {
		case <-started:
		case <-time.After(15 * time.Second):
			t.Fatal("a handler never started")
		}
	}
	cancel()
	// Bounded absence check: with both handlers blocked, Run must not return.
	select {
	case err := <-runDone:
		t.Fatalf("Run returned (%v) while handlers were still in flight", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after the in-flight handlers finished")
	}
	if finished.Load() != 2 {
		t.Fatalf("finished = %d, want 2 before Run returned", finished.Load())
	}
	for _, q := range []string{asynqx.QueueCritical, asynqx.QueueLow} {
		if s := state(t, insp, q, "drain-"+q); s != "completed" {
			t.Errorf("task on %s = %s after drain, want completed", q, s)
		}
	}
}
