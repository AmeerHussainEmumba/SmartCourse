//go:build integration

// What this checks, against a real Redis (RFC §18.2: no mocks for
// infrastructure): the behaviours the rest of Week 3 relies on. Dedupe by
// task ID, capped retry with backoff, the exhausted-retries hook firing once,
// concurrency that is really bounded, queues draining in the 6:3:1 proportion,
// a shutdown that drains and one that honours its deadline, panic recovery,
// and one enqueue per scheduler tick across two replicas.
//
// One Redis container is shared by the subtests (testenv.Setup also starts
// Postgres, which costs seconds); each subtest flushes Redis first. Nothing
// synchronises on a fixed sleep: tests poll to a deadline or block on channels.
// The only timed waits are bounded absence checks, labelled where used.
package asynqx_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

const taskType = "test:task"

func TestAsynqx(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)

	// sub runs fn as a subtest on a clean Redis, and fails it if goroutines
	// started by the test outlive it.
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

	sub("DedupeByTaskID", testDedupe)
	sub("RetryWithBackoffThenSuccess", testRetryBackoff)
	sub("ExhaustedRetriesInvokeHookOnce", testExhaustedOnce)
	sub("SkipRetryInvokesHookOnce", testSkipRetry)
	sub("ConcurrencyIsBounded", testBoundedConcurrency)
	sub("WeightedQueuesDrainInProportion", testWeightedQueues)
	sub("GracefulShutdownDrainsInFlight", testShutdownDrains)
	sub("ShutdownDeadlineCancelsAndRequeues", testShutdownDeadline)
	sub("PanicIsRecoveredAndRetried", testPanicRetried)
	sub("PanicExhaustsAsFailure", testPanicExhausts)
	sub("SchedulerReplicasEnqueueOncePerTick", testSchedulerDuplicate)
}

// ---- helpers ----------------------------------------------------------------

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for: %s", d, what)
		}
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond) // poll interval, not synchronisation
	}
}

func newClient(t *testing.T, redis asynqx.RedisOpt) *asynqx.Client {
	t.Helper()
	c := asynqx.NewClient(redis)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newInspector(t *testing.T, redis asynqx.RedisOpt) *asynqx.Inspector {
	t.Helper()
	i := asynqx.NewInspector(redis)
	t.Cleanup(func() { _ = i.Close() })
	return i
}

// runServer starts srv.Run in a goroutine. stop cancels and waits for Run's
// return value; it is also registered as cleanup (idempotent).
func runServer(t *testing.T, srv *asynqx.Server) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- srv.Run(ctx) }()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() { cancel(); result = <-ch })
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func newServer(t *testing.T, cfg asynqx.ServerConfig) *asynqx.Server {
	t.Helper()
	if cfg.DelayedTaskCheckInterval == 0 {
		cfg.DelayedTaskCheckInterval = 50 * time.Millisecond
	}
	srv, err := asynqx.NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

func mustEnqueue(t *testing.T, c *asynqx.Client, j asynqx.Job) asynqx.Result {
	t.Helper()
	res, err := c.Enqueue(context.Background(), j)
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", j.ID, err)
	}
	return res
}

func stats(t *testing.T, i *asynqx.Inspector, queue string) asynqx.QueueStats {
	t.Helper()
	s, err := i.Stats(queue)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	return s[0]
}

func state(t *testing.T, i *asynqx.Inspector, queue, id string) string {
	t.Helper()
	s, found, err := i.TaskState(queue, id)
	if err != nil {
		t.Fatalf("TaskState: %v", err)
	}
	if !found {
		return "absent"
	}
	return s
}

// ---- tests ------------------------------------------------------------------

func testDedupe(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	insp := newInspector(t, redis)
	id := asynqx.DeterministicID("notification", "evt-1", "alice@example.test")
	job := asynqx.Job{Type: taskType, ID: id, Queue: asynqx.QueueCritical, Retention: time.Minute}

	first := mustEnqueue(t, c, job)
	second := mustEnqueue(t, c, job)
	if first.Duplicate || !second.Duplicate {
		t.Fatalf("first.Duplicate=%v second.Duplicate=%v, want false/true", first.Duplicate, second.Duplicate)
	}
	if got := stats(t, insp, asynqx.QueueCritical).Pending; got != 1 {
		t.Fatalf("pending after two identical enqueues = %d, want 1", got)
	}
	other := mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: asynqx.DeterministicID("notification", "evt-2", "alice@example.test"), Queue: asynqx.QueueCritical})
	if other.Duplicate {
		t.Fatal("a different logical job must not be a duplicate")
	}

	var ran atomic.Int32
	srv := newServer(t, asynqx.ServerConfig{Redis: redis})
	srv.Handle(taskType, func(context.Context, *asynqx.Task) error { ran.Add(1); return nil })
	stop := runServer(t, srv)
	eventually(t, 15*time.Second, "both distinct jobs executed", func() bool { return ran.Load() == 2 })

	// With Retention the ID outlives completion, so a late duplicate is still a no-op.
	eventually(t, 5*time.Second, "completed task retained", func() bool { return state(t, insp, asynqx.QueueCritical, id) == "completed" })
	if late := mustEnqueue(t, c, job); !late.Duplicate {
		t.Fatal("duplicate enqueue after completion (within Retention) must be a no-op")
	}
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := ran.Load(); got != 2 {
		t.Fatalf("handler ran %d times, want 2 (one per logical job)", got)
	}
}

func testRetryBackoff(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	// Asynq stores the retry time with one-second resolution, so delays are
	// asserted with a one-second floor tolerance (below).
	backoff := asynqx.BackoffConfig{Base: 2 * time.Second, Max: 10 * time.Second}
	var mu sync.Mutex
	var ns []int
	var attempts []time.Time
	var exhausted atomic.Int32
	srv := newServer(t, asynqx.ServerConfig{
		Redis: redis,
		RetryDelay: func(n int, err error, task *asynqx.Task) time.Duration {
			mu.Lock()
			ns = append(ns, n)
			mu.Unlock()
			return backoff.Func()(n, err, task)
		},
		OnExhausted: func(context.Context, asynqx.FailedTask) { exhausted.Add(1) },
	})
	var succeeded atomic.Bool
	srv.Handle(taskType, func(context.Context, *asynqx.Task) error {
		mu.Lock()
		attempts = append(attempts, time.Now())
		n := len(attempts)
		mu.Unlock()
		if n < 3 {
			return errors.New("provider down")
		}
		succeeded.Store(true)
		return nil
	})
	mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: "retry-1", MaxRetry: 5})
	runServer(t, srv)
	eventually(t, 30*time.Second, "third attempt succeeds", succeeded.Load)

	mu.Lock()
	defer mu.Unlock()
	if len(ns) != 2 || ns[0] != 0 || ns[1] != 1 {
		t.Fatalf("delay func called with n=%v, want [0 1]", ns)
	}
	// Requested delays 2s then 4s; with second-truncation each gap is at least
	// delay-1s.
	if g := attempts[1].Sub(attempts[0]); g < time.Second {
		t.Errorf("gap before retry 1 = %v, want >= 1s (2s backoff, 1s truncation)", g)
	}
	if g := attempts[2].Sub(attempts[1]); g < 3*time.Second {
		t.Errorf("gap before retry 2 = %v, want >= 3s (4s backoff, 1s truncation)", g)
	}
	if exhausted.Load() != 0 {
		t.Error("OnExhausted must not fire for a task that eventually succeeds")
	}
}

func testExhaustedOnce(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	insp := newInspector(t, redis)
	var calls atomic.Int32
	var mu sync.Mutex
	var got []asynqx.FailedTask
	srv := newServer(t, asynqx.ServerConfig{
		Redis:   redis,
		Backoff: asynqx.BackoffConfig{Base: time.Second, Max: time.Second},
		OnExhausted: func(_ context.Context, f asynqx.FailedTask) {
			mu.Lock()
			got = append(got, f)
			mu.Unlock()
		},
	})
	boom := errors.New("provider rejected")
	srv.Handle(taskType, func(context.Context, *asynqx.Task) error { calls.Add(1); return boom })
	mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: "doomed", Payload: []byte(`{"to":"x"}`), MaxRetry: 2})
	runServer(t, srv)

	eventually(t, 30*time.Second, "task archived", func() bool { return state(t, insp, asynqx.QueueDefault, "doomed") == "archived" })
	mu.Lock()
	defer mu.Unlock()
	if calls.Load() != 3 {
		t.Errorf("attempts = %d, want 3 (first try + 2 retries)", calls.Load())
	}
	if len(got) != 1 {
		t.Fatalf("OnExhausted called %d times, want exactly 1", len(got))
	}
	f := got[0]
	if f.ID != "doomed" || f.Type != taskType || f.Queue != asynqx.QueueDefault || f.Retried != 2 || f.MaxRetry != 2 ||
		string(f.Payload) != `{"to":"x"}` || !errors.Is(f.Err, boom) {
		t.Errorf("FailedTask = %+v", f)
	}
	if srv.ExhaustedCount() != 1 {
		t.Errorf("ExhaustedCount = %d", srv.ExhaustedCount())
	}
}

func testSkipRetry(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	insp := newInspector(t, redis)
	var hook atomic.Int32
	var retried atomic.Int32
	retried.Store(-1)
	srv := newServer(t, asynqx.ServerConfig{
		Redis: redis,
		OnExhausted: func(_ context.Context, f asynqx.FailedTask) {
			retried.Store(int32(f.Retried))
			hook.Add(1)
		},
	})
	srv.Handle(taskType, func(context.Context, *asynqx.Task) error {
		return fmt.Errorf("invalid recipient: %w", asynqx.SkipRetry)
	})
	mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: "skip", MaxRetry: 9})
	runServer(t, srv)
	eventually(t, 15*time.Second, "task archived", func() bool { return state(t, insp, asynqx.QueueDefault, "skip") == "archived" })
	if hook.Load() != 1 || retried.Load() != 0 {
		t.Fatalf("hook calls=%d retried=%d, want 1 and 0", hook.Load(), retried.Load())
	}
}

func testBoundedConcurrency(t *testing.T, redis asynqx.RedisOpt) {
	const limit, total = 3, 40
	c := newClient(t, redis)
	insp := newInspector(t, redis)
	for i := range total {
		mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: fmt.Sprintf("c-%d", i)})
	}

	var inflight, maxSeen, done atomic.Int32
	release := make(chan struct{})
	srv := newServer(t, asynqx.ServerConfig{Redis: redis, Concurrency: limit})
	srv.Handle(taskType, func(ctx context.Context, _ *asynqx.Task) error {
		n := inflight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		for range 50 { // widen the overlap window once released
			runtime.Gosched()
		}
		inflight.Add(-1)
		done.Add(1)
		return nil
	})
	runServer(t, srv)

	// All workers saturated, and the server has pulled exactly `limit` tasks
	// out of the queue and no more, while 37 sit pending.
	eventually(t, 15*time.Second, "limit handlers in flight", func() bool { return inflight.Load() >= limit })
	eventually(t, 15*time.Second, "queue shows exactly limit active, rest pending", func() bool {
		s := stats(t, insp, asynqx.QueueDefault)
		return s.Active == limit && s.Pending == total-limit
	})
	close(release)
	eventually(t, 30*time.Second, "all tasks done", func() bool { return done.Load() == total })
	if got := maxSeen.Load(); got != limit {
		t.Fatalf("max in-flight = %d, want exactly %d", got, limit)
	}
}

// Tolerance. With concurrency 1, completion order equals dequeue order, and
// each dequeue picks the first non-empty queue from a weighted shuffle, so the
// share of the first N completions is binomial with p = weight/10. For N=500
// the standard deviation of a share is sqrt(p(1-p)/N): 0.022 (critical,
// p=.6), 0.020 (default, p=.3), 0.013 (low, p=.1). We allow +-0.10, +-0.10 and
// +-0.06, which is 4.5-5 sigma each, so a correct implementation fails about
// once in a hundred thousand runs, while still failing equal weights
// (0.33/0.33/0.33) and strict priority (1/0/0) by a wide margin. Every queue
// holds 600 tasks, so none runs dry within the first 500 dequeues.
func testWeightedQueues(t *testing.T, redis asynqx.RedisOpt) {
	const perQueue, sample = 600, 500
	c := newClient(t, redis)
	for _, q := range []string{asynqx.QueueCritical, asynqx.QueueDefault, asynqx.QueueLow} {
		for i := range perQueue {
			mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: fmt.Sprintf("%s-%d", q, i), Queue: q})
		}
	}

	var mu sync.Mutex
	counts := map[string]int{}
	var seen int
	reached := make(chan struct{})
	srv := newServer(t, asynqx.ServerConfig{Redis: redis, Concurrency: 1, ShutdownTimeout: 5 * time.Second})
	srv.Handle(taskType, func(ctx context.Context, _ *asynqx.Task) error {
		meta, _ := asynqx.MetaFrom(ctx)
		mu.Lock()
		defer mu.Unlock()
		if seen < sample {
			counts[meta.Queue]++
			seen++
			if seen == sample {
				close(reached)
			}
		}
		return nil
	})
	stop := runServer(t, srv)
	select {
	case <-reached:
	case <-time.After(60 * time.Second):
		t.Fatal("timed out collecting the sample")
	}
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for q, want := range map[string]struct{ share, tol float64 }{
		asynqx.QueueCritical: {0.6, 0.10},
		asynqx.QueueDefault:  {0.3, 0.10},
		asynqx.QueueLow:      {0.1, 0.06},
	} {
		got := float64(counts[q]) / sample
		if got < want.share-want.tol || got > want.share+want.tol {
			t.Errorf("queue %s share = %.3f, want %.2f +- %.2f (counts %v)", q, got, want.share, want.tol, counts)
		}
	}
}

func testShutdownDrains(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	insp := newInspector(t, redis)
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	srv := newServer(t, asynqx.ServerConfig{Redis: redis, ShutdownTimeout: 20 * time.Second})
	srv.Handle(taskType, func(context.Context, *asynqx.Task) error {
		close(started)
		<-release
		finished.Store(true)
		return nil
	})
	mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: "drain", Retention: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- srv.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()
	// Bounded absence check: with the handler still blocked, Run must not return.
	select {
	case err := <-runDone:
		t.Fatalf("Run returned (%v) while a handler was still in flight", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after the in-flight handler finished")
	}
	if !finished.Load() {
		t.Fatal("Run returned before the in-flight handler completed")
	}
	if s := state(t, insp, asynqx.QueueDefault, "drain"); s != "completed" {
		t.Fatalf("task state after drain = %s, want completed", s)
	}
}

func testShutdownDeadline(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	insp := newInspector(t, redis)
	started := make(chan struct{})
	sawCancel := make(chan struct{})
	srv := newServer(t, asynqx.ServerConfig{Redis: redis, ShutdownTimeout: 500 * time.Millisecond})
	srv.Handle(taskType, func(ctx context.Context, _ *asynqx.Task) error {
		close(started)
		<-ctx.Done() // a stuck job that only stops when told to
		close(sawCancel)
		return ctx.Err()
	})
	mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: "stuck"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}
	begin := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return within a bound after the shutdown deadline")
	}
	if d := time.Since(begin); d < 400*time.Millisecond {
		t.Errorf("Run returned after %v, before the 500ms drain deadline", d)
	}
	select {
	case <-sawCancel:
	default:
		t.Error("handler context was not cancelled at the deadline")
	}
	// The unfinished job goes back to the queue for another worker, not to the dead set.
	if s := state(t, insp, asynqx.QueueDefault, "stuck"); s != "pending" && s != "retry" {
		t.Errorf("interrupted task state = %s, want pending (or retry)", s)
	}
}

func testPanicRetried(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	var attempts, hook, other atomic.Int32
	srv := newServer(t, asynqx.ServerConfig{
		Redis:       redis,
		Backoff:     asynqx.BackoffConfig{Base: time.Second, Max: time.Second},
		OnExhausted: func(context.Context, asynqx.FailedTask) { hook.Add(1) },
	})
	srv.Handle(taskType, func(context.Context, *asynqx.Task) error {
		if attempts.Add(1) == 1 {
			panic("nil map write")
		}
		return nil
	})
	srv.Handle("test:other", func(context.Context, *asynqx.Task) error { other.Add(1); return nil })
	mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: "panicky", MaxRetry: 3})
	runServer(t, srv)

	eventually(t, 30*time.Second, "panicked task retried to success", func() bool { return attempts.Load() == 2 })
	// The worker survived: it still serves unrelated work.
	mustEnqueue(t, c, asynqx.Job{Type: "test:other", ID: "after-panic"})
	eventually(t, 15*time.Second, "worker still processing after a panic", func() bool { return other.Load() == 1 })
	if srv.PanicCount() != 1 {
		t.Errorf("PanicCount = %d, want 1", srv.PanicCount())
	}
	if hook.Load() != 0 {
		t.Errorf("OnExhausted fired %d times for a task that recovered", hook.Load())
	}
}

func testPanicExhausts(t *testing.T, redis asynqx.RedisOpt) {
	c := newClient(t, redis)
	insp := newInspector(t, redis)
	var hook atomic.Int32
	var gotErr atomic.Value
	srv := newServer(t, asynqx.ServerConfig{
		Redis: redis,
		OnExhausted: func(_ context.Context, f asynqx.FailedTask) {
			gotErr.Store(f.Err)
			hook.Add(1)
		},
	})
	srv.Handle(taskType, func(context.Context, *asynqx.Task) error { panic("always") })
	mustEnqueue(t, c, asynqx.Job{Type: taskType, ID: "always-panics", NoRetry: true})
	runServer(t, srv)
	eventually(t, 15*time.Second, "task archived", func() bool { return state(t, insp, asynqx.QueueDefault, "always-panics") == "archived" })
	if hook.Load() != 1 {
		t.Fatalf("OnExhausted calls = %d, want 1", hook.Load())
	}
	if err, _ := gotErr.Load().(error); !errors.Is(err, asynqx.ErrPanic) {
		t.Fatalf("hook error = %v, want ErrPanic", err)
	}
	if srv.PanicCount() != 1 {
		t.Errorf("PanicCount = %d, want 1", srv.PanicCount())
	}
}

// Two schedulers (two worker replicas) register the same every-minute entry.
// Cron fires on the wall-clock minute, so this waits for up to a minute. Both
// attempt the same tick; Redis must hold one task for it.
func testSchedulerDuplicate(t *testing.T, redis asynqx.RedisOpt) {
	insp := newInspector(t, redis)
	var mu sync.Mutex
	attempts := map[string]int{} // task ID -> attempts across both replicas
	var schedulers []*asynqx.Scheduler
	for range 2 {
		sc, err := asynqx.NewScheduler(asynqx.SchedulerConfig{
			Redis: redis,
			OnAttempt: func(name, id string) {
				if name != "analytics-reconcile" {
					t.Errorf("unexpected entry %q", name)
				}
				mu.Lock()
				attempts[id]++
				mu.Unlock()
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := sc.Register(asynqx.Entry{Name: "analytics-reconcile", Spec: "* * * * *", Type: taskType}); err != nil {
			t.Fatal(err)
		}
		schedulers = append(schedulers, sc)
	}
	for _, sc := range schedulers {
		if err := sc.Start(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, sc := range schedulers {
			sc.Shutdown()
		}
	})

	var tickID string
	eventually(t, 100*time.Second, "both replicas attempted the same tick", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for id, n := range attempts {
			if n >= 2 {
				tickID = id
				return true
			}
		}
		return false
	})
	mu.Lock()
	distinct := len(attempts)
	mu.Unlock()
	// OnAttempt runs before the enqueue, so the task may not be in Redis yet
	// at the moment both attempts are seen: wait for it rather than assume.
	eventually(t, 10*time.Second, "tick task enqueued", func() bool {
		return state(t, insp, asynqx.QueueLow, tickID) == "pending"
	})
	// One task per distinct tick seen, never one per replica.
	if got := stats(t, insp, asynqx.QueueLow).Pending; got != distinct {
		t.Fatalf("pending tasks = %d for %d distinct tick(s): replicas double-enqueued", got, distinct)
	}
	var dups int64
	for _, sc := range schedulers {
		dups += sc.DuplicateCount()
	}
	if dups < 1 {
		t.Errorf("DuplicateCount across replicas = %d, want >= 1", dups)
	}
}
