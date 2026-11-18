package auth

import (
	"context"
	"errors"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

func gateCfg(n int, wait time.Duration) config.Argon2Config {
	c := testArgonCfg()
	c.MaxConcurrent = n
	c.QueueTimeout = wait
	return c
}

// Never more than MaxConcurrent holders at once, however many callers.
func TestArgonGate_NeverExceedsMaxConcurrent(t *testing.T) {
	defer goleak.VerifyNone(t)
	const slots, callers = 3, 100
	g := NewArgonGate(gateCfg(slots, 10*time.Second))

	var cur, peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := g.Acquire(context.Background())
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			defer release()
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			cur.Add(-1)
		}()
	}
	wg.Wait()
	if got := peak.Load(); got > slots {
		t.Fatalf("peak concurrent holders = %d, want <= %d", got, slots)
	}
	if g.InFlight() != 0 {
		t.Fatalf("slots leaked: %d", g.InFlight())
	}
}

// Cancelled waiters neither hold nor leak a slot, and leave no goroutine behind.
func TestArgonGate_CancelledWaitersLeakNothing(t *testing.T) {
	defer goleak.VerifyNone(t)
	g := NewArgonGate(gateCfg(2, 30*time.Second))

	var holders []func()
	for i := 0; i < 2; i++ {
		rel, err := g.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		holders = append(holders, rel)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := g.Acquire(ctx)
			if err == nil {
				rel()
			}
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond) // let them all queue
	cancel()
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context.Canceled", err)
		}
	}
	if g.InFlight() != 2 {
		t.Fatalf("in flight = %d after waiters cancelled, want exactly the 2 real holders", g.InFlight())
	}
	for _, rel := range holders {
		rel()
		rel() // idempotent
	}
	if g.InFlight() != 0 {
		t.Fatalf("slots leaked: %d", g.InFlight())
	}
	// A fresh caller gets a slot at once: nothing was consumed by the cancelled ones.
	rel, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rel()
}

func TestArgonGate_QueueTimeoutReturnsBusy(t *testing.T) {
	defer goleak.VerifyNone(t)
	g := NewArgonGate(gateCfg(1, 40*time.Millisecond))
	rel, _ := g.Acquire(context.Background())
	defer rel()
	start := time.Now()
	_, err := g.Acquire(context.Background())
	if !errors.Is(err, ErrArgonBusy) {
		t.Fatalf("err = %v, want ErrArgonBusy", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("waited %v, queue timeout should bound it", d)
	}
	if _, err := g.Acquire(canceledCtx()); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-cancelled ctx: err = %v", err)
	}
}

func canceledCtx() context.Context {
	c, cancel := context.WithCancel(context.Background())
	cancel()
	return c
}

// 200 concurrent real argon2 verifications with 2 slots stay under a heap
// ceiling that unbounded execution would exceed by an order of magnitude, and
// the correct password still verifies for every caller.
//
// Measurement: a sampler reads runtime.MemStats.HeapAlloc every 2ms and keeps
// the maximum; the ceiling is baseline + 4 x (slots x memory) + 64 MiB. The 4x
// allows for completed-but-not-yet-collected argon blocks (HeapAlloc counts
// garbage until the next GC cycle; GC percent is lowered to 25 to keep that
// small) and the 64 MiB for 200 goroutines and the test's own allocations.
// Unbounded, 200 callers each allocate 8 MiB up front (about 1.6 GiB) against
// a ceiling of about 130 MiB, an order of magnitude apart, so scheduling
// jitter cannot flip the result in either direction.
func TestArgonGate_HeapStaysBoundedUnder200Callers(t *testing.T) {
	defer goleak.VerifyNone(t)
	const (
		slots   = 2
		callers = 200
		memKiB  = 8 * 1024
	)
	cfg := config.Argon2Config{MemoryKiB: memKiB, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32, MaxConcurrent: slots, QueueTimeout: time.Minute}
	enc, err := HashPassword(cfg, "correct horse")
	if err != nil {
		t.Fatal(err)
	}
	g := NewArgonGate(cfg)

	defer debug.SetGCPercent(debug.SetGCPercent(25))
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	baseline := ms.HeapAlloc

	var peak atomic.Uint64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()

	var wg sync.WaitGroup
	var ok atomic.Int64
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			good, err := g.Verify(context.Background(), enc, "correct horse")
			if err != nil || !good {
				t.Errorf("Verify = %v, %v; correct credentials must succeed under load", good, err)
				return
			}
			ok.Add(1)
		}()
	}
	wg.Wait()
	close(stop)
	<-sampled

	if ok.Load() != callers {
		t.Fatalf("%d/%d verifications succeeded", ok.Load(), callers)
	}
	ceiling := baseline + 4*slots*memKiB*1024 + 64<<20
	t.Logf("baseline %d MiB, peak %d MiB, ceiling %d MiB", baseline>>20, peak.Load()>>20, ceiling>>20)
	if peak.Load() > ceiling {
		t.Fatalf("peak heap %d MiB exceeds ceiling %d MiB: argon2 concurrency is not bounded", peak.Load()>>20, ceiling>>20)
	}
}

func TestNewArgonGate_ZeroConfigGetsDefaults(t *testing.T) {
	g := NewArgonGate(config.Argon2Config{Parallelism: 2})
	if cap(g.slots) < 2 || g.wait != config.DefaultArgon2QueueTimeout {
		t.Fatalf("defaults not applied: cap=%d wait=%v", cap(g.slots), g.wait)
	}
}
