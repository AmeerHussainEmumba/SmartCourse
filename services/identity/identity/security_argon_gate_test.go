//go:build integration

// Bounded argon2 concurrency (F-26 residue): a flood of logins must neither
// pin memory in proportion to its size nor starve correct credentials.
package identity_test

import (
	"fmt"
	"net/http"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

func floodIP(i int) string { return fmt.Sprintf("198.18.%d.%d", i/250, i%250+1) }

// 200 concurrent logins (150 correct, 50 for unknown accounts, which also run
// argon2 against the dummy hash) with 2 slots: every correct login succeeds
// and the heap stays under a ceiling that the unbounded service exceeds.
//
// Measurement: a sampler reads runtime.MemStats.HeapAlloc every 5ms and keeps
// the maximum. Ceiling = post-GC baseline + 4 x (slots x 16 MiB) + 64 MiB: the
// 4x covers finished-but-uncollected argon blocks (GC percent is lowered to
// 25) and the 64 MiB the request/goroutine overhead of 200 callers. Measured
// on the author's machine (-race): bounded peak about 73 MiB against a ceiling
// of 194 MiB; with the gate widened to 100000 slots (mutation check) peak was
// about 458 MiB. Both sides are therefore more than 2x from the ceiling.
// The theoretical unbounded figure (200 x 16 MiB) is not reached because
// hashes finish and are collected while others start, so do not read the
// ceiling as a tight bound; the libs/auth test (1.6 GiB unbounded) has the
// wider margin and the identity test proves the gate is actually wired in.
func TestSecurity_ArgonGate_200ConcurrentLogins_BoundedHeap_CorrectCredentialsSucceed(t *testing.T) {
	const (
		slots   = 2
		callers = 200
		memKiB  = 16 * 1024
	)
	cfg := config.Argon2Config{MemoryKiB: memKiB, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32, MaxConcurrent: slots, QueueTimeout: time.Minute}
	h := newHarnessWith(t, setupIdentity(t), cfg)

	const users = 10
	emails := make([]string, users)
	for i := range emails {
		emails[i] = uniq("gate")
		h.mustRegister(emails[i], "CorrectPass123!")
	}

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
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()

	var wg sync.WaitGroup
	var goodOK, unknownRejected atomic.Int64
	gate := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			if i%4 == 3 { // unknown account: dummy-hash comparison, answers 401
				rec := h.login(fmt.Sprintf("nobody-%d@example.com", i), "whatever-password", withIP(floodIP(i)))
				if rec.Code == http.StatusUnauthorized && errCode(t, rec) == "invalid_credentials" {
					unknownRejected.Add(1)
				} else {
					t.Errorf("unknown account login: %d %s", rec.Code, rec.Body.String())
				}
				return
			}
			rec := h.login(emails[i%users], "CorrectPass123!", withIP(floodIP(i)))
			if rec.Code != http.StatusOK {
				t.Errorf("correct credentials refused under load: %d %s", rec.Code, rec.Body.String())
				return
			}
			goodOK.Add(1)
		}(i)
	}
	close(gate)
	wg.Wait()
	close(stop)
	<-sampled

	if goodOK.Load() != 150 || unknownRejected.Load() != 50 {
		t.Fatalf("correct logins ok=%d (want 150), unknown rejected=%d (want 50)", goodOK.Load(), unknownRejected.Load())
	}
	ceiling := baseline + 4*slots*memKiB*1024 + 64<<20
	t.Logf("baseline %d MiB, peak heap %d MiB, ceiling %d MiB", baseline>>20, peak.Load()>>20, ceiling>>20)
	if peak.Load() > ceiling {
		t.Fatalf("peak heap %d MiB exceeds ceiling %d MiB: argon2 concurrency is not bounded", peak.Load()>>20, ceiling>>20)
	}
}

// When the queue timeout elapses the client gets the standard 503 shape (never
// a 500, never a hang), the refusal does not count as a failed login, and the
// service recovers as soon as the pressure ends (with a single slot, a leaked
// one would make the final login fail). Cancelled waiters are covered by the
// goleak tests in libs/auth.
func TestSecurity_ArgonGate_SaturationAnswers503_AndRecovers(t *testing.T) {
	// Heavy enough that 24 callers cannot drain through one slot in 10ms.
	cfg := config.Argon2Config{MemoryKiB: 32 * 1024, Iterations: 3, Parallelism: 1, SaltLength: 16, KeyLength: 32, MaxConcurrent: 1, QueueTimeout: 10 * time.Millisecond}
	h := newHarnessWith(t, setupIdentity(t), cfg)
	email := uniq("sat")
	h.mustRegister(email, "CorrectPass123!")

	const callers = 24
	var wg sync.WaitGroup
	var ok, busy atomic.Int64
	gate := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			// one account, distinct IPs (the route allows 20/min per IP): refusals
			// must not consume the per-account failure budget.
			rec := h.login(email, "CorrectPass123!", withIP(floodIP(i)))
			switch {
			case rec.Code == http.StatusOK:
				ok.Add(1)
			case rec.Code == http.StatusServiceUnavailable && errCode(t, rec) == "temporarily_unavailable":
				busy.Add(1)
			default:
				t.Errorf("unexpected response under saturation: %d %s", rec.Code, rec.Body.String())
			}
		}(i)
	}
	close(gate)
	wg.Wait()
	t.Logf("ok=%d busy(503)=%d", ok.Load(), busy.Load())
	if busy.Load() == 0 {
		t.Fatal("expected at least one 503 when callers exceed the slot and the queue timeout")
	}
	if ok.Load() == 0 {
		t.Fatal("expected at least one login to succeed")
	}
	rec := h.login(email, "CorrectPass123!", withIP("203.0.113.9"))
	if rec.Code != http.StatusOK {
		t.Fatalf("after the flood, correct credentials: %d %s (503s must not consume the failure budget)", rec.Code, rec.Body.String())
	}
}
