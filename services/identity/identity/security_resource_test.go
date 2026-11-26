//go:build integration

// Resource-abuse checks (security tester, area G): does a flood of the most
// expensive endpoint (login/register -> argon2id, 64 MiB per hash with the
// production defaults) starve cheap endpoints, and how much memory does an
// unthrottled burst pin?
package identity_test

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

func TestSecurity_Resource_ArgonFlood_DoesNotStarveCheapEndpoints(t *testing.T) {
	h := newHarnessWith(t, setupIdentity(t), config.Argon2Config{MemoryKiB: 64 * 1024, Iterations: 3, Parallelism: 2, SaltLength: 16, KeyLength: 32})

	cheap := func() time.Duration {
		s := time.Now()
		rec := h.call("GET", "/api/v1/me", nil, withToken("not-a-jwt"))
		if rec.Code != 401 {
			t.Errorf("cheap endpoint: %d", rec.Code)
		}
		return time.Since(s)
	}
	var base []time.Duration
	for i := 0; i < 20; i++ {
		base = append(base, cheap())
	}

	const flood = 16
	var peak uint64
	stop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peak {
					peak = ms.HeapAlloc
				}
			}
		}
	}()

	var wg sync.WaitGroup
	gate := make(chan struct{})
	started := time.Now()
	for i := 0; i < flood; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			// each from its own IP and a distinct unknown email: neither limiter applies
			h.login(fmt.Sprintf("flood-%d@example.com", i), "irrelevant-password", withIP(fmt.Sprintf("198.18.0.%d", i+1)))
		}(i)
	}
	close(gate)
	time.Sleep(50 * time.Millisecond) // let the flood get going
	var during []time.Duration
	for i := 0; i < 20; i++ {
		during = append(during, cheap())
	}
	wg.Wait()
	close(stop)
	sampler.Wait()

	pct := func(d []time.Duration, p float64) time.Duration {
		c := append([]time.Duration(nil), d...)
		sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
		return c[int(float64(len(c)-1)*p)]
	}
	t.Logf("%d concurrent argon2id logins finished in %v; peak heap %d MiB", flood, time.Since(started), peak>>20)
	t.Logf("cheap endpoint p95: idle=%v  during flood=%v", pct(base, 0.95), pct(during, 0.95))
	if p := pct(during, 0.95); p > 3*time.Second {
		t.Fatalf("a login flood starves cheap endpoints: p95 %v", p)
	}
	// The only gate against concurrent hashing is the per-IP/per-account limiter, which
	// X-Forwarded-For rotation defeats (see TestBug_RateLimit_PerIPLimitBypassedBySpoofedForwardedFor);
	// there is no semaphore around argon2. Report the memory cost instead of asserting on it.
	if peak == 0 {
		t.Log("memory sampler saw no heap growth")
	}
}
