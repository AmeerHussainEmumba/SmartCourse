//go:build integration

// Rate-limit abuse tests (security tester, area B): per-IP limits, account
// limits vs credential stuffing, key isolation, atomicity under concurrency
// and what happens when Redis is unavailable (ADR-0020).
package identity_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestSecurity_RateLimit_RegisterPerIP_BlocksSixth_AndRoutesDoNotShareBuckets(t *testing.T) {
	h := newHarness(t)
	ip := "203.0.113.50"
	for i := 1; i <= 5; i++ {
		rec := h.call("POST", "/api/v1/auth/register", map[string]any{"email": uniq("rl"), "password": "correct-horse-battery", "full_name": "R"}, withIP(ip))
		if rec.Code != 202 {
			t.Fatalf("request %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}
	rec := h.call("POST", "/api/v1/auth/register", map[string]any{"email": uniq("rl"), "password": "correct-horse-battery", "full_name": "R"}, withIP(ip))
	if rec.Code != 429 || errCode(t, rec) != "rate_limited" {
		t.Fatalf("6th register from one IP: %d %s", rec.Code, rec.Body.String())
	}
	assertNoInternalLeak(t, "429", rec.Body.String())
	// A different IP is unaffected.
	if rec := h.call("POST", "/api/v1/auth/register", map[string]any{"email": uniq("rl"), "password": "correct-horse-battery", "full_name": "R"}, withIP("203.0.113.51")); rec.Code != 202 {
		t.Fatalf("other IP got %d", rec.Code)
	}
	// Exhausting /register must not throttle /login for the same IP (no key collision between routes).
	if rec := h.login(uniq("nobody"), "whatever-123", withIP(ip)); rec.Code != 401 {
		t.Fatalf("login from the register-exhausted IP: want 401 (bad creds), got %d", rec.Code)
	}
	// Redis key layout: namespaced per route and scope, no raw secrets in keys.
	keys, err := h.env.RDB.Keys(context.Background(), "*ratelimit:*").Result()
	if err != nil {
		t.Fatal(err)
	}
	var sawRegister, sawLogin bool
	for _, k := range keys {
		sawRegister = sawRegister || strings.Contains(k, "ratelimit:register:ip:"+ip)
		sawLogin = sawLogin || strings.Contains(k, "ratelimit:login:ip:"+ip)
		if strings.Contains(k, "correct-horse") || strings.Contains(k, "whatever-123") {
			t.Errorf("password material in a Redis key: %q", k)
		}
	}
	if !sawRegister || !sawLogin {
		t.Fatalf("expected distinct per-route keys, got %v", keys)
	}
}

// N parallel requests must never beat the configured burst (atomic GCRA).
func TestSecurity_RateLimit_Concurrent_NeverExceedsBurst(t *testing.T) {
	h := newHarness(t)
	const n = 80
	start := time.Now()
	var passed, limited, other int32
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			// distinct emails: the per-account limiter is not the subject; one IP: the per-IP limiter is.
			rec := h.login(fmt.Sprintf("conc-%d-%s@example.com", i, uuid.NewString()[:6]), "whatever-123", withIP("203.0.113.60"))
			switch rec.Code {
			case 401:
				atomic.AddInt32(&passed, 1)
			case 429:
				atomic.AddInt32(&limited, 1)
			default:
				atomic.AddInt32(&other, 1)
			}
		}(i)
	}
	close(gate)
	wg.Wait()
	elapsed := time.Since(start)
	// PerMinute(20): burst 20, one new token per 3s of wall clock.
	max := 20 + int(elapsed/(3*time.Second)) + 1
	if int(passed) > max || other != 0 {
		t.Fatalf("limit 20/min but %d of %d parallel requests passed in %v (max allowed %d), other=%d", passed, n, elapsed, max, other)
	}
	if limited == 0 {
		t.Fatal("no request was rate limited")
	}
}

// Credential stuffing for ONE account spread over many IPs: the account
// limiter (5/min) must stop it, even when each IP is individually fresh and
// even once the guess is correct.
func TestSecurity_RateLimit_AccountLimit_StopsDistributedStuffing(t *testing.T) {
	h := newHarness(t)
	email := uniq("stuff")
	h.mustRegister(email, "correct-horse-battery")
	// Updated for F-31: only failures count and the all-IP cap is now 20/min
	// (loginPerAccountLimit), so five bad guesses from elsewhere can no longer
	// lock the owner out (TestBug_RateLimit_AnyoneCanLockOutAnyKnownEmail),
	// while sustained stuffing across many IPs is still stopped.
	for i := 0; i < 20; i++ {
		rec := h.login(email, fmt.Sprintf("guess-%d-aaaa", i), withIP(fmt.Sprintf("203.0.113.%d", 100+i)))
		if rec.Code != 401 {
			t.Fatalf("attempt %d: %d", i, rec.Code)
		}
	}
	rec := h.login(email, "correct-horse-battery", withIP("203.0.113.200"))
	if rec.Code != 429 {
		t.Fatalf("21st failed-attempt (correct password, brand-new IP) must be blocked by the account limit, got %d", rec.Code)
	}
	// Case, plus-tag and whitespace variants must hit the same bucket for the case-insensitive part.
	for _, v := range []string{strings.ToUpper(email), strings.ToUpper(email[:1]) + email[1:]} {
		if rec := h.login(v, "correct-horse-battery", withIP("203.0.113.201")); rec.Code != 429 {
			t.Fatalf("case variant %q dodged the account limit: %d", v, rec.Code)
		}
	}
}

// ADR-0020 says nothing about outages; code returns nil on limiter errors
// (fail OPEN). Pin the current behaviour so a change is a conscious one:
// the per-IP/per-account throttles vanish when Redis is unreachable, while
// the fresh-privilege check (RequireFreshPrivilege) fails CLOSED (503).
func TestSecurity_RateLimit_RedisDown_LimiterFailsOpen_PrivilegeCheckFailsClosed(t *testing.T) {
	env := setupIdentity(t)
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = dead.Close() })
	h := newHarnessRDB(t, env, dead, lightArgon)

	// 8 failed logins for ONE account (limit 5/min per account+IP) all reach the credential check with Redis down.
	email := uniq("down")
	for i := 0; i < 8; i++ {
		rec := h.login(email, "wrong-password-1", withIP("203.0.113.70"))
		if rec.Code != 401 {
			t.Fatalf("attempt %d with Redis down: got %d (limiter is expected to fail open => 401)", i, rec.Code)
		}
	}
	// Admin-only route: token valid, role admin, Redis unreachable => 503, never 200.
	adminTok := mustAccess(t, h, uuid.New(), "admin", 1)
	rec := h.call("PATCH", "/api/v1/admin/users/"+uuid.NewString()+"/role", map[string]any{"role": "student", "status": "active"}, withToken(adminTok))
	if rec.Code != 503 {
		t.Fatalf("privileged route with Redis down: want 503 fail-closed, got %d %s", rec.Code, rec.Body.String())
	}
	assertNoInternalLeak(t, "503", rec.Body.String())
}
