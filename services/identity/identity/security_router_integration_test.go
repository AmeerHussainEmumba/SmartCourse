//go:build integration

// Regression tests for two router-level findings (F-02 spoofable client IP,
// F-16 no request-body cap). The fixes live in libs/router and libs/httpx; the
// harness serves identity on the real router stack, so these assert the
// behaviour end to end. Other fixed findings live in security_fixed_test.go.
package identity_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// libs/router.New never sets trusted proxies, so c.ClientIP() honours any
// X-Forwarded-For / X-Real-IP. The per-IP login/register limit
// (ratelimit.Middleware) is bypassed by rotating the header.
func TestBug_RateLimit_PerIPLimitBypassedBySpoofedForwardedFor(t *testing.T) {
	h := newHarness(t)
	var limited int
	for i := 1; i <= 12; i++ {
		rec := h.call("POST", "/api/v1/auth/register",
			map[string]any{"email": uniq("xff"), "password": "correct-horse-battery", "full_name": "X"},
			withIP("203.0.113.80"), withHeader("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", i)))
		if rec.Code == 429 {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("12 registrations in a burst from ONE TCP peer were never limited (limit is 5/min): attacker rotates X-Forwarded-For")
	}
}

// There is no request-body size limit anywhere (no MaxBytesReader, no
// http.Server.MaxBytes), so a client can stream arbitrarily large bodies at
// every endpoint (LoginRequest.Password has no max at all).
func TestBug_NoRequestBodyLimit_LoginAcceptsHugeBody(t *testing.T) {
	h := newHarness(t)
	huge := `{"email":"nobody@example.com","password":"` + strings.Repeat("A", 16<<20) + `"}`
	rec := h.rawCall("POST", "/api/v1/auth/login", []byte(huge), withIP("203.0.113.95"))
	if rec.Code != http.StatusRequestEntityTooLarge && rec.Code != http.StatusBadRequest {
		t.Fatalf("a 16 MiB login body was fully read and processed: status %d (want 413/400)", rec.Code)
	}
}
