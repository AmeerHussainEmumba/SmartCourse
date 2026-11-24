// Reproducers for REAL findings in libs/router. Run with
// regression tests (originally failing reproducers, now fixed).
package router

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// router.New never calls SetTrustedProxies, so gin trusts X-Forwarded-For /
// X-Real-IP from ANY peer. Every per-IP rate limit (ratelimit.Middleware keys
// on c.ClientIP()) is therefore bypassable by sending a fresh random
// X-Forwarded-For per request.
func TestBug_ClientIPIsSpoofableViaForwardedHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := New("test", nil)
	r.GET("/ip", func(c *gin.Context) { c.String(200, c.ClientIP()) })

	req := httptest.NewRequest("GET", "/ip", nil)
	req.RemoteAddr = "203.0.113.9:4444" // the real TCP peer, not a configured proxy
	req.Header.Set("X-Forwarded-For", "198.51.100.77")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if got := rec.Body.String(); got != "203.0.113.9" {
		t.Fatalf("ClientIP() trusted a spoofed X-Forwarded-For from an untrusted peer: got %q", got)
	}

	req = httptest.NewRequest("GET", "/ip", nil)
	req.RemoteAddr = "203.0.113.9:4444"
	req.Header.Set("X-Real-IP", "198.51.100.88")
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if got := rec.Body.String(); got != "203.0.113.9" {
		t.Fatalf("ClientIP() trusted a spoofed X-Real-IP from an untrusted peer: got %q", got)
	}
}

// /health/ready is unauthenticated and echoes raw checker error strings, which
// for pgx/go-redis contain host:port, user and database names.
func TestBug_HealthReadyLeaksDriverErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterHealth(r, map[string]Checker{
		"postgres": func(context.Context) error {
			return errors.New(`failed to connect to host=pg.internal.smartcourse.svc user=smartcourse database=smartcourse: dial tcp 10.0.3.14:5432: connect: connection refused`)
		},
	})
	req := httptest.NewRequest("GET", "/health/ready", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	for _, leak := range []string{"pg.internal", "10.0.3.14", "user=smartcourse", "5432"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("/health/ready leaks %q: %s", leak, rec.Body.String())
		}
	}
}

// No security headers at all on any response.
func TestBug_NoSecurityHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := New("test", nil)
	r.GET("/ok", func(c *gin.Context) { c.JSON(200, gin.H{}) })
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/ok", nil))
	for _, h := range []string{"X-Content-Type-Options", "Cache-Control", "X-Frame-Options", "Referrer-Policy", "Content-Security-Policy"} {
		if rec.Header().Get(h) == "" {
			t.Errorf("missing %s", h)
		}
	}
}

// X-Request-Id from the client is echoed back and logged verbatim with no
// length or charset validation (log forging / header bloat).
func TestBug_RequestIDNotValidated(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := New("test", nil)
	r.GET("/ok", func(c *gin.Context) { c.Status(204) })
	req := httptest.NewRequest("GET", "/ok", nil)
	long := strings.Repeat("A", 8000) + ` {"level":"ERROR","msg":"forged log line"}`
	req.Header.Set("X-Request-Id", long)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-Id"); got == long {
		t.Fatalf("an 8KB attacker-chosen X-Request-Id containing JSON was echoed and will be logged verbatim")
	}
}
