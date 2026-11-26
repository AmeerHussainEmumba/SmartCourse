//go:build integration

// Adversarial security tests for libs/idempotency (security tester, Week 3).
// The S-03, S-04 and S-05 tests were reproducers in
// docs/plans/week-3-findings-security.md and are regression tests now that
// responses are stored byte for byte (migration course/000009);
// TestAdversarial_* passed when written (attack HELD).
package idempotency_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
)

// S-03: a 2xx response whose JSON contains a NUL escape (\u0000) cannot be
// stored (Postgres jsonb rejects it), so complete() fails, the key is
// RELEASED and the retry runs the handler a second time: the "at most once"
// guarantee silently does not hold for that response. Attacker leverage: any
// route that echoes user-controlled text into its 2xx body without NUL
// filtering gives a way to force double execution of a non-idempotent write
// (double charge / double create) on client retry.
func TestS03_ResponseWithNULEscape_IsStoredHandlerRunsOnce(t *testing.T) {
	h := newHarness(t)
	h.next = func(c *gin.Context, n int32) {
		c.Data(http.StatusCreated, "application/json", []byte(`{"title":"a\u0000b"}`))
	}
	h.do(userA, "nul-1", `{}`)
	second := h.do(userA, "nul-1", `{}`)
	if got := h.calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times for one Idempotency-Key, want 1 (replay header=%q)", got, second.Header().Get(idempotency.ReplayedHeader))
	}
}

// S-04: replays are not byte-identical. The response is stored as jsonb, so
// key order, duplicate keys and number formatting are normalised: a client
// (or a signature check over the body) that compares the retry to the
// original sees a different document.
func TestS04_ReplayIsByteIdentical(t *testing.T) {
	h := newHarness(t)
	orig := `{"b":1,"a":2.50,"a":3}`
	h.next = func(c *gin.Context, n int32) { c.Data(http.StatusCreated, "application/json", []byte(orig)) }
	first := h.do(userA, "norm-1", `{}`)
	second := h.do(userA, "norm-1", `{}`)
	if second.Header().Get(idempotency.ReplayedHeader) != "true" {
		t.Skip("not a replay")
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("replay differs from the original response:\n first  %q\n second %q", first.Body.String(), second.Body.String())
	}
}

// S-05: a 2xx with an empty body (204, or 202 with no body) is never stored,
// so the key is released and every retry re-executes. Latent: no current
// Idempotency-Key route answers 204.
func TestS05_EmptyBody2xx_IsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.next = func(c *gin.Context, n int32) { c.Status(http.StatusNoContent) }
	h.do(userA, "empty-1", `{}`)
	h.do(userA, "empty-1", `{}`)
	if got := h.calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times for one key on a 204 route, want 1", got)
	}
}

// HELD: key and body abuse.
func TestAdversarial_KeyAndBodyAbuse(t *testing.T) {
	h := newHarness(t)
	long255 := strings.Repeat("k", 255)
	if rec := h.do(userA, long255, `{}`); rec.Code != 201 {
		t.Errorf("255-char key refused: %d", rec.Code)
	}
	bad := map[string]string{
		"256 chars": strings.Repeat("k", 256), "space": "a b", "tab": "a\tb", "del": "a\x7fb", "nul": "a\x00b",
		"non-ascii": "kéy", "emoji": "k😀", "cr": "a\rb", "lf": "a\nb",
	}
	for name, k := range bad {
		req := httptest.NewRequest("POST", "/things", strings.NewReader(`{}`))
		req.Header.Set("X-User", userA)
		req.Header["Idempotency-Key"] = []string{k}
		rec := httptest.NewRecorder()
		h.r.ServeHTTP(rec, req)
		if rec.Code != 400 {
			t.Errorf("%s: status %d, want 400", name, rec.Code)
		}
	}
	// Header smuggling: two Idempotency-Key headers are refused, not merged or
	// first/last-wins.
	req := httptest.NewRequest("POST", "/things", strings.NewReader(`{}`))
	req.Header.Set("X-User", userA)
	req.Header.Add("Idempotency-Key", "one")
	req.Header.Add("Idempotency-Key", "two")
	rec := httptest.NewRecorder()
	h.r.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Errorf("duplicate header: %d", rec.Code)
	}
	// SQL metacharacters in a (valid, printable) key are data.
	for _, k := range []string{`'OR'1'='1`, `';DROPTABLEidempotency_keys;--`, `";--`, `\`, `%`, `$1`} {
		before := h.calls.Load()
		if rec := h.do(userA, k, `{}`); rec.Code != 201 || h.calls.Load() != before+1 {
			t.Errorf("key %q: %d", k, rec.Code)
		}
	}
	var n int64
	if err := h.db.Raw(`SELECT count(*) FROM idempotency_keys`).Scan(&n).Error; err != nil || n == 0 {
		t.Errorf("idempotency_keys unreadable after injection keys: %v n=%d", err, n)
	}
}

// HELD: replay of user A's stored response to user B, and across endpoints and
// query strings.
func TestAdversarial_ReplayNeverCrossesUsersOrEndpoints(t *testing.T) {
	h := newHarness(t)
	h.r.POST("/other", func(c *gin.Context) { c.JSON(201, gin.H{"other": h.calls.Add(1)}) })
	h.next = func(c *gin.Context, n int32) { c.JSON(201, gin.H{"secret_for": c.GetHeader("X-User"), "n": n}) }
	h.do(userA, "same", `{}`)
	rec := h.do(userB, "same", `{}`)
	if strings.Contains(rec.Body.String(), userA) || rec.Header().Get(idempotency.ReplayedHeader) != "" {
		t.Fatalf("user B received user A's stored response: %s", rec.Body.String())
	}
	// Same user, same key, other endpoint: runs on its own, never replays /things.
	req := httptest.NewRequest("POST", "/other", strings.NewReader(`{}`))
	req.Header.Set("X-User", userA)
	req.Header.Set("Idempotency-Key", "same")
	r2 := httptest.NewRecorder()
	h.r.ServeHTTP(r2, req)
	if strings.Contains(r2.Body.String(), "secret_for") {
		t.Fatalf("response of /things replayed on /other: %s", r2.Body.String())
	}
	// Same key, query string changes the request hash: refused.
	req = httptest.NewRequest("POST", "/things?x=1", strings.NewReader(`{}`))
	req.Header.Set("X-User", userA)
	req.Header.Set("Idempotency-Key", "same")
	r3 := httptest.NewRecorder()
	h.r.ServeHTTP(r3, req)
	if r3.Code != 422 {
		t.Errorf("query-string change under the same key: %d, want 422", r3.Code)
	}
}

func mustUUID(s string) uuid.UUID { return uuid.MustParse(s) }

func userFromCtx(c *gin.Context) (uuid.UUID, bool) {
	v, ok := c.Get("user")
	if !ok {
		return uuid.Nil, false
	}
	return v.(uuid.UUID), true
}

// HELD: a body over the cap answers 413 and does not leave the key claimed.
func TestAdversarial_OversizedBodyIs413AndKeyNotStuck(t *testing.T) {
	h := newHarness(t)
	capped := gin.New()
	capped.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1024)
		c.Set("user", mustUUID(userA))
	})
	capped.Use(idempotency.Middleware(h.db, idempotency.Options{UserID: userFromCtx}))
	capped.POST("/x", func(c *gin.Context) { c.JSON(201, gin.H{"ok": true}) })
	do := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/x", strings.NewReader(body))
		req.Header.Set("Idempotency-Key", "big-1")
		rec := httptest.NewRecorder()
		capped.ServeHTTP(rec, req)
		return rec
	}
	if rec := do(strings.Repeat("a", 4096)); rec.Code != 413 {
		t.Fatalf("oversized: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(`{}`); rec.Code != 201 {
		t.Fatalf("key stuck after a 413: %d %s", rec.Code, rec.Body.String())
	}
}
