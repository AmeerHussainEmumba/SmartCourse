//go:build integration

// What this file checks, against real Postgres (RFC §15.1, §8.1 step 2): that
// a repeated Idempotency-Key replays the stored response instead of running
// the handler again; that concurrent requests with one key run it once; that
// reusing a key with a different request is refused; that failed and panicked
// requests release the key; and that abandoned or expired keys can be
// claimed again.
package idempotency_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
)

type harness struct {
	db    *gorm.DB
	r     *gin.Engine
	calls atomic.Int32
	// next decides the handler's response; nil means 201 {"n":<call number>}.
	next func(c *gin.Context, n int32)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	h := &harness{db: setupCourse(t).DB}
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(func(c *gin.Context) {
		if u := c.GetHeader("X-User"); u != "" {
			c.Set("user", uuid.MustParse(u))
		}
	})
	r.Use(idempotency.Middleware(h.db, idempotency.Options{
		UserID: func(c *gin.Context) (uuid.UUID, bool) {
			v, ok := c.Get("user")
			if !ok {
				return uuid.Nil, false
			}
			return v.(uuid.UUID), true
		},
		Lease: 2 * time.Second,
	}))
	r.POST("/things", func(c *gin.Context) {
		n := h.calls.Add(1)
		if h.next != nil {
			h.next(c, n)
			return
		}
		c.JSON(http.StatusCreated, gin.H{"n": n})
	})
	h.r = r
	return h
}

var userA = uuid.New().String()
var userB = uuid.New().String()

func (h *harness) do(user, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/things", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if user != "" {
		req.Header.Set("X-User", user)
	}
	if key != "" {
		req.Header.Set(idempotency.Header, key)
	}
	rec := httptest.NewRecorder()
	h.r.ServeHTTP(rec, req)
	return rec
}

func TestNoKey_RunsEveryTime(t *testing.T) {
	h := newHarness(t)
	h.do(userA, "", `{}`)
	h.do(userA, "", `{}`)
	if got := h.calls.Load(); got != 2 {
		t.Fatalf("handler ran %d times, want 2", got)
	}
}

func TestSameKey_ReplaysStoredResponse(t *testing.T) {
	h := newHarness(t)
	first := h.do(userA, "k1", `{"a":1}`)
	second := h.do(userA, "k1", `{"a":1}`)
	if got := h.calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
	if first.Code != 201 || second.Code != 201 {
		t.Fatalf("statuses %d, %d want 201, 201", first.Code, second.Code)
	}
	if strings.ReplaceAll(first.Body.String(), " ", "") != strings.ReplaceAll(second.Body.String(), " ", "") {
		t.Errorf("replayed body %q != first %q", second.Body.String(), first.Body.String())
	}
	if second.Header().Get(idempotency.ReplayedHeader) != "true" {
		t.Error("replay not marked with Idempotent-Replayed")
	}
	if first.Header().Get(idempotency.ReplayedHeader) != "" {
		t.Error("first response wrongly marked as a replay")
	}
}

func TestSameKey_DifferentBody_Refused(t *testing.T) {
	h := newHarness(t)
	h.do(userA, "k2", `{"a":1}`)
	rec := h.do(userA, "k2", `{"a":2}`)
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "idempotency_key_reused") {
		t.Fatalf("got %d %s, want 422 idempotency_key_reused", rec.Code, rec.Body.String())
	}
	if h.calls.Load() != 1 {
		t.Errorf("handler ran again for a mismatched request")
	}
}

func TestKeysAreScopedPerUser(t *testing.T) {
	h := newHarness(t)
	h.do(userA, "shared", `{}`)
	rec := h.do(userB, "shared", `{}`)
	if rec.Header().Get(idempotency.ReplayedHeader) != "" || h.calls.Load() != 2 {
		t.Fatalf("user B was served user A's response (calls=%d)", h.calls.Load())
	}
}

func TestUnauthenticated_IsNotSubjectToKeys(t *testing.T) {
	h := newHarness(t)
	h.do("", "k3", `{}`)
	h.do("", "k3", `{}`)
	if h.calls.Load() != 2 {
		t.Fatalf("anonymous requests were deduplicated (calls=%d)", h.calls.Load())
	}
}

func TestConcurrentSameKey_ExecutesOnce(t *testing.T) {
	h := newHarness(t)
	h.next = func(c *gin.Context, n int32) {
		time.Sleep(300 * time.Millisecond) // keep the owner in flight while the others arrive
		c.JSON(http.StatusCreated, gin.H{"n": n})
	}
	const workers = 10
	codes := make([]int, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = h.do(userA, "race", `{}`).Code
		}()
	}
	wg.Wait()
	if got := h.calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times under concurrency, want 1", got)
	}
	created := 0
	for _, c := range codes {
		switch c {
		case 201:
			created++
		case 409:
		default:
			t.Errorf("unexpected status %d", c)
		}
	}
	if created < 1 {
		t.Fatalf("no request got the 201: %v", codes)
	}
	// After the owner finished, a retry is a clean replay.
	if rec := h.do(userA, "race", `{}`); rec.Code != 201 || rec.Header().Get(idempotency.ReplayedHeader) != "true" {
		t.Errorf("retry after completion: %d replayed=%q", rec.Code, rec.Header().Get(idempotency.ReplayedHeader))
	}
}

func TestFailedRequests_ReleaseTheKey(t *testing.T) {
	for name, status := range map[string]int{"validation": 400, "conflict": 409, "server error": 500} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.next = func(c *gin.Context, n int32) {
				if n == 1 {
					c.JSON(status, gin.H{"code": "x"})
					return
				}
				c.JSON(http.StatusCreated, gin.H{"n": n})
			}
			if rec := h.do(userA, "retry", `{}`); rec.Code != status {
				t.Fatalf("first: %d want %d", rec.Code, status)
			}
			rec := h.do(userA, "retry", `{}`)
			if rec.Code != 201 || h.calls.Load() != 2 {
				t.Fatalf("retry after %d: status %d calls %d; want a fresh execution", status, rec.Code, h.calls.Load())
			}
		})
	}
}

func TestPanickingHandler_ReleasesTheKey(t *testing.T) {
	h := newHarness(t)
	h.next = func(c *gin.Context, n int32) {
		if n == 1 {
			panic("boom")
		}
		c.JSON(http.StatusCreated, gin.H{"n": n})
	}
	if rec := h.do(userA, "panic", `{}`); rec.Code != 500 {
		t.Fatalf("panic response %d want 500", rec.Code)
	}
	if rec := h.do(userA, "panic", `{}`); rec.Code != 201 {
		t.Fatalf("retry after panic: %d want 201 (key should be released)", rec.Code)
	}
}

func TestInvalidKey_Rejected(t *testing.T) {
	h := newHarness(t)
	for _, k := range []string{"has space", strings.Repeat("a", 256), "tab\there", "é"} {
		if rec := h.do(userA, k, `{}`); rec.Code != 400 {
			t.Errorf("key %q: %d want 400", k, rec.Code)
		}
	}
	if h.calls.Load() != 0 {
		t.Error("handler ran for an invalid key")
	}
}

func TestAbandonedInProgress_CanBeTakenOver(t *testing.T) {
	h := newHarness(t)
	uid := uuid.MustParse(userA)
	// A crashed owner: in_progress, older than the 2 s test lease.
	if err := h.db.Exec(`INSERT INTO idempotency_keys (key, user_id, endpoint, request_hash, state, created_at, expires_at)
		VALUES ('stale', ?, 'POST /things', ?, 'in_progress', now() - interval '1 minute', now() + interval '1 day')`,
		uid, []byte("old")).Error; err != nil {
		t.Fatal(err)
	}
	if rec := h.do(userA, "stale", `{}`); rec.Code != 201 || h.calls.Load() != 1 {
		t.Fatalf("takeover: %d calls %d", rec.Code, h.calls.Load())
	}
}

func TestExpiredKey_ExecutesAgain_AndPruneRemovesIt(t *testing.T) {
	h := newHarness(t)
	h.do(userA, "old", `{}`)
	if err := h.db.Exec(`UPDATE idempotency_keys SET expires_at = now() - interval '1 second' WHERE key = 'old'`).Error; err != nil {
		t.Fatal(err)
	}
	if rec := h.do(userA, "old", `{}`); rec.Code != 201 || h.calls.Load() != 2 {
		t.Fatalf("expired key: %d calls %d want a fresh execution", rec.Code, h.calls.Load())
	}
	if err := h.db.Exec(`UPDATE idempotency_keys SET expires_at = now() - interval '1 second'`).Error; err != nil {
		t.Fatal(err)
	}
	n, err := idempotency.PruneExpired(t.Context(), h.db)
	if err != nil || n != 1 {
		t.Fatalf("PruneExpired = %d, %v; want 1, nil", n, err)
	}
}
