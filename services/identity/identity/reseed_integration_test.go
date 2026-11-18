//go:build integration

// F-38 residue: after Redis loses user:tv:<id>, identity (the only owner of
// token_version) rewrites the keys itself, so course-api, which cannot read
// identity.users (ADR-0024), stops answering token_stale.
package identity_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
)

func (h *harness) setVersionSQL(id string, v int) {
	h.t.Helper()
	if err := h.env.DB.Exec(`UPDATE users SET token_version = ? WHERE id = ?`, v, id).Error; err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) tvKey(id string) (string, bool) {
	h.t.Helper()
	v, err := h.env.RDB.Get(context.Background(), auth.TokenVersionRedisKey(id)).Result()
	if err != nil {
		return "", false
	}
	return v, true
}

// courseAPIGuard is the guard course-api uses on instructor writes: Redis only,
// a miss reads as version 1 (no durable source).
func (h *harness) courseAPIGuard(token string) int {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/w", auth.RequireAuth(h.jwtCfg), auth.RequireFreshPrivilege(h.env.RDB), func(c *gin.Context) { c.Status(http.StatusOK) })
	req := httptest.NewRequest("POST", "/w", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code == http.StatusUnauthorized && errCode(h.t, rec) != "token_stale" {
		h.t.Fatalf("unexpected 401 code %q", errCode(h.t, rec))
	}
	return rec.Code
}

func TestReseed_AfterRedisFlush_CourseAPIStopsAnsweringTokenStale(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	promoted := h.mustRegister(uniq("promoted"), "CorrectPass123!").User.ID
	newer := h.mustRegister(uniq("newer"), "CorrectPass123!").User.ID
	plain := h.mustRegister(uniq("plain"), "CorrectPass123!").User.ID
	h.setVersionSQL(promoted, 3)
	h.setVersionSQL(newer, 4)

	tok := func(id string, v int16) string {
		uid := uuid.MustParse(id)
		s, err := auth.IssueAccessToken(h.jwtCfg, uid, "instructor", v)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	// The flush: every user:tv:* key and the sentinel are gone.
	if err := h.env.RDB.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if got := h.courseAPIGuard(tok(promoted, 3)); got != http.StatusUnauthorized {
		t.Fatalf("precondition: after a flush a version-3 token should be token_stale at course-api, got %d", got)
	}

	// Meanwhile a role change on another replica wrote a newer value for `newer`.
	if err := h.env.RDB.Set(ctx, auth.TokenVersionRedisKey(newer), 9, 0).Err(); err != nil {
		t.Fatal(err)
	}

	n, err := h.svc.ReseedTokenVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("keys written = %d, want 1 (promoted only: newer already has a key, plain is version 1)", n)
	}
	if got := h.courseAPIGuard(tok(promoted, 3)); got != http.StatusOK {
		t.Fatalf("after re-seed a current version-3 token must pass course-api's guard, got %d", got)
	}
	if got := h.courseAPIGuard(tok(promoted, 2)); got != http.StatusUnauthorized {
		t.Fatalf("a revoked (older) token must still be refused after re-seed, got %d", got)
	}
	if v, _ := h.tvKey(newer); v != "9" {
		t.Fatalf("SET NX overwrote a newer value: newer key = %q, want 9", v)
	}
	if _, ok := h.tvKey(plain); ok {
		t.Fatal("a version-1 user needs no key")
	}

	// Sentinel present: the next pass does nothing.
	if n, err := h.svc.ReseedTokenVersions(ctx); err != nil || n != 0 {
		t.Fatalf("second pass = %d, %v; want a no-op", n, err)
	}
	// While the sentinel exists the pass does not touch the database or Redis
	// again (a key lost to eviction waits for the sentinel TTL).
	if err := h.env.RDB.Del(ctx, auth.TokenVersionRedisKey(promoted)).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := h.svc.ReseedTokenVersions(ctx); err != nil || n != 0 {
		t.Fatalf("pass under a live sentinel = %d, %v; want 0", n, err)
	}
	if _, ok := h.tvKey(promoted); ok {
		t.Fatal("a pass ran although the sentinel was present")
	}
	// Another flush removes the sentinel, so the work happens again.
	if err := h.env.RDB.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := h.svc.ReseedTokenVersions(ctx); err != nil || n != 2 {
		t.Fatalf("pass after second flush = %d, %v; want 2", n, err)
	}
}

func TestReseed_PagesThroughAllUsers(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	var ids []string
	for i := 0; i < 5; i++ {
		id := h.mustRegister(uniq(fmt.Sprintf("page%d", i)), "CorrectPass123!").User.ID
		h.setVersionSQL(id, 2+i)
		ids = append(ids, id)
	}
	h.svc.SetReseedBatchForTest(2) // 2+2+1: exercises the keyset boundary
	if err := h.env.RDB.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	n, err := h.svc.ReseedTokenVersions(ctx)
	if err != nil || n != 5 {
		t.Fatalf("written = %d, %v; want 5", n, err)
	}
	for i, id := range ids {
		if v, ok := h.tvKey(id); !ok || v != fmt.Sprint(2+i) {
			t.Fatalf("user %d key = %q (present %v), want %d", i, v, ok, 2+i)
		}
	}
}

func TestReseed_LoopRepairsAFlushWithoutAnyLogin(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	id := h.mustRegister(uniq("loop"), "CorrectPass123!").User.ID
	h.setVersionSQL(id, 3)
	if err := h.env.RDB.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { defer close(done); h.svc.RunTokenVersionReseeder(ctx, 20*time.Millisecond) }()

	waitKey := func(want string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if v, ok := h.tvKey(id); ok && v == want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("key never became %s", want)
	}
	waitKey("3")
	// And again after a second flush, with no login in between.
	if err := h.env.RDB.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	waitKey("3")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reseeder did not stop when its context was cancelled")
	}
}
