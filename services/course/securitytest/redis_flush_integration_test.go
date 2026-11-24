//go:build integration

package securitytest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
)

// Regression test for F-38: instructors whose token_version is above 1 were
// locked out of course-api after a Redis restart, even with a freshly issued
// token.
//
// course-api checks the caller's token version on every instructor write
// (RequireFreshPrivilege, F-11). It cannot read identity.users (ADR-0024), so
// the cached version in Redis is the only source it has, and a missing entry
// reads as version 1. Identity therefore seeds the version in Redis at login
// and refresh (services/identity: TestFix_LoginAndRefreshAfterRedisFlush_
// SeedTokenVersion covers that half; this module cannot import identity).
//
// This test pins the course-api half of the contract that seeding relies on:
//   - Redis flushed and a token above version 1: the write is refused with
//     token_stale (strict, so a revoked token cannot slip through whenever Redis
//     is empty), not a 5xx;
//   - once the version is present again (what identity's login does), a token
//     of that version works, with no role change needed;
//   - revocation is still enforced when Redis holds a newer version, and an
//     older token is still refused.
func TestCourseAPI_InstructorWithVersionAboveOne_RedisFlush_RecoversOnceVersionSeeded(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	id := w.actors[instrA].id
	key := auth.TokenVersionRedisKey(id.String())

	tv2, err := auth.IssueAccessToken(jwtCfg(), id, "instructor", 2)
	if err != nil {
		t.Fatal(err)
	}
	tv1, err := auth.IssueAccessToken(jwtCfg(), id, "instructor", 1)
	if err != nil {
		t.Fatal(err)
	}
	create := func(token string) (int, string) {
		rec := w.doRaw(token, "POST", "/api/v1/courses", map[string]any{
			"title": "Flush", "slug": "flush-" + strings.ToLower(uuid.NewString()[:8]),
		})
		return rec.Code, rec.Body.String()
	}

	// 1. The state identity leaves after a role change: key present, version 2.
	if err := w.env.RDB.Set(ctx, key, 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if code, body := create(tv2); !okStatus(code) {
		t.Fatalf("version 2 with the version cached: want success, got %d %s", code, body)
	}

	// 2. Redis restarts. The same user, same token: refused as stale (never a 5xx).
	if err := w.env.RDB.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	code, body := create(tv2)
	if code != 401 || !strings.Contains(body, `"code":"token_stale"`) {
		t.Fatalf("after a flush a version-2 token should be refused with 401 token_stale, got %d %s", code, body)
	}
	assertNoInternalLeak(t, "flush", w.doRaw(tv2, "POST", "/api/v1/courses", map[string]any{"title": "x", "slug": "flush-leak"}))

	// 3. Identity's login seeds the version (SET NX). The user's new token has
	// version 2, and now works with no role change in between.
	if err := w.env.RDB.SetNX(ctx, key, 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if code, body := create(tv2); !okStatus(code) {
		t.Fatalf("after the version is seeded, a version-2 token should work again: %d %s", code, body)
	}

	// 4. Revocation is still enforced: an older token is refused...
	if code, body := create(tv1); code != 401 || !strings.Contains(body, `"code":"token_stale"`) {
		t.Fatalf("a version-1 token against cached version 2 should be refused as token_stale: %d %s", code, body)
	}
	// ...and a demotion (identity writes a newer version) revokes the version-2 token.
	if err := w.env.RDB.Set(ctx, key, 3, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if code, body := create(tv2); code != 401 || !strings.Contains(body, `"code":"token_stale"`) {
		t.Fatalf("after a newer version is cached the version-2 token should be refused: %d %s", code, body)
	}
	// SET NX (what seeding uses) must not roll the cached version back.
	if err := w.env.RDB.SetNX(ctx, key, 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if v, _ := w.env.RDB.Get(ctx, key).Int(); v != 3 {
		t.Fatalf("seeding must never overwrite a newer cached version: got %d, want 3", v)
	}
}
