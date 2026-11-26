//go:build integration

package analytics_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
)

// ACCEPTED LIMITATION (security finding S-07, owner decision 2026-11-26,
// docs/plans/week-3-findings-security.md): this test asserts documented
// behaviour, it is not a bug reproducer. Do not "fix" it without reopening
// the decision.
//
// The per-course analytics routes verify the access token's signature and
// expiry but not its token version (only the admin issues route does). A
// suspended or demoted instructor's already-issued access token therefore
// keeps READING analytics of their own courses until it expires (at most 15
// minutes). Writes are a different matter (see S-09 in services/course).
func TestAcceptedLimitation_S07_SuspendedInstructorStillReadsOwnCourseAnalyticsUntilTokenExpiry(t *testing.T) {
	s := newSecEnv(t)
	course, owner := uuid.New(), uuid.New()
	feed(t, s.env, ev(1, "course.course_published", map[string]any{"course_id": course, "instructor_id": owner}))
	tok, _ := auth.IssueAccessToken(testJWTCfg(), owner, "instructor", 1)
	p := courseRoutes(course)[0]
	if rec := s.get(p, tok); rec.Code != 200 {
		t.Fatalf("precondition: owner should read 200, got %d", rec.Code)
	}

	// Suspension bumps the token version in Redis.
	if err := s.env.RDB.Set(context.Background(), auth.TokenVersionRedisKey(owner.String()), 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if rec := s.get(p, tok); rec.Code != 200 {
		t.Fatalf("documented behaviour: the stale token still reads its own course analytics until it expires, got %d", rec.Code)
	}

	// The bound is token expiry: an expired token of the same instructor is refused.
	cfg := testJWTCfg()
	cfg.AccessTokenTTL = -time.Minute
	expired, _ := auth.IssueAccessToken(cfg, owner, "instructor", 1)
	if rec := s.get(p, expired); rec.Code != 401 {
		t.Fatalf("an expired token must be refused, got %d", rec.Code)
	}
}
