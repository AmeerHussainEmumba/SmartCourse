//go:build integration

// Adversarial security tests for the analytics routes (security tester, Week 3).
// TestBugrepro_* fail today and reproduce a finding in
// docs/plans/week-3-findings-security.md; TestAdversarial_* passed when
// written (attack HELD). Run:
//
//	cd services/analytics && go test -tags='integration bugrepro' -run 'Bugrepro|Adversarial' ./analytics/
package analytics_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

// S-06 (possibly in flight): GET /analytics/issues returns dead-letter
// summaries (event types, consumers, ages) to a token that carries role=admin
// even when the token is STALE (the admin was demoted: token_version bumped).
// The route has RequireAuth + RequireRole only, no RequireFreshPrivilege, so a
// demoted admin keeps reading operational detail for the rest of the access
// token's TTL. The worker's own admin routes do check freshness.
func TestBugrepro_AnalyticsIssues_StaleAdminTokenAccepted(t *testing.T) {
	fe := &fakeFailedEvents{sum: &analytics.FailedEventsSummary{Total: 1, Unresolved: 1, ByEventType: map[string]int64{"x": 1}}}
	s := issuesEnv(t, fe, nil)
	adminID := uuid.New()
	tok, _ := auth.IssueAccessToken(testJWTCfg(), adminID, "admin", 1)
	if err := s.env.RDB.Set(context.Background(), auth.TokenVersionRedisKey(adminID.String()), 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if rec := s.get(issuesPath, tok); rec.Code == 200 {
		t.Fatalf("stale (demoted) admin token read /analytics/issues: 200 %s", rec.Body.String())
	}
}

// HELD: popular-courses and path parameters survive hostile input without a 5xx
// and the limit is bounded.
func TestAdversarial_Analytics_ParameterAbuse(t *testing.T) {
	s := newSecEnv(t)
	tok := s.token("student")
	for _, q := range []string{"limit=0", "limit=-1", "limit=abc", "limit=99999999999999999999", "limit=1000000", "limit=5&limit=500", "limit[]=1", "limit=%00", "limit=1e2", "limit=%20"} {
		rec := s.get("/api/v1/analytics/popular-courses?"+q, tok)
		if rec.Code >= 500 {
			t.Errorf("popular-courses?%s: %d %s", q, rec.Code, rec.Body.String())
		}
	}
	for _, id := range []string{"%00", "..%2f..", "nope", "00000000-0000-0000-0000-000000000000", url.PathEscape(strings.Repeat("9", 3000))} {
		for _, suffix := range []string{"", "/daily-enrollments"} {
			rec := s.get("/api/v1/analytics/courses/"+id+suffix, tok)
			if rec.Code >= 500 {
				t.Errorf("course %q%s: %d", id, suffix, rec.Code)
			}
		}
	}
	// A student with no enrolment learns nothing about whether a course exists.
	a := s.get("/api/v1/analytics/courses/"+uuid.NewString(), tok)
	b := s.get("/api/v1/analytics/courses/"+uuid.NewString(), tok)
	if a.Code != 404 || a.Body.String() != b.Body.String() {
		t.Errorf("student probe: %d vs %d", a.Code, b.Code)
	}
}

// HELD: a withdrawn student loses per-course analytics; an active one has it.
func TestAdversarial_Analytics_WithdrawnStudentLosesAccess(t *testing.T) {
	s := newSecEnv(t)
	course, student, enr := uuid.New(), uuid.New(), uuid.New()
	feed(t, s.env, ev(1, "course.course_published", map[string]any{"course_id": course, "instructor_id": uuid.New()}))
	feed(t, s.env, ev(2, "enrollment.student_enrolled", map[string]any{"course_id": course, "student_id": student, "enrollment_id": enr}))
	tok := tokenFor(t, student, "student")
	p := courseRoutes(course)[0]
	if rec := s.get(p, tok); rec.Code != 200 {
		t.Fatalf("enrolled student: %d (precondition)", rec.Code)
	}
	feed(t, s.env, ev(3, "enrollment.student_withdrawn", map[string]any{"course_id": course, "student_id": student, "enrollment_id": enr}))
	if rec := s.get(p, tok); rec.Code != 404 {
		t.Errorf("withdrawn student: %d, want 404", rec.Code)
	}
}
