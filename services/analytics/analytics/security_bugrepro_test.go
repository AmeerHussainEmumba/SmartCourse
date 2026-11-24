//go:build integration

// Regression tests for F-18 (fixed; kept as regression tests).
package analytics_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
)

// Analytics routes were RequireAuth only, so any student could read any
// course's stats. Rule now: platform totals for any authenticated user;
// per-course routes for admin, the owning instructor, or a student with an active or
// completed enrollment in that course; everyone else gets 404.
func TestBug_Analytics_StudentCanReadAnyCoursesStatsAndPlatformTotals(t *testing.T) {
	s := newSecEnv(t)
	courseID, otherCourse := uuid.New(), uuid.New()
	for _, c := range []uuid.UUID{courseID, otherCourse} {
		if err := s.env.DB.Exec(`INSERT INTO course_stats (course_id, total_enrollments, active_enrollments, completions, completion_rate, avg_completion_s) VALUES (?, 120, 80, 40, 0.3333, 7200)`, c).Error; err != nil {
			t.Fatal(err)
		}
	}
	studentID, withdrawnID := uuid.New(), uuid.New()
	for _, e := range []struct {
		stu    uuid.UUID
		status string
	}{{studentID, "active"}, {withdrawnID, "withdrawn"}} {
		if err := s.env.DB.Exec(`INSERT INTO enrollment_mirror (enrollment_id, course_id, student_id, status, enrolled_at) VALUES (?, ?, ?, ?, now())`, uuid.New(), courseID, e.stu, e.status).Error; err != nil {
			t.Fatal(err)
		}
	}
	tok := func(id uuid.UUID, role string) string {
		v, err := auth.IssueAccessToken(testJWTCfg(), id, role, 1)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	suffixes := []string{"", "/daily-enrollments"}
	for _, sfx := range suffixes {
		own := "/api/v1/analytics/courses/" + courseID.String() + sfx
		other := "/api/v1/analytics/courses/" + otherCourse.String() + sfx
		if rec := s.get(own, tok(studentID, "student")); rec.Code != 200 {
			t.Errorf("enrolled student on %s: want 200, got %d", own, rec.Code)
		}
		if rec := s.get(other, tok(studentID, "student")); rec.Code != 404 {
			t.Errorf("student not enrolled on %s: want 404, got %d", other, rec.Code)
		}
		if rec := s.get(own, tok(withdrawnID, "student")); rec.Code != 404 {
			t.Errorf("withdrawn student on %s: want 404, got %d", own, rec.Code)
		}
		if rec := s.get(other, tok(uuid.New(), "admin")); rec.Code != 200 {
			t.Errorf("admin on %s: want 200, got %d", other, rec.Code)
		}
		// Instructors are owner-gated since Week 3 (see
		// instructor_ownership_integration_test.go for the full matrix).
		if rec := s.get(other, tok(uuid.New(), "instructor")); rec.Code != 404 {
			t.Errorf("non-owner instructor on %s: want 404, got %d", other, rec.Code)
		}
	}
	for _, role := range []string{"student", "instructor", "admin"} {
		if rec := s.get("/api/v1/analytics/platform", tok(uuid.New(), role)); rec.Code != 200 {
			t.Errorf("platform totals as %s: want 200, got %d", role, rec.Code)
		}
	}
}
