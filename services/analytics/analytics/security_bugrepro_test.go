//go:build integration && bugrepro

// Reproducers for REAL authorization gaps in analytics. Excluded from the
// default suite; run with -tags='integration bugrepro'. Each FAILS today.
package analytics_test

import (
	"testing"

	"github.com/google/uuid"
)

// Every analytics route is RequireAuth only. Registration is open to anyone
// (POST /auth/register makes a student), so "authenticated" means "anybody":
// any student can read platform totals and the enrollment counts, completion
// rate and daily enrollment series of ANY instructor's course, published or
// not. The module stores no course owner (course_mirror has no instructor_id),
// so ownership cannot be enforced here without a schema + event change.
func TestBug_Analytics_StudentCanReadAnyCoursesStatsAndPlatformTotals(t *testing.T) {
	s := newSecEnv(t)
	courseID := uuid.New()
	if err := s.env.DB.Exec(`INSERT INTO course_stats (course_id, total_enrollments, active_enrollments, completions, completion_rate, avg_completion_s) VALUES (?, 120, 80, 40, 0.3333, 7200)`, courseID).Error; err != nil {
		t.Fatal(err)
	}
	student := s.token("student")
	for _, p := range []string{
		"/api/v1/analytics/courses/" + courseID.String(),
		"/api/v1/analytics/courses/" + courseID.String() + "/daily-enrollments",
		"/api/v1/analytics/platform",
	} {
		if rec := s.get(p, student); rec.Code == 200 {
			t.Errorf("a student token reads %s: %d %.120s (expected 403: instructor-of-course or admin only)", p, rec.Code, rec.Body.String())
		}
	}
}
