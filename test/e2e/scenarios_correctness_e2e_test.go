//go:build e2e

package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// Correctness scenarios E08 and E09 on the ten-binary harness. E08's jobs run
// in analytics-worker (platform and course-stats reconcile, popular ranking)
// and course-worker (enrollment_count); both read the JOBS_* variables, which
// WorkerEnv gives to every worker.

// TestE2E_E08_CounterDrift is E8, matrix row E08 (Phase 5 job, Phase 6 metrics).
//
// Steps:
//  1. Stack as E01 with the scheduled jobs at their shortest interval,
//     JOBS_ANALYTICS_RECONCILE_INTERVAL=1m and JOBS_ENROLLMENT_COUNT_INTERVAL=1m
//     (production: 1 m and 1 h). The cron scheduler cannot go below one minute,
//     so the spec's 5 s is not configurable; the test waits up to two minutes.
//  2. Create real data: two students, one published course, one enrollment.
//     Wait until the analytics platform snapshot has seen both students
//     (GET /analytics/platform).
//  3. Corrupt state by hand: analytics.platform_snapshot (total_students 9999,
//     avg_courses_per_student 42), analytics.course_stats for the course
//     (total_enrollments 500), the Redis popular-courses ranking (999 for the
//     course) and the enrollment counter course.courses.enrollment_count (77).
//     Nothing is done through the API after this point, so no event can repair
//     anything: only the scheduled jobs can.
//  4. Wait up to two reconciliation intervals.
//
// Assertions:
//   - Immediately after step 3 the APIs show the corrupted values (proves the
//     test changed what the services serve).
//   - Within two intervals GET /analytics/platform, the course stats, the
//     popular-courses ranking and enrollment_count are back to the values a
//     direct SQL recompute from the mirrors and the enrollment rows gives, and
//     not the corrupted ones.
//   - The jobs ran as scheduled Asynq tasks: completed cron ticks of both job
//     types are in the low queue's completed set in Redis, and course-worker logged the
//     enrollment-count repair (stored 77, actual 1). The analytics reconciler
//     logs nothing when it repairs (observed; not asserted).
//
// A second enrollment is not made after the corruption: it would trigger the
// event-side recompute and heal the aggregates for the wrong reason.
func TestE2E_E08_CounterDrift(t *testing.T) {
	s := harness.Start(t, harness.Options{WorkerEnv: map[string]string{
		"JOBS_ANALYTICS_RECONCILE_INTERVAL": "1m",
		"JOBS_ENROLLMENT_COUNT_INTERVAL":    "1m",
	}})
	inst := s.NewInstructor(t)
	a, b := s.NewStudentNamed(t, "Alice"), s.NewStudentNamed(t, "Bob")
	course := s.PublishedCourse(t, inst, 1)
	s.Enroll(t, a, course)
	s.WaitSettled(t, time.Minute)
	admin := s.Login(t, s.AdminEmail())

	harness.Eventually(t, time.Minute, "analytics to have seen both students and the enrollment", func() error {
		if p := s.PlatformStats(t, admin); p.TotalStudents < 2 {
			return harness.Errf("platform %+v, want at least 2 students", p)
		}
		if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != 1 {
			return harness.Errf("course stats %+v, want 1 enrollment", cs)
		}
		return nil
	})
	_ = b

	// What a recompute from the source tables gives.
	type truth struct {
		students, instructors, published int64
		avg                              float64
		enrollments                      int64
	}
	recompute := func() truth {
		var tr truth
		tr.students = s.MustCount(t, `SELECT count(*) FROM analytics.user_role_mirror WHERE role = 'student'`)
		tr.instructors = s.MustCount(t, `SELECT count(*) FROM analytics.user_role_mirror WHERE role = 'instructor'`)
		tr.published = s.MustCount(t, `SELECT count(*) FROM analytics.course_mirror WHERE published`)
		tr.enrollments = s.MustCount(t, `SELECT count(*) FROM analytics.enrollment_mirror WHERE course_id = ? AND status IN ('active','completed')`, course.ID)
		all := s.MustCount(t, `SELECT count(*) FROM analytics.enrollment_mirror WHERE status IN ('active','completed')`)
		if tr.students > 0 {
			tr.avg = float64(all) / float64(tr.students)
		}
		return tr
	}
	want := recompute()
	if want.students < 2 || want.enrollments != 1 {
		t.Fatalf("source tables hold %+v before the corruption, want at least 2 students and 1 enrollment", want)
	}
	if n := s.MustCount(t, `SELECT enrollment_count FROM course.courses WHERE id = ?`, course.ID); n != 1 {
		t.Fatalf("enrollment_count is %d before the corruption, want 1", n)
	}

	// Step 3: corrupt by hand.
	ctx := context.Background()
	if err := s.DB().Exec(`UPDATE analytics.platform_snapshot SET total_students = 9999, avg_courses_per_student = 42`).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Exec(`UPDATE analytics.course_stats SET total_enrollments = 500 WHERE course_id = ?`, course.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Exec(`UPDATE course.courses SET enrollment_count = 77 WHERE id = ?`, course.ID).Error; err != nil {
		t.Fatal(err)
	}
	const popularKey = "analytics:popular_courses"
	if err := s.Env.RDB.ZAdd(ctx, popularKey, redisZ(999, course.ID.String())).Err(); err != nil {
		t.Fatal(err)
	}

	// The services serve the corruption.
	if p := s.PlatformStats(t, admin); p.TotalStudents != 9999 || p.AvgCoursesPerStudent != 42 {
		t.Fatalf("GET /analytics/platform after the corruption: %+v, want total_students 9999 and avg 42 (the test did not change what is served, or a job already ran)", p)
	}
	if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != 500 {
		t.Fatalf("course stats after the corruption: %+v, want 500 enrollments", cs)
	}
	popularFor := func() int64 {
		for _, it := range s.PopularCourses(t, a) {
			if it.CourseID == course.ID.String() {
				return it.Enrollments
			}
		}
		return -1
	}
	if got := popularFor(); got != 999 {
		t.Fatalf("popular-courses shows %d enrollments for the course after the corruption, want 999", got)
	}
	corrupted := time.Now()

	// Step 4: the scheduled jobs repair it.
	harness.Eventually(t, 2*time.Minute+30*time.Second, "the scheduled jobs to repair every corrupted value", func() error {
		p := s.PlatformStats(t, admin)
		if p.TotalStudents != want.students || p.TotalInstructors != want.instructors || p.TotalCoursesPublished != want.published || abs(p.AvgCoursesPerStudent-want.avg) > 0.001 {
			return harness.Errf("platform %+v, want students %d instructors %d published %d avg %.3f", p, want.students, want.instructors, want.published, want.avg)
		}
		if cs := s.CourseStatsFor(t, inst, course); cs.TotalEnrollments != want.enrollments || cs.ActiveEnrollments != 1 {
			return harness.Errf("course stats %+v, want total %d active 1", cs, want.enrollments)
		}
		if got := popularFor(); got != want.enrollments {
			return harness.Errf("popular-courses shows %d for the course, want %d", got, want.enrollments)
		}
		if n := s.MustCount(t, `SELECT enrollment_count FROM course.courses WHERE id = ?`, course.ID); n != 1 {
			return harness.Errf("courses.enrollment_count %d, want 1", n)
		}
		return nil
	})
	t.Logf("every corrupted value was repaired %s after the corruption", time.Since(corrupted).Round(time.Second))

	// The repairs came from scheduled Asynq tasks.
	harness.Eventually(t, 30*time.Second, "course-worker to log the enrollment_count repair", func() error {
		if !s.Proc(harness.CourseWorker).LogContains("enrollment_count drift repaired") {
			return harness.Errf("no enrollment_count drift repaired line in the course-worker log")
		}
		return nil
	})
	if log := s.Proc(harness.CourseWorker).Logs(); !strings.Contains(log, `"stored":77`) || !strings.Contains(log, `"actual":1`) {
		t.Errorf("the repair log line does not say stored 77 and actual 1")
	}
	ticks := completedTicks(t, s)
	for _, job := range []string{"maintenance:analytics-reconcile", "maintenance:enrollment-counts"} {
		if ticks[job] == 0 {
			t.Errorf("no completed scheduler tick of %s in the low queue (ticks seen: %v)", job, ticks)
		}
	}
	t.Logf("completed scheduler ticks retained in Redis: %v", ticks)
}
