//go:build integration

package analytics_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func mirrorState(env *testenv.Env, enr, course uuid.UUID) (status string, enrolledAt *time.Time, active, total int64) {
	env.DB.Raw(`SELECT status, enrolled_at FROM enrollment_mirror WHERE enrollment_id = ?`, enr).Row().Scan(&status, &enrolledAt)
	env.DB.Raw(`SELECT active_enrollments, total_enrollments FROM course_stats WHERE course_id = ?`, course).Row().Scan(&active, &total)
	return
}

// Every delivery order (and duplicates) of enrolled(1), withdrawn(2) must
// converge to the same final state: withdrawn, enrolled_at filled in.
func TestEnrollmentMirror_EnrolledWithdrawn_AnyOrderAndDuplicatesConverge(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	orders := map[string][]int{"in-order": {0, 1}, "reversed": {1, 0}, "dup-reversed": {1, 1, 0, 0, 1}, "dup-in-order": {0, 0, 1, 1}}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			env := setup(t)
			course, enr, stu := uuid.New(), uuid.New(), uuid.New()
			evs := []advEvent{
				ev(1, "enrollment.student_enrolled", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course, "enrolled_at": at}),
				ev(2, "enrollment.student_withdrawn", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course}),
			}
			for _, i := range order {
				if err := deliver(env, evs[i]); err != nil {
					t.Fatal(err)
				}
			}
			status, enrolledAt, active, total := mirrorState(env, enr, course)
			if status != "withdrawn" || enrolledAt == nil || !enrolledAt.Equal(at) || active != 0 || total != 0 {
				t.Errorf("status=%q enrolled_at=%v active=%d total=%d, want withdrawn/%v/0/0", status, enrolledAt, active, total, at)
			}
		})
	}
}

// A late-arriving completion (id 3) must not be undone by a delayed older
// enrolled (id 1), and a stale withdrawn (id 2) must not undo completion.
func TestEnrollmentMirror_CompletedNotRevertedByOlderEvents(t *testing.T) {
	env := setup(t)
	course, enr, stu := uuid.New(), uuid.New(), uuid.New()
	at := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	enrolled := ev(1, "enrollment.student_enrolled", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course, "enrolled_at": at})
	withdrawn := ev(2, "enrollment.student_withdrawn", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course})
	completed := ev(3, "progress.course_completed", map[string]any{"enrollment_id": enr, "student_id": stu, "completed_at": at.Add(time.Hour)})

	if err := deliver(env, completed); err == nil {
		t.Fatal("completion before the enrollment row exists must error so the relay retries")
	}
	if err := deliver(env, enrolled); err != nil {
		t.Fatal(err)
	}
	if err := deliver(env, completed); err != nil {
		t.Fatal(err)
	}
	if err := deliver(env, withdrawn); err != nil { // stale: id 2 < 3
		t.Fatal(err)
	}
	status, _, active, total := mirrorState(env, enr, course)
	if status != "completed" || active != 0 || total != 1 {
		t.Errorf("status=%q active=%d total=%d, want completed/0/1", status, active, total)
	}
}

// Daily bucket follows enrolled_at's UTC day, including for a non-UTC
// offset in the payload, and is not double counted on redelivery.
func TestDailyEnrollment_BucketsByEnrolledAtUTC(t *testing.T) {
	env := setup(t)
	course := uuid.New()
	// 2026-03-01 01:30 at +05:00 is 2026-02-28 20:30 UTC.
	e := ev(1, "enrollment.student_enrolled", map[string]any{
		"enrollment_id": uuid.New(), "student_id": uuid.New(), "course_id": course, "enrolled_at": "2026-03-01T01:30:00+05:00",
	})
	for i := 0; i < 2; i++ {
		if err := deliver(env, e); err != nil {
			t.Fatal(err)
		}
	}
	var day string
	var n int
	env.DB.Raw(`SELECT to_char(day, 'YYYY-MM-DD'), count FROM daily_enrollments WHERE course_id = ?`, course).Row().Scan(&day, &n)
	if day != "2026-02-28" || n != 1 {
		t.Errorf("day=%s count=%d, want 2026-02-28 / 1", day, n)
	}
}

// A student denied a course gets the same 404 whether or not the course
// exists, so existence is not leaked.
func TestAnalytics_StudentDenial_Is404_ForUnknownCourse(t *testing.T) {
	s := newSecEnv(t)
	cid := uuid.NewString()
	for _, p := range []string{"/api/v1/analytics/courses/" + cid, "/api/v1/analytics/courses/" + cid + "/daily-enrollments"} {
		if rec := s.get(p, s.token("student")); rec.Code != 404 {
			t.Errorf("%s as student: want 404, got %d", p, rec.Code)
		}
	}
}
