//go:build integration

// Reproducers for real defects in analytics' event handling. These fail today
// (fixed; kept as regression tests).
package analytics_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// The relay's worker pool has no per-aggregate ordering (migration comment on
// user_role_mirror says so itself). If student_withdrawn is processed before
// student_enrolled for the same enrollment, UpdateEnrollmentMirrorStatus
// matches zero rows and the withdrawn event is marked processed and lost; the
// enrolled event then inserts the row as 'active'. The mirror, and with it
// course_stats.active_enrollments, permanently says the student is enrolled.
// (user_role_mirror and instructor_mirror have a last_event_id guard for the
// same hazard; enrollment_mirror has none.)
func TestBugrepro_WithdrawnBeforeEnrolled_MirrorEndsActive(t *testing.T) {
	env := testenv.Setup(t)
	course, enr, stu := uuid.New(), uuid.New(), uuid.New()
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	withdrawn := ev(2, "enrollment.student_withdrawn", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course})
	enrolled := ev(1, "enrollment.student_enrolled", map[string]any{"enrollment_id": enr, "student_id": stu, "course_id": course, "enrolled_at": at})
	if err := deliver(env, withdrawn); err != nil {
		t.Fatalf("withdrawn-before-enrolled returned an error (relay would retry): %v", err)
	}
	if err := deliver(env, enrolled); err != nil {
		t.Fatal(err)
	}

	var status string
	var active int64
	env.DB.Raw(`SELECT status FROM enrollment_mirror WHERE enrollment_id = ?`, enr).Row().Scan(&status)
	env.DB.Raw(`SELECT active_enrollments FROM course_stats WHERE course_id = ?`, course).Row().Scan(&active)
	if status != "withdrawn" || active != 0 {
		t.Errorf("enrollment was withdrawn (outbox id 2 > 1) but mirror status=%q, active_enrollments=%d", status, active)
	}
}

// IncrementDailyEnrollment buckets by the database's current_date at
// PROCESSING time, not by the event's enrolled_at (which the payload
// carries). An enrollment at 23:59:59 UTC that the relay processes after
// midnight (relay lag, retry, outage, replay after downtime) is counted on the
// wrong day; so is any enrollment whose DB session timezone is not UTC.
// The function's own comment promises "an enrollment happened on this UTC day".
func TestBugrepro_DailyEnrollment_UsesProcessingDayNotEnrolledAt(t *testing.T) {
	env := testenv.Setup(t)
	course, enr, stu := uuid.New(), uuid.New(), uuid.New()
	enrolledAt := time.Now().UTC().Truncate(24 * time.Hour).Add(-time.Second) // yesterday 23:59:59 UTC
	if err := deliver(env, ev(1, "enrollment.student_enrolled", map[string]any{
		"enrollment_id": enr, "student_id": stu, "course_id": course, "enrolled_at": enrolledAt,
	})); err != nil {
		t.Fatal(err)
	}
	var n int64
	env.DB.Raw(`SELECT count(*) FROM daily_enrollments WHERE course_id = ? AND day = ?`, course, enrolledAt.Format("2006-01-02")).Row().Scan(&n)
	if n != 1 {
		var day time.Time
		env.DB.Raw(`SELECT day FROM daily_enrollments WHERE course_id = ?`, course).Row().Scan(&day)
		t.Errorf("enrollment at %s was bucketed on %s instead of its own UTC day", enrolledAt.Format(time.RFC3339), day.Format("2006-01-02"))
	}
}
