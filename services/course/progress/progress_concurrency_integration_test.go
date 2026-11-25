//go:build integration

// Reproducers for real defects in services/course/progress found by
// adversarial testing. Regression tests; run with
// -tags=integration.
package progress_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/progress"
)

func scalarInt(t *testing.T, env *testenv.Env, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := env.DB.Raw(q, args...).Row().Scan(&n); err != nil {
		t.Fatalf("%q: %v", q, err)
	}
	return n
}

// markComplete is not serialised per enrollment (no row lock on the
// enrollment, READ COMMITTED). Two students-in-a-hurry marking the LAST TWO
// lessons at the same moment: each transaction updates its own lesson, then
// counts completed rows; neither sees the other's uncommitted update, so
// neither concludes "all done". Both commit; every lesson is complete, the
// enrollment stays 'active' forever (until some later MarkComplete call
// re-evaluates), and no progress.course_completed event / certificate trigger
// is ever written.
//
// Determinism: the test holds an ACCESS EXCLUSIVE lock on `lessons` so both
// transactions are parked after their UPDATE and before their count query,
// then releases both together.
func TestBugrepro_ConcurrentFinalLessons_EnrollmentNeverCompletes(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := progress.NewService(progress.NewRepository(env.DB))
	ctx := context.Background()
	enrollmentID, keys, studentID := seedEnrollmentWithTwoLessons(t, env)

	hold := env.DB.Begin()
	if err := hold.Exec(`LOCK TABLE lessons IN ACCESS EXCLUSIVE MODE`).Error; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, k := range keys {
		wg.Add(1)
		go func(k uuid.UUID) {
			defer wg.Done()
			if err := svc.MarkComplete(ctx, studentID, enrollmentID, k); err != nil {
				t.Errorf("MarkComplete: %v", err)
			}
		}(k)
	}
	deadline := time.Now().Add(20 * time.Second)
	for scalarInt(t, env, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database() AND (query ILIKE '%FROM lessons%' OR query ILIKE '%FOR UPDATE%')`) < 2 {
		if time.Now().After(deadline) {
			hold.Rollback()
			t.Fatal("both transactions never reached the lessons read")
		}
		time.Sleep(10 * time.Millisecond)
	}
	hold.Commit()
	wg.Wait()

	done := scalarInt(t, env, `SELECT count(*) FROM lesson_progress WHERE enrollment_id = ? AND completed_at IS NOT NULL`, enrollmentID)
	if done != 2 {
		t.Fatalf("precondition: both lessons should be marked complete, got %d", done)
	}
	var status string
	env.DB.Raw(`SELECT status FROM enrollments WHERE id = ?`, enrollmentID).Row().Scan(&status)
	events := scalarInt(t, env, `SELECT count(*) FROM outbox WHERE event_type = 'progress.course_completed'`)
	if status != "completed" || events != 1 {
		t.Errorf("all lessons are complete but enrollment status=%q and %d course_completed events (want completed, 1)", status, events)
	}
}

// Two concurrent MarkComplete calls for the SAME final lesson (double click):
// both read status='active', both pass isFullyComplete, both call
// markEnrollmentCompleted whose UPDATE ... WHERE status='active' matches zero
// rows the second time, but the code ignores RowsAffected and writes a second
// progress.course_completed event (with a different completed_at).
// Determinism: the test holds the lesson_progress row lock so both are parked
// before their UPDATE, then releases.
func TestBugrepro_ConcurrentSameFinalLesson_EmitsDuplicateCompletionEvents(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := progress.NewService(progress.NewRepository(env.DB))
	ctx := context.Background()
	enrollmentID, keys, studentID := seedEnrollmentWithTwoLessons(t, env)
	if err := svc.MarkComplete(ctx, studentID, enrollmentID, keys[0]); err != nil {
		t.Fatal(err)
	}

	hold := env.DB.Begin()
	if err := hold.Exec(`SELECT 1 FROM lesson_progress WHERE enrollment_id = ? AND lesson_key = ? FOR UPDATE`, enrollmentID, keys[1]).Error; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := svc.MarkComplete(ctx, studentID, enrollmentID, keys[1]); err != nil {
				t.Errorf("MarkComplete: %v", err)
			}
		}()
	}
	deadline := time.Now().Add(20 * time.Second)
	for scalarInt(t, env, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database() AND (query ILIKE '%UPDATE lesson_progress%' OR query ILIKE '%FOR UPDATE OF e%')`) < 2 {
		if time.Now().After(deadline) {
			hold.Rollback()
			t.Fatal("both transactions never parked on the lesson_progress row")
		}
		time.Sleep(10 * time.Millisecond)
	}
	hold.Commit()
	wg.Wait()

	if n := scalarInt(t, env, `SELECT count(*) FROM outbox WHERE event_type = 'progress.course_completed'`); n != 1 {
		t.Errorf("%d progress.course_completed events for ONE completion", n)
	}
}

// Progress rows are only pre-created at enrollment time, for the lessons of
// the version published THEN. isFullyComplete compares against the CURRENT
// published version (RFC assumption A5), so after a republish that adds a
// lesson, an existing enrollment has no lesson_progress row for the new
// lesson: markLessonComplete's UPDATE matches nothing (silently, MarkComplete
// returns success), and the student can never reach completion.
func TestBugrepro_LessonAddedByRepublish_ExistingEnrollmentCanNeverComplete(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := progress.NewService(progress.NewRepository(env.DB))
	ctx := context.Background()
	enrollmentID, keys, studentID := seedEnrollmentWithTwoLessons(t, env)

	var courseID uuid.UUID
	env.DB.Raw(`SELECT course_id FROM enrollments WHERE id = ?`, enrollmentID).Row().Scan(&courseID)
	v2, mod2, newKey := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.New()
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 2, 'ready', 'v2', '{}')`, []any{v2, courseID}},
		{`INSERT INTO modules (id, course_version_id, module_key, position, title) VALUES (?, ?, ?, 1, 'M')`, []any{mod2, v2, uuid.New()}},
		// ADR-0011: lesson keys are stable across versions; the two originals are kept, one is added.
		{`INSERT INTO lessons (id, module_id, lesson_key, position, title, content_type) VALUES (?, ?, ?, 1, 'A', 'text')`, []any{uuid.Must(uuid.NewV7()), mod2, keys[0]}},
		{`INSERT INTO lessons (id, module_id, lesson_key, position, title, content_type) VALUES (?, ?, ?, 2, 'B', 'text')`, []any{uuid.Must(uuid.NewV7()), mod2, keys[1]}},
		{`INSERT INTO lessons (id, module_id, lesson_key, position, title, content_type) VALUES (?, ?, ?, 3, 'NEW', 'text')`, []any{uuid.Must(uuid.NewV7()), mod2, newKey}},
		{`UPDATE courses SET published_version_id = ? WHERE id = ?`, []any{v2, courseID}},
	} {
		if err := env.DB.Exec(q.sql, q.args...).Error; err != nil {
			t.Fatal(err)
		}
	}

	for _, k := range []uuid.UUID{keys[0], keys[1], newKey} {
		if err := svc.MarkComplete(ctx, studentID, enrollmentID, k); err != nil {
			t.Fatalf("MarkComplete(%s): %v", k, err)
		}
	}
	var status string
	env.DB.Raw(`SELECT status FROM enrollments WHERE id = ?`, enrollmentID).Row().Scan(&status)
	rows := scalarInt(t, env, `SELECT count(*) FROM lesson_progress WHERE enrollment_id = ?`, enrollmentID)
	if status != "completed" {
		t.Errorf("student completed all 3 lessons of the published version but enrollment is %q; lesson_progress has %d rows (no row exists for the lesson added by the republish, so MarkComplete silently did nothing)", status, rows)
	}
}

// MarkComplete for a lesson_key that is not part of the enrollment at all
// (typo, another course's lesson, tampering) returns success.
func TestBugrepro_MarkCompleteUnknownLessonKeyReportsSuccess(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := progress.NewService(progress.NewRepository(env.DB))
	enrollmentID, _, studentID := seedEnrollmentWithTwoLessons(t, env)
	if err := svc.MarkComplete(context.Background(), studentID, enrollmentID, uuid.New()); err == nil {
		t.Error("MarkComplete accepted a lesson_key that does not belong to the enrollment's course and reported success (HTTP 204)")
	}
}
