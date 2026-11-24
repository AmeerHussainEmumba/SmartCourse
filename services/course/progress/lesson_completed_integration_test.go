//go:build integration

// What this file checks, against real Postgres: progress.lesson_completed is
// written once per lesson completion, in the same transaction as the
// lesson_progress update. An idempotent repeat, a concurrent duplicate and a
// rejected request write nothing; a completion that finishes the course
// writes the lesson event and then course_completed.
package progress_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/progress"
)

func outboxCount(t *testing.T, env *testenv.Env, eventType string, enrollmentID uuid.UUID) int64 {
	t.Helper()
	return scalarInt(t, env, `SELECT count(*) FROM outbox WHERE event_type = ? AND aggregate_id = ?`, eventType, enrollmentID)
}

func TestMarkComplete_WritesLessonCompletedOncePerLesson(t *testing.T) {
	env := testenv.Setup(t)
	svc := progress.NewService(progress.NewRepository(env.DB))
	ctx := context.Background()
	enrollmentID, lessonKeys, studentID := seedEnrollmentWithTwoLessons(t, env)

	if err := svc.MarkComplete(ctx, studentID, enrollmentID, lessonKeys[0]); err != nil {
		t.Fatal(err)
	}
	if n := outboxCount(t, env, progress.EventLessonCompleted, enrollmentID); n != 1 {
		t.Fatalf("lesson_completed events after one completion: %d, want 1", n)
	}
	// The idempotent repeat (RFC §8.3) changes no row and must write nothing.
	for range 3 {
		if err := svc.MarkComplete(ctx, studentID, enrollmentID, lessonKeys[0]); err != nil {
			t.Fatal(err)
		}
	}
	if n := outboxCount(t, env, progress.EventLessonCompleted, enrollmentID); n != 1 {
		t.Fatalf("lesson_completed events after repeats of the same lesson: %d, want 1", n)
	}

	var raw []byte
	var aggregateType string
	if err := env.DB.Raw(`SELECT payload, aggregate_type FROM outbox WHERE event_type = ? AND aggregate_id = ?`,
		progress.EventLessonCompleted, enrollmentID).Row().Scan(&raw, &aggregateType); err != nil {
		t.Fatal(err)
	}
	if aggregateType != "enrollment" {
		t.Errorf("aggregate_type = %q, want enrollment", aggregateType)
	}
	var p progress.LessonCompletedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	var rowDone string
	_ = env.DB.Raw(`SELECT to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') FROM lesson_progress WHERE enrollment_id = ? AND lesson_key = ?`,
		enrollmentID, lessonKeys[0]).Row().Scan(&rowDone)
	if p.EnrollmentID != enrollmentID || p.StudentID != studentID || p.LessonKey != lessonKeys[0] || p.CompletedAt.IsZero() {
		t.Errorf("payload = %+v", p)
	}
	if got := p.CompletedAt.UTC().Format("2006-01-02T15:04:05.000000"); got != rowDone {
		t.Errorf("payload completed_at %s differs from the lesson_progress row %s", got, rowDone)
	}
	if n := outboxCount(t, env, progress.EventCourseCompleted, enrollmentID); n != 0 {
		t.Errorf("course_completed written after 1 of 2 lessons: %d", n)
	}

	// The last lesson writes its own lesson event and then course_completed.
	if err := svc.MarkComplete(ctx, studentID, enrollmentID, lessonKeys[1]); err != nil {
		t.Fatal(err)
	}
	if n := outboxCount(t, env, progress.EventLessonCompleted, enrollmentID); n != 2 {
		t.Errorf("lesson_completed events after both lessons: %d, want 2", n)
	}
	if n := outboxCount(t, env, progress.EventCourseCompleted, enrollmentID); n != 1 {
		t.Errorf("course_completed events: %d, want 1", n)
	}
	var lessonID, courseID int64
	_ = env.DB.Raw(`SELECT max(id) FROM outbox WHERE event_type = ? AND aggregate_id = ?`, progress.EventLessonCompleted, enrollmentID).Row().Scan(&lessonID)
	_ = env.DB.Raw(`SELECT max(id) FROM outbox WHERE event_type = ? AND aggregate_id = ?`, progress.EventCourseCompleted, enrollmentID).Row().Scan(&courseID)
	if lessonID >= courseID {
		t.Errorf("lesson_completed (outbox %d) must precede course_completed (outbox %d)", lessonID, courseID)
	}
	// A completed enrollment re-confirming a lesson stays silent.
	if err := svc.MarkComplete(ctx, studentID, enrollmentID, lessonKeys[1]); err != nil {
		t.Fatal(err)
	}
	if n := outboxCount(t, env, progress.EventLessonCompleted, enrollmentID); n != 2 {
		t.Errorf("lesson_completed events after a repeat on a completed enrollment: %d, want 2", n)
	}
}

func TestMarkComplete_ConcurrentSameLessonWritesLessonCompletedOnce(t *testing.T) {
	env := testenv.Setup(t)
	svc := progress.NewService(progress.NewRepository(env.DB))
	enrollmentID, lessonKeys, studentID := seedEnrollmentWithTwoLessons(t, env)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := svc.MarkComplete(context.Background(), studentID, enrollmentID, lessonKeys[0]); err != nil {
				t.Errorf("MarkComplete: %v", err)
			}
		}()
	}
	wg.Wait()
	if n := outboxCount(t, env, progress.EventLessonCompleted, enrollmentID); n != 1 {
		t.Fatalf("lesson_completed events after 8 concurrent completions of one lesson: %d, want 1", n)
	}
}

func TestMarkComplete_RejectedRequestWritesNoLessonEvent(t *testing.T) {
	env := testenv.Setup(t)
	svc := progress.NewService(progress.NewRepository(env.DB))
	enrollmentID, _, studentID := seedEnrollmentWithTwoLessons(t, env)

	if err := svc.MarkComplete(context.Background(), studentID, enrollmentID, uuid.New()); err == nil {
		t.Fatal("an unknown lesson key must be rejected")
	}
	if err := svc.MarkComplete(context.Background(), uuid.New(), enrollmentID, uuid.New()); err == nil {
		t.Fatal("another student must be rejected")
	}
	if n := outboxCount(t, env, progress.EventLessonCompleted, enrollmentID); n != 0 {
		t.Fatalf("lesson_completed events after rejected requests: %d, want 0", n)
	}
}
