//go:build integration

// What this file checks, against real Postgres (RFC §18.2): marking a
// lesson complete is idempotent; an enrollment transitions to `completed`
// only once every lesson in the current published version is done, not
// before; and ownership is enforced. Run with `make test-integration`; see
// docs/guides/testing.md.
package progress_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/progress"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

// seedEnrollmentWithTwoLessons builds a published course with two lessons
// and an active enrollment with both lesson_progress rows pre-created —
// exactly what internal/enrollment.Enroll does, replicated here via raw SQL
// so this test file has no dependency on internal/enrollment or
// internal/course (RFC §4.4 applies to test code too).
func seedEnrollmentWithTwoLessons(t *testing.T, env *testenv.Env) (enrollmentID uuid.UUID, lessonKeys [2]uuid.UUID, studentID uuid.UUID) {
	t.Helper()

	instructorID := uuid.Must(uuid.NewV7())
	env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Instructor', 'instructor', 'active')`,
		instructorID, instructorID.String()+"@example.com")

	studentID = uuid.Must(uuid.NewV7())
	env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Student', 'student', 'active')`,
		studentID, studentID.String()+"@example.com")

	courseID := uuid.Must(uuid.NewV7())
	versionID := uuid.Must(uuid.NewV7())
	moduleID := uuid.Must(uuid.NewV7())
	lessonKeys = [2]uuid.UUID{uuid.New(), uuid.New()}

	env.DB.Exec(`INSERT INTO courses (id, instructor_id, slug, published_version_id) VALUES (?, ?, ?, NULL)`,
		courseID, instructorID, courseID.String())
	env.DB.Exec(`INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 1, 'ready', 'Course', '{}')`,
		versionID, courseID)
	env.DB.Exec(`INSERT INTO modules (id, course_version_id, module_key, position, title) VALUES (?, ?, ?, 1, 'Module 1')`,
		moduleID, versionID, uuid.New())
	env.DB.Exec(`INSERT INTO lessons (id, module_id, lesson_key, position, title, content_type) VALUES (?, ?, ?, 1, 'Lesson A', 'text')`,
		uuid.Must(uuid.NewV7()), moduleID, lessonKeys[0])
	env.DB.Exec(`INSERT INTO lessons (id, module_id, lesson_key, position, title, content_type) VALUES (?, ?, ?, 2, 'Lesson B', 'text')`,
		uuid.Must(uuid.NewV7()), moduleID, lessonKeys[1])
	env.DB.Exec(`UPDATE courses SET published_version_id = ? WHERE id = ?`, versionID, courseID)

	enrollmentID = uuid.Must(uuid.NewV7())
	env.DB.Exec(`INSERT INTO enrollments (id, student_id, course_id, status) VALUES (?, ?, ?, 'active')`,
		enrollmentID, studentID, courseID)
	for _, key := range lessonKeys {
		env.DB.Exec(`INSERT INTO lesson_progress (id, enrollment_id, lesson_key) VALUES (?, ?, ?)`,
			uuid.Must(uuid.NewV7()), enrollmentID, key)
	}
	return enrollmentID, lessonKeys, studentID
}

func TestMarkComplete_TransitionsEnrollmentOnlyWhenAllLessonsDone(t *testing.T) {
	env := testenv.Setup(t)
	svc := progress.NewService(progress.NewRepository(env.DB))
	ctx := context.Background()

	enrollmentID, lessonKeys, studentID := seedEnrollmentWithTwoLessons(t, env)

	if err := svc.MarkComplete(ctx, studentID, enrollmentID, lessonKeys[0]); err != nil {
		t.Fatalf("mark first lesson complete: %v", err)
	}

	var status string
	env.DB.Raw(`SELECT status FROM enrollments WHERE id = ?`, enrollmentID).Row().Scan(&status)
	if status != "active" {
		t.Fatalf("expected enrollment still active after 1 of 2 lessons, got %s", status)
	}

	if err := svc.MarkComplete(ctx, studentID, enrollmentID, lessonKeys[1]); err != nil {
		t.Fatalf("mark second lesson complete: %v", err)
	}

	env.DB.Raw(`SELECT status FROM enrollments WHERE id = ?`, enrollmentID).Row().Scan(&status)
	if status != "completed" {
		t.Fatalf("expected enrollment completed after both lessons done, got %s", status)
	}

	var completedAtIsNull bool
	env.DB.Raw(`SELECT completed_at IS NULL FROM enrollments WHERE id = ?`, enrollmentID).Row().Scan(&completedAtIsNull)
	if completedAtIsNull {
		t.Fatal("expected completed_at to be set once the enrollment transitions to completed")
	}
}

func TestMarkComplete_IsIdempotent(t *testing.T) {
	env := testenv.Setup(t)
	svc := progress.NewService(progress.NewRepository(env.DB))
	ctx := context.Background()

	enrollmentID, lessonKeys, studentID := seedEnrollmentWithTwoLessons(t, env)

	if err := svc.MarkComplete(ctx, studentID, enrollmentID, lessonKeys[0]); err != nil {
		t.Fatalf("first call: %v", err)
	}
	var firstCompletedAt string
	env.DB.Raw(`SELECT completed_at FROM lesson_progress WHERE enrollment_id = ? AND lesson_key = ?`, enrollmentID, lessonKeys[0]).
		Row().Scan(&firstCompletedAt)

	// Calling it again on an already-completed lesson must be a no-op
	// (RFC §8.3) — same completed_at, no error.
	if err := svc.MarkComplete(ctx, studentID, enrollmentID, lessonKeys[0]); err != nil {
		t.Fatalf("second (idempotent) call: %v", err)
	}
	var secondCompletedAt string
	env.DB.Raw(`SELECT completed_at FROM lesson_progress WHERE enrollment_id = ? AND lesson_key = ?`, enrollmentID, lessonKeys[0]).
		Row().Scan(&secondCompletedAt)

	if firstCompletedAt != secondCompletedAt {
		t.Fatalf("expected completed_at to be unchanged by a repeat call, got %s then %s", firstCompletedAt, secondCompletedAt)
	}
}

func TestMarkComplete_RejectsNonOwner(t *testing.T) {
	env := testenv.Setup(t)
	svc := progress.NewService(progress.NewRepository(env.DB))
	ctx := context.Background()

	enrollmentID, lessonKeys, _ := seedEnrollmentWithTwoLessons(t, env)
	someoneElse := uuid.Must(uuid.NewV7())

	err := svc.MarkComplete(ctx, someoneElse, enrollmentID, lessonKeys[0])
	if err == nil {
		t.Fatal("expected marking progress on someone else's enrollment to be rejected")
	}
	appErr, ok := apperrors.As(err)
	if !ok || appErr.Code != "not_your_enrollment" {
		t.Fatalf("expected not_your_enrollment, got: %v", err)
	}
}
