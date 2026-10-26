//go:build integration

// What this file checks, against real Postgres (RFC §18.2/§18.3): this is
// where ADR-0012 is actually proven, not just documented. Seat-limit
// correctness under real concurrency cannot be verified any other way — a
// sequential test passes whether or not the row lock exists; only N
// goroutines racing for the same seats can distinguish "correct" from
// "looks correct until the second real user." Run with
// `make test-integration`; see docs/guides/testing.md, "concurrency" tier.
package enrollment_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/enrollment"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

// seedCourse creates a published course with the given enrollment limit
// (nil = unlimited) directly via SQL — deliberately not through the course
// module's Go API, so this test file has no import of internal/course
// (RFC §4.4's boundary applies to test code too, not just production
// paths).
func seedCourse(t *testing.T, env *testenv.Env, limit *int) uuid.UUID {
	t.Helper()
	ctx := context.Background()

	instructorID := uuid.Must(uuid.NewV7())
	if err := env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Instructor', 'instructor', 'active')`,
		instructorID, instructorID.String()+"@example.com").Error; err != nil {
		t.Fatalf("seed instructor: %v", err)
	}

	courseID := uuid.Must(uuid.NewV7())
	versionID := uuid.Must(uuid.NewV7())
	moduleID := uuid.Must(uuid.NewV7())
	lessonID := uuid.Must(uuid.NewV7())

	if err := env.DB.WithContext(ctx).Exec(
		`INSERT INTO courses (id, instructor_id, slug, enrollment_limit) VALUES (?, ?, ?, ?)`,
		courseID, instructorID, courseID.String(), limit).Error; err != nil {
		t.Fatalf("seed course: %v", err)
	}
	if err := env.DB.Exec(
		`INSERT INTO course_versions (id, course_id, version_number, state, title, tags) VALUES (?, ?, 1, 'ready', 'Seeded Course', '{}')`,
		versionID, courseID).Error; err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := env.DB.Exec(`INSERT INTO modules (id, course_version_id, module_key, position, title) VALUES (?, ?, ?, 1, 'Module 1')`,
		moduleID, versionID, uuid.New()).Error; err != nil {
		t.Fatalf("seed module: %v", err)
	}
	if err := env.DB.Exec(`INSERT INTO lessons (id, module_id, lesson_key, position, title, content_type) VALUES (?, ?, ?, 1, 'Lesson 1', 'text')`,
		lessonID, moduleID, uuid.New()).Error; err != nil {
		t.Fatalf("seed lesson: %v", err)
	}
	if err := env.DB.Exec(`UPDATE courses SET published_version_id = ? WHERE id = ?`, versionID, courseID).Error; err != nil {
		t.Fatalf("publish seeded course: %v", err)
	}
	return courseID
}

func seedStudent(t *testing.T, env *testenv.Env) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	if err := env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Student', 'student', 'active')`,
		id, id.String()+"@example.com").Error; err != nil {
		t.Fatalf("seed student: %v", err)
	}
	return id
}

// TestEnroll_SeatLimit_NeverExceededUnderConcurrency is, per ADR-0012's own
// text, "the single most valuable test in the suite, because the failure
// it catches is invisible to functional testing." 30 distinct students
// race for 10 seats. Without the row lock in repository.go's
// lockCourseForEnrollment, this test is flaky-to-always-fails (every
// goroutine reads the same stale count and all 30 pass the check). With
// it, exactly 10 succeed, every time.
func TestEnroll_SeatLimit_NeverExceededUnderConcurrency(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()

	const seatLimit = 10
	const contenders = 30
	limit := seatLimit
	courseID := seedCourse(t, env, &limit)

	studentIDs := make([]uuid.UUID, contenders)
	for i := range studentIDs {
		studentIDs[i] = seedStudent(t, env)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	successes, fullRejections, otherErrors := 0, 0, 0

	for _, studentID := range studentIDs {
		wg.Add(1)
		go func(sid uuid.UUID) {
			defer wg.Done()
			_, err := svc.Enroll(ctx, sid, courseID)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			default:
				if appErr, ok := apperrors.As(err); ok && appErr.Code == "course_full" {
					fullRejections++
				} else {
					otherErrors++
					t.Logf("unexpected enrollment error: %v", err)
				}
			}
		}(studentID)
	}
	wg.Wait()

	if otherErrors != 0 {
		t.Fatalf("expected only success or course_full errors, got %d unexpected errors", otherErrors)
	}
	if successes != seatLimit {
		t.Fatalf("expected exactly %d successful enrollments, got %d (seat limit violated or under-filled)", seatLimit, successes)
	}
	if fullRejections != contenders-seatLimit {
		t.Fatalf("expected %d course_full rejections, got %d", contenders-seatLimit, fullRejections)
	}

	// The counter itself must match reality — not just the count of
	// successful calls, in case a bug double-counts or under-counts.
	var actualCount int
	if err := env.DB.Raw(`SELECT enrollment_count FROM courses WHERE id = ?`, courseID).Row().Scan(&actualCount); err != nil {
		t.Fatalf("read back enrollment_count: %v", err)
	}
	if actualCount != seatLimit {
		t.Fatalf("courses.enrollment_count = %d, want %d — counter drifted from actual successful enrollments", actualCount, seatLimit)
	}

	var actualRows int64
	if err := env.DB.Raw(`SELECT count(*) FROM enrollments WHERE course_id = ? AND status = 'active'`, courseID).Row().Scan(&actualRows); err != nil {
		t.Fatalf("count enrollment rows: %v", err)
	}
	if actualRows != seatLimit {
		t.Fatalf("actual enrollment rows = %d, want %d", actualRows, seatLimit)
	}
}

// TestEnroll_DuplicateAttempt_ExactlyOneSucceeds: the same student firing
// concurrent duplicate enrollment requests (e.g. a double-clicked button)
// must result in exactly one active enrollment, enforced by the partial
// unique index (RFC §5.3.4), not by application-level de-duplication that
// could itself race.
func TestEnroll_DuplicateAttempt_ExactlyOneSucceeds(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()

	courseID := seedCourse(t, env, nil) // unlimited seats — isolates duplicate-detection from seat-limit logic
	studentID := seedStudent(t, env)

	const attempts = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes, conflicts, otherErrors := 0, 0, 0

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.Enroll(ctx, studentID, courseID)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			default:
				if appErr, ok := apperrors.As(err); ok && appErr.Code == "already_enrolled" {
					conflicts++
				} else {
					otherErrors++
					t.Logf("unexpected error: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if otherErrors != 0 {
		t.Fatalf("expected only success or already_enrolled errors, got %d unexpected errors", otherErrors)
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 successful enrollment out of %d concurrent attempts, got %d", attempts, successes)
	}
}

func TestWithdraw_ThenReenroll_Allowed(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()

	courseID := seedCourse(t, env, nil)
	studentID := seedStudent(t, env)

	e, err := svc.Enroll(ctx, studentID, courseID)
	if err != nil {
		t.Fatalf("initial enroll: %v", err)
	}
	if err := svc.Withdraw(ctx, studentID, e.ID); err != nil {
		t.Fatalf("withdraw: %v", err)
	}

	// A plain unique constraint would block this; the partial index
	// (WHERE status IN ('active','completed')) must permit it.
	if _, err := svc.Enroll(ctx, studentID, courseID); err != nil {
		t.Fatalf("expected re-enrollment after withdrawal to succeed, got: %v", err)
	}
}

func TestEnroll_UnpublishedCourse_Rejected(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()

	instructorID := uuid.Must(uuid.NewV7())
	env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Instructor', 'instructor', 'active')`,
		instructorID, instructorID.String()+"@example.com")
	courseID := uuid.Must(uuid.NewV7())
	env.DB.Exec(`INSERT INTO courses (id, instructor_id, slug) VALUES (?, ?, ?)`, courseID, instructorID, courseID.String())
	studentID := seedStudent(t, env)

	_, err := svc.Enroll(ctx, studentID, courseID)
	if err == nil {
		t.Fatal("expected enrollment in an unpublished course to be rejected")
	}
	appErr, ok := apperrors.As(err)
	if !ok || appErr.Code != "course_not_published" {
		t.Fatalf("expected course_not_published, got: %v", err)
	}
}
