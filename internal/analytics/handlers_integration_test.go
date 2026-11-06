//go:build integration

// What this file checks, against real Postgres and the real relay (RFC
// §18.2): that this module's 5 relay handlers (handlers.go) actually
// decode a real outbox event's JSON payload and apply the right effect,
// driven through the same outbox -> relay -> handler path production
// traffic uses — not a direct call to the underlying repository upsert
// function, which (as mirror_integration_test.go and
// service_integration_test.go already do) only proves the SQL is right,
// never that the handler's own json.Unmarshal against the real payload
// shape each event is actually written with (internal/identity,
// internal/course, internal/enrollment, internal/progress) is correct.
package analytics_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func newTestRelay(env *testenv.Env) *relay.Relay {
	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond))
	for _, h := range analytics.Handlers() {
		rl.Register(h)
	}
	return rl
}

func writeEvent(t *testing.T, env *testenv.Env, aggType string, aggID uuid.UUID, eventType string, payload map[string]any) {
	t.Helper()
	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, outbox.Event{
			AggregateType: aggType, AggregateID: aggID,
			EventType: eventType, SchemaVersion: 1, Payload: payload,
		})
	}); err != nil {
		t.Fatalf("write %s event: %v", eventType, err)
	}
}

// pollUntil polls fn every 50ms until it returns true or the deadline
// passes, same shape as enrollment's handlers_integration_test.go.
func pollUntil(t *testing.T, deadline time.Duration, fn func() bool) {
	t.Helper()
	timeout := time.After(deadline)
	for {
		if fn() {
			return
		}
		select {
		case <-timeout:
			t.Fatal("condition never became true before deadline")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestHandlePlatformCounts_UserRegistered_UpsertsRoleMirrorAndRecomputesSnapshot(t *testing.T) {
	env := testenv.Setup(t)
	userID := uuid.Must(uuid.NewV7())

	rl := newTestRelay(env)
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	writeEvent(t, env, "identity", userID, "identity.user_registered", map[string]any{
		"user_id": userID, "email": "a@example.com", "role": "student", "full_name": "A Student",
	})

	pollUntil(t, 2500*time.Millisecond, func() bool {
		var role string
		err := env.DB.Raw(`SELECT role FROM user_role_mirror WHERE user_id = ?`, userID).Row().Scan(&role)
		return err == nil && role == "student"
	})

	var totalStudents int64
	env.DB.Raw(`SELECT total_students FROM platform_snapshot WHERE id = true`).Row().Scan(&totalStudents)
	if totalStudents < 1 {
		t.Fatalf("expected platform_snapshot.total_students >= 1 after a real user_registered event, got %d", totalStudents)
	}
}

func TestHandlePlatformCounts_UserRoleChanged_UpdatesRoleMirror(t *testing.T) {
	env := testenv.Setup(t)
	userID := uuid.Must(uuid.NewV7())

	rl := newTestRelay(env)
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	writeEvent(t, env, "identity", userID, "identity.user_registered", map[string]any{
		"user_id": userID, "email": "b@example.com", "role": "student", "full_name": "B Student",
	})
	pollUntil(t, 2500*time.Millisecond, func() bool {
		var role string
		err := env.DB.Raw(`SELECT role FROM user_role_mirror WHERE user_id = ?`, userID).Row().Scan(&role)
		return err == nil && role == "student"
	})

	writeEvent(t, env, "identity", userID, "identity.user_role_changed", map[string]any{
		"user_id": userID, "role": "instructor", "status": "active",
	})
	pollUntil(t, 2500*time.Millisecond, func() bool {
		var role string
		err := env.DB.Raw(`SELECT role FROM user_role_mirror WHERE user_id = ?`, userID).Row().Scan(&role)
		return err == nil && role == "instructor"
	})
}

func TestHandleCoursePublished_UpsertsCourseMirrorAndStatsRow(t *testing.T) {
	env := testenv.Setup(t)
	courseID := uuid.Must(uuid.NewV7())

	rl := newTestRelay(env)
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	writeEvent(t, env, "course", courseID, "course.course_published", map[string]any{
		"course_id": courseID, "version_id": uuid.Must(uuid.NewV7()), "title": "A Course",
	})

	pollUntil(t, 2500*time.Millisecond, func() bool {
		var published bool
		err := env.DB.Raw(`SELECT published FROM course_mirror WHERE course_id = ?`, courseID).Row().Scan(&published)
		return err == nil && published
	})

	var statsRows int64
	env.DB.Raw(`SELECT count(*) FROM course_stats WHERE course_id = ?`, courseID).Row().Scan(&statsRows)
	if statsRows != 1 {
		t.Fatalf("expected EnsureCourseStatsRow to have created exactly 1 course_stats row, got %d", statsRows)
	}
}

func TestHandleStudentEnrolled_InsertsEnrollmentMirror(t *testing.T) {
	env := testenv.Setup(t)
	courseID := uuid.Must(uuid.NewV7())
	studentID := uuid.Must(uuid.NewV7())
	enrollmentID := uuid.Must(uuid.NewV7())
	enrolledAt := time.Now().UTC()

	rl := newTestRelay(env)
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	writeEvent(t, env, "enrollment", enrollmentID, "enrollment.student_enrolled", map[string]any{
		"enrollment_id": enrollmentID, "student_id": studentID, "course_id": courseID, "enrolled_at": enrolledAt,
	})

	pollUntil(t, 2500*time.Millisecond, func() bool {
		var status string
		err := env.DB.Raw(`SELECT status FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID).Row().Scan(&status)
		return err == nil && status == "active"
	})

	var mirroredCourseID, mirroredStudentID uuid.UUID
	env.DB.Raw(`SELECT course_id, student_id FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID).
		Row().Scan(&mirroredCourseID, &mirroredStudentID)
	if mirroredCourseID != courseID || mirroredStudentID != studentID {
		t.Fatalf("expected mirror row to carry course_id=%s student_id=%s, got course_id=%s student_id=%s",
			courseID, studentID, mirroredCourseID, mirroredStudentID)
	}

	var totalEnrollments int64
	env.DB.Raw(`SELECT total_enrollments FROM course_stats WHERE course_id = ?`, courseID).Row().Scan(&totalEnrollments)
	if totalEnrollments != 1 {
		t.Fatalf("expected RecomputeCourseStats to show 1 total_enrollments, got %d", totalEnrollments)
	}
}

func TestHandleStudentWithdrawn_UpdatesEnrollmentMirrorStatus(t *testing.T) {
	env := testenv.Setup(t)
	courseID := uuid.Must(uuid.NewV7())
	studentID := uuid.Must(uuid.NewV7())
	enrollmentID := uuid.Must(uuid.NewV7())

	rl := newTestRelay(env)
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	writeEvent(t, env, "enrollment", enrollmentID, "enrollment.student_enrolled", map[string]any{
		"enrollment_id": enrollmentID, "student_id": studentID, "course_id": courseID, "enrolled_at": time.Now().UTC(),
	})
	pollUntil(t, 2500*time.Millisecond, func() bool {
		var status string
		err := env.DB.Raw(`SELECT status FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID).Row().Scan(&status)
		return err == nil && status == "active"
	})

	writeEvent(t, env, "enrollment", enrollmentID, "enrollment.student_withdrawn", map[string]any{
		"enrollment_id": enrollmentID, "student_id": studentID, "course_id": courseID,
	})
	pollUntil(t, 2500*time.Millisecond, func() bool {
		var status string
		err := env.DB.Raw(`SELECT status FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID).Row().Scan(&status)
		return err == nil && status == "withdrawn"
	})

	var activeEnrollments int64
	env.DB.Raw(`SELECT active_enrollments FROM course_stats WHERE course_id = ?`, courseID).Row().Scan(&activeEnrollments)
	if activeEnrollments != 0 {
		t.Fatalf("expected RecomputeCourseStats to show 0 active_enrollments after withdrawal, got %d", activeEnrollments)
	}
}

// TestHandleCourseCompleted_ResolvesCourseIDAndUpdatesStats also proves
// CourseIDForEnrollment (handlers.go's doc comment: progress.
// course_completed's payload carries enrollment_id/student_id, not
// course_id, so the handler must resolve it from this module's own
// enrollment_mirror row, seeded here by a real student_enrolled event
// first) — a real cross-handler dependency, not just a single handler in
// isolation.
func TestHandleCourseCompleted_ResolvesCourseIDAndUpdatesStats(t *testing.T) {
	env := testenv.Setup(t)
	courseID := uuid.Must(uuid.NewV7())
	studentID := uuid.Must(uuid.NewV7())
	enrollmentID := uuid.Must(uuid.NewV7())

	rl := newTestRelay(env)
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	writeEvent(t, env, "enrollment", enrollmentID, "enrollment.student_enrolled", map[string]any{
		"enrollment_id": enrollmentID, "student_id": studentID, "course_id": courseID, "enrolled_at": time.Now().UTC(),
	})
	pollUntil(t, 2500*time.Millisecond, func() bool {
		var status string
		err := env.DB.Raw(`SELECT status FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID).Row().Scan(&status)
		return err == nil && status == "active"
	})

	completedAt := time.Now().UTC()
	writeEvent(t, env, "progress", enrollmentID, "progress.course_completed", map[string]any{
		"enrollment_id": enrollmentID, "student_id": studentID, "completed_at": completedAt,
	})

	pollUntil(t, 2500*time.Millisecond, func() bool {
		var status string
		err := env.DB.Raw(`SELECT status FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID).Row().Scan(&status)
		return err == nil && status == "completed"
	})

	var completions int64
	env.DB.Raw(`SELECT completions FROM course_stats WHERE course_id = ?`, courseID).Row().Scan(&completions)
	if completions != 1 {
		t.Fatalf("expected RecomputeCourseStats (resolved via CourseIDForEnrollment) to show 1 completion for course %s, got %d", courseID, completions)
	}
}
