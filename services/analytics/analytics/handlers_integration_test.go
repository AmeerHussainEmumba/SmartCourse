//go:build integration

// What this file checks, against real Postgres as svc_analytics: that this
// module's relay handlers (handlers.go) decode the JSON payload each event is
// published with and apply the right effect, driven through the consumer's own
// path (relay.Deliver: handler plus the processed_events marker in one
// transaction), not a direct call to the repository upsert, which only proves
// the SQL is right. The payload shapes are the ones the producing services
// publish (libs/events), asserted here as literals because analytics may not
// import those services.
package analytics_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

// outboxSeq numbers the delivered events: the outbox id analytics' handlers see
// is whatever the producing service's outbox assigned, here just a counter.
var outboxSeq atomic.Int64

// writeEvent delivers one event the way the analytics-worker's Kafka consumer
// does: every handler registered for the event type runs through relay.Deliver,
// which commits its effect and the analytics.processed_events marker in one
// transaction. Analytics has no outbox, so there is nothing to write to one.
func writeEvent(t *testing.T, env *testenv.Env, aggType string, aggID uuid.UUID, eventType string, payload map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	evt := relay.DecodedEvent{
		OutboxID: outboxSeq.Add(1), EventID: uuid.New(), AggregateType: aggType, AggregateID: aggID,
		EventType: eventType, SchemaVersion: 1, Payload: raw, OccurredAt: time.Now().UTC(),
	}
	n := 0
	for _, h := range analytics.Handlers() {
		if !h.Handles(eventType) {
			continue
		}
		n++
		if err := relay.Deliver(context.Background(), env.DB, h, evt); err != nil {
			t.Fatalf("deliver %s event: %v", eventType, err)
		}
	}
	if n == 0 {
		t.Fatalf("no analytics handler handles %s", eventType)
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
	env := setup(t)
	userID := uuid.Must(uuid.NewV7())

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
	env := setup(t)
	userID := uuid.Must(uuid.NewV7())

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
	env := setup(t)
	courseID := uuid.Must(uuid.NewV7())

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
	env := setup(t)
	courseID := uuid.Must(uuid.NewV7())
	studentID := uuid.Must(uuid.NewV7())
	enrollmentID := uuid.Must(uuid.NewV7())
	enrolledAt := time.Now().UTC()

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
	env := setup(t)
	courseID := uuid.Must(uuid.NewV7())
	studentID := uuid.Must(uuid.NewV7())
	enrollmentID := uuid.Must(uuid.NewV7())

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
	env := setup(t)
	courseID := uuid.Must(uuid.NewV7())
	studentID := uuid.Must(uuid.NewV7())
	enrollmentID := uuid.Must(uuid.NewV7())

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
