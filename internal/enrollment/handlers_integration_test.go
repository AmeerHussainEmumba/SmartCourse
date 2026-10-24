//go:build integration

// What this file checks, against real Postgres and the real relay (RFC
// §18.2): that archiving a course actually withdraws its active students,
// end to end through the same outbox -> relay -> handler path production
// traffic uses — not a direct call to an unexported function, which would
// only prove the function does what it does, not that it's wired up.
package enrollment_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

// TestHandleCourseArchived_WithdrawsActiveEnrollments is the regression
// test for the supervisor-flagged gap: "if a course is archived, all
// active students of it should be outrolled." One student is withdrawn
// BEFORE the archive, specifically to prove the handler only touches
// still-active enrollments and leaves an already-withdrawn one's
// withdrawn_at alone, not just that it withdraws everyone indiscriminately.
func TestHandleCourseArchived_WithdrawsActiveEnrollments(t *testing.T) {
	env := testenv.Setup(t)
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	ctx := context.Background()

	courseID := seedCourse(t, env, nil)
	studentA, studentB, studentC := seedStudent(t, env), seedStudent(t, env), seedStudent(t, env)

	for _, sid := range []uuid.UUID{studentA, studentB, studentC} {
		if _, err := svc.Enroll(ctx, sid, courseID); err != nil {
			t.Fatalf("Enroll %s: %v", sid, err)
		}
	}
	if err := svc.Withdraw(ctx, studentA, mustEnrollmentID(t, env, studentA, courseID)); err != nil {
		t.Fatalf("Withdraw studentA ahead of archive: %v", err)
	}

	var preArchiveWithdrawnAt time.Time
	if err := env.DB.Raw(`SELECT withdrawn_at FROM enrollments WHERE student_id = ? AND course_id = ?`, studentA, courseID).
		Row().Scan(&preArchiveWithdrawnAt); err != nil {
		t.Fatalf("read studentA's pre-archive withdrawn_at: %v", err)
	}

	var countBefore int
	env.DB.Raw(`SELECT enrollment_count FROM courses WHERE id = ?`, courseID).Row().Scan(&countBefore)
	if countBefore != 2 {
		t.Fatalf("expected enrollment_count 2 after one withdrawal of three enrollments, got %d", countBefore)
	}

	// The real trigger: course.ArchiveCourse writes this event in the same
	// transaction as setting archived_at. Written directly here via
	// outbox.Write (not through internal/course) because this test file,
	// like service_integration_test.go, doesn't import internal/course —
	// RFC §4.4's module boundary applies to test code too.
	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "course", AggregateID: courseID,
			EventType: "course.course_archived", SchemaVersion: 1,
			Payload: map[string]any{"course_id": courseID},
		})
	}); err != nil {
		t.Fatalf("write course.course_archived event: %v", err)
	}

	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond))
	for _, h := range enrollment.Handlers() {
		rl.Register(h)
	}
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	deadline := time.After(2500 * time.Millisecond)
	for {
		var activeCount int
		env.DB.Raw(`SELECT count(*) FROM enrollments WHERE course_id = ? AND status = 'active'`, courseID).Row().Scan(&activeCount)
		if activeCount == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("active enrollments never reached 0; still %d active after the archive event", activeCount)
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	time.Sleep(200 * time.Millisecond) // let the handler's transaction land fully before the final assertions

	var statuses []string
	env.DB.Raw(`SELECT status FROM enrollments WHERE course_id = ? ORDER BY student_id`, courseID).
		Scan(&statuses) //nolint:errcheck // assertion below catches a wrong result either way
	for _, s := range statuses {
		if s != "withdrawn" {
			t.Fatalf("expected every enrollment withdrawn after archive, got statuses %v", statuses)
		}
	}

	var countAfter int
	env.DB.Raw(`SELECT enrollment_count FROM courses WHERE id = ?`, courseID).Row().Scan(&countAfter)
	if countAfter != 0 {
		t.Fatalf("expected enrollment_count 0 after archiving withdrew the remaining active students, got %d", countAfter)
	}

	var postArchiveWithdrawnAt time.Time
	if err := env.DB.Raw(`SELECT withdrawn_at FROM enrollments WHERE student_id = ? AND course_id = ?`, studentA, courseID).
		Row().Scan(&postArchiveWithdrawnAt); err != nil {
		t.Fatalf("read studentA's post-archive withdrawn_at: %v", err)
	}
	if !postArchiveWithdrawnAt.Equal(preArchiveWithdrawnAt) {
		t.Fatalf("expected the archive handler to leave an already-withdrawn enrollment's withdrawn_at untouched; was %v, now %v",
			preArchiveWithdrawnAt, postArchiveWithdrawnAt)
	}
}

func mustEnrollmentID(t *testing.T, env *testenv.Env, studentID, courseID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := env.DB.Raw(`SELECT id FROM enrollments WHERE student_id = ? AND course_id = ?`, studentID, courseID).Row().Scan(&id); err != nil {
		t.Fatalf("look up enrollment id: %v", err)
	}
	return id
}
