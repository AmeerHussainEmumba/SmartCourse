//go:build integration

// What this file checks (RFC §18.2, ADR-0024): that the mirror tables
// (repository.go's Upsert*/Insert*/Update* functions) are what
// RecomputeCourseStats/RecomputePlatformSnapshot actually read — the
// replacement for this module's pre-ADR-0024 direct reads of
// course.courses/course.enrollments/identity.users. Drives the same
// sequence handlers.go's handler functions drive (mirror write, then
// recompute), without going through the relay/JSON decoding, which is
// handlers.go's own concern, not repository.go's.
package analytics_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func TestRecomputeCourseStats_ReflectsEnrollmentMirror(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	courseID := uuid.Must(uuid.NewV7())

	enrolledA := uuid.Must(uuid.NewV7())
	enrolledB := uuid.Must(uuid.NewV7())
	withdrawn := uuid.Must(uuid.NewV7())
	now := time.Now().UTC()

	run := func(fn func(tx *gorm.DB) error) {
		t.Helper()
		if err := env.DB.Transaction(fn); err != nil {
			t.Fatalf("transaction: %v", err)
		}
	}

	// Mirrors what handleStudentEnrolled does for three enrollments.
	run(func(tx *gorm.DB) error {
		if err := analytics.InsertEnrollmentMirror(ctx, tx, enrolledA, courseID, uuid.Must(uuid.NewV7()), now); err != nil {
			return err
		}
		if err := analytics.InsertEnrollmentMirror(ctx, tx, enrolledB, courseID, uuid.Must(uuid.NewV7()), now); err != nil {
			return err
		}
		return analytics.InsertEnrollmentMirror(ctx, tx, withdrawn, courseID, uuid.Must(uuid.NewV7()), now)
	})

	// Mirrors handleStudentWithdrawn for one of the three.
	run(func(tx *gorm.DB) error {
		return analytics.UpdateEnrollmentMirrorStatus(ctx, tx, withdrawn, "withdrawn", nil)
	})

	// Mirrors handleCourseCompleted for one of the remaining two —
	// including the course_id lookup handleCourseCompleted actually uses
	// (ADR-0024: resolved from this module's own mirror, never from
	// course.enrollments).
	run(func(tx *gorm.DB) error {
		gotCourseID, err := analytics.CourseIDForEnrollment(ctx, tx, enrolledA)
		if err != nil {
			return err
		}
		if gotCourseID != courseID {
			t.Fatalf("CourseIDForEnrollment: got %s, want %s", gotCourseID, courseID)
		}
		completedAt := now.Add(time.Hour)
		return analytics.UpdateEnrollmentMirrorStatus(ctx, tx, enrolledA, "completed", &completedAt)
	})

	run(func(tx *gorm.DB) error {
		return analytics.RecomputeCourseStats(ctx, tx, courseID)
	})

	// GetCourseStats isn't built yet (README.md) — read the row directly,
	// same boundary the integration test suite uses elsewhere for
	// not-yet-exposed state.
	var total, active, completions int64
	if err := env.DB.WithContext(ctx).Raw(
		`SELECT total_enrollments, active_enrollments, completions FROM course_stats WHERE course_id = ?`, courseID,
	).Row().Scan(&total, &active, &completions); err != nil {
		t.Fatalf("read course_stats: %v", err)
	}
	// withdrawn is excluded from total_enrollments entirely (matches the
	// pre-ADR-0024 behavior: WHERE status IN ('active','completed')).
	if total != 2 {
		t.Errorf("total_enrollments: got %d, want 2", total)
	}
	if active != 1 {
		t.Errorf("active_enrollments: got %d, want 1 (enrolledB)", active)
	}
	if completions != 1 {
		t.Errorf("completions: got %d, want 1 (enrolledA)", completions)
	}
}

// TestUpsertUserRoleMirror_IgnoresOutOfOrderDelivery is a regression test:
// the relay's worker pool (default 4 workers, internal/platform/relay) has
// no per-aggregate ordering, so a user's identity.user_registered and a
// later identity.user_role_changed can have their handlers finish in
// either order. Without last_event_id's ordering guard, whichever commits
// last would win — silently reverting a promotion back to 'student' if
// the older registered event's handler happened to finish second. This
// was caught live (a seeded instructor showing up as a student in
// analytics.platform_snapshot) before this guard existed.
func TestUpsertUserRoleMirror_IgnoresOutOfOrderDelivery(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	userID := uuid.Must(uuid.NewV7())

	run := func(fn func(tx *gorm.DB) error) {
		t.Helper()
		if err := env.DB.Transaction(fn); err != nil {
			t.Fatalf("transaction: %v", err)
		}
	}

	// The role_changed event (outbox id 20) is applied FIRST, simulating
	// its handler finishing before the registered event's (outbox id 10)
	// — the exact race the ordering guard exists for.
	run(func(tx *gorm.DB) error {
		return analytics.UpsertUserRoleMirror(ctx, tx, userID, "instructor", 20)
	})
	run(func(tx *gorm.DB) error {
		return analytics.UpsertUserRoleMirror(ctx, tx, userID, "student", 10)
	})

	var role string
	if err := env.DB.WithContext(ctx).Raw(`SELECT role FROM user_role_mirror WHERE user_id = ?`, userID).
		Row().Scan(&role); err != nil {
		t.Fatalf("read user_role_mirror: %v", err)
	}
	if role != "instructor" {
		t.Errorf("role: got %q, want \"instructor\" (the older, out-of-order student event must not win)", role)
	}
}
