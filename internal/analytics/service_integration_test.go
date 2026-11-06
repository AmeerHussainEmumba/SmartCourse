//go:build integration

// What this file checks, against real Postgres (RFC §18.2): that
// RecomputePlatformSnapshot's upsert actually lands in the row
// GetPlatformSnapshot reads back out, through the full Repository →
// Service → (what the HTTP handler calls) path — not just that the SQL
// parses. This module had no test file at all until the handler → service
// → repository layering was added (see README.md's "Bugs found during
// review" equivalent note in internal/identity, and this module's own
// history); this is the first one. Run with `make test-integration`; see
// docs/guides/testing.md.
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

func TestGetPlatformSnapshot_ReflectsRecompute(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	svc := analytics.NewService(analytics.NewRepository(env.DB))

	// No events processed yet — zeros, not an error (repository.go's own
	// documented behavior for "no row yet").
	snap, err := svc.GetPlatformSnapshot(ctx)
	if err != nil {
		t.Fatalf("GetPlatformSnapshot before any data: %v", err)
	}
	if snap.TotalStudents != 0 || snap.TotalInstructors != 0 {
		t.Fatalf("expected zeros before any recompute, got %+v", snap)
	}

	// Seed two students and one instructor directly into this module's own
	// mirror (ADR-0024) — not identity.users: analytics never reads
	// another service's tables, even in a test, same RFC §4.4 boundary
	// that applies to production code.
	for i := 0; i < 2; i++ {
		id := uuid.Must(uuid.NewV7())
		env.DB.Exec(`INSERT INTO user_role_mirror (user_id, role) VALUES (?, 'student')`, id)
	}
	instructorID := uuid.Must(uuid.NewV7())
	env.DB.Exec(`INSERT INTO user_role_mirror (user_id, role) VALUES (?, 'instructor')`, instructorID)

	// This is what the relay's analytics handler does on every relevant
	// event (internal/analytics/handlers.go) — recompute from source,
	// inside a transaction, exactly as it would in production.
	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return analytics.RecomputePlatformSnapshot(ctx, tx)
	}); err != nil {
		t.Fatalf("RecomputePlatformSnapshot: %v", err)
	}

	snap, err = svc.GetPlatformSnapshot(ctx)
	if err != nil {
		t.Fatalf("GetPlatformSnapshot after recompute: %v", err)
	}
	if snap.TotalStudents != 2 {
		t.Errorf("expected 2 students, got %d", snap.TotalStudents)
	}
	if snap.TotalInstructors != 1 {
		t.Errorf("expected 1 instructor, got %d", snap.TotalInstructors)
	}
}

// TestGetCourseStats_ReflectsRecompute closes a gap found during the
// baseline-compliance audit: RecomputeCourseStats has been writing this
// row since ADR-0024, but nothing ever read it back — no repository
// method, no Service method, no route. Proves the full read path now
// exists and actually reflects what a recompute wrote, the same way
// TestGetPlatformSnapshot_ReflectsRecompute does for the platform row.
func TestGetCourseStats_ReflectsRecompute(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	svc := analytics.NewService(analytics.NewRepository(env.DB))
	courseID := uuid.Must(uuid.NewV7())

	// No course_stats row yet — zeros, not an error (same tradeoff
	// GetPlatformSnapshot makes, and for the same cross-schema reason —
	// see GetCourseStats' doc comment in repository.go).
	stats, err := svc.GetCourseStats(ctx, courseID)
	if err != nil {
		t.Fatalf("GetCourseStats before any data: %v", err)
	}
	if stats.TotalEnrollments != 0 || stats.AvgCompletionS != nil {
		t.Fatalf("expected zeros/nil before any recompute, got %+v", stats)
	}

	studentA, studentB := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	now := time.Now().UTC()
	env.DB.Exec(`INSERT INTO enrollment_mirror (enrollment_id, course_id, student_id, status, enrolled_at, completed_at) VALUES (?, ?, ?, 'completed', ?, ?)`,
		uuid.Must(uuid.NewV7()), courseID, studentA, now.Add(-time.Hour), now)
	env.DB.Exec(`INSERT INTO enrollment_mirror (enrollment_id, course_id, student_id, status, enrolled_at) VALUES (?, ?, ?, 'active', ?)`,
		uuid.Must(uuid.NewV7()), courseID, studentB, now)

	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return analytics.RecomputeCourseStats(ctx, tx, courseID)
	}); err != nil {
		t.Fatalf("RecomputeCourseStats: %v", err)
	}

	stats, err = svc.GetCourseStats(ctx, courseID)
	if err != nil {
		t.Fatalf("GetCourseStats after recompute: %v", err)
	}
	if stats.TotalEnrollments != 2 || stats.ActiveEnrollments != 1 || stats.Completions != 1 {
		t.Fatalf("expected 2 total/1 active/1 completed, got %+v", stats)
	}
	if stats.AvgCompletionS == nil || *stats.AvgCompletionS <= 0 {
		t.Fatalf("expected a positive avg_completion_s now that one student has completed, got %+v", stats)
	}
}

// TestListDailyEnrollments_ReturnsTimeSeries is the same gap, for FR-5's
// "New Enrollments Over Time" — IncrementDailyEnrollment has written this
// table since M0/M1; nothing ever read it back before this.
func TestListDailyEnrollments_ReturnsTimeSeries(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	svc := analytics.NewService(analytics.NewRepository(env.DB))
	courseID := uuid.Must(uuid.NewV7())

	empty, err := svc.ListDailyEnrollments(ctx, courseID)
	if err != nil {
		t.Fatalf("ListDailyEnrollments before any data: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected no rows before any enrollment, got %+v", empty)
	}

	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		if err := analytics.IncrementDailyEnrollment(ctx, tx, courseID); err != nil {
			return err
		}
		return analytics.IncrementDailyEnrollment(ctx, tx, courseID)
	}); err != nil {
		t.Fatalf("IncrementDailyEnrollment: %v", err)
	}

	rows, err := svc.ListDailyEnrollments(ctx, courseID)
	if err != nil {
		t.Fatalf("ListDailyEnrollments after increments: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 day bucket for two same-day increments, got %+v", rows)
	}
	if rows[0].Count != 2 {
		t.Fatalf("expected today's bucket to count 2, got %d", rows[0].Count)
	}
}
