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

	// Seed two students and one instructor directly via SQL — this test
	// has no dependency on internal/identity's Go types (RFC §4.4 applies
	// to test code too), same pattern internal/enrollment's and
	// internal/progress's integration tests use.
	for i := 0; i < 2; i++ {
		id := uuid.Must(uuid.NewV7())
		env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Student', 'student', 'active')`,
			id, id.String()+"@example.com")
	}
	instructorID := uuid.Must(uuid.NewV7())
	env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Instructor', 'instructor', 'active')`,
		instructorID, instructorID.String()+"@example.com")

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
