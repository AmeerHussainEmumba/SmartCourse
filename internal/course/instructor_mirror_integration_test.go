//go:build integration

// What this file checks (RFC §18.2, ADR-0024): instructor_mirror, this
// module's own denormalized copy of display names, replacing the
// cross-schema JOIN ListCatalogue used to do against identity.users.
package course_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

// TestUpsertInstructorMirror_IgnoresOutOfOrderDelivery is the same
// regression this module's README documents borrowing from
// internal/analytics: the relay's worker pool has no per-aggregate
// ordering, so a user's identity.user_registered and a later identity.
// user_profile_updated can have their handlers finish in either order.
// Without last_event_id's guard, whichever commits last would win.
func TestUpsertInstructorMirror_IgnoresOutOfOrderDelivery(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	userID := uuid.Must(uuid.NewV7())

	// The profile_updated event (outbox id 20) is applied FIRST, simulating
	// its handler finishing before the registered event's (outbox id 10).
	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return course.UpsertInstructorMirror(ctx, tx, userID, "New Name", 20)
	}); err != nil {
		t.Fatalf("upsert (newer, first): %v", err)
	}
	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return course.UpsertInstructorMirror(ctx, tx, userID, "Original Name", 10)
	}); err != nil {
		t.Fatalf("upsert (older, second): %v", err)
	}

	var fullName string
	if err := env.DB.WithContext(ctx).Raw(`SELECT full_name FROM instructor_mirror WHERE user_id = ?`, userID).
		Row().Scan(&fullName); err != nil {
		t.Fatalf("read instructor_mirror: %v", err)
	}
	if fullName != "New Name" {
		t.Errorf("full_name: got %q, want \"New Name\" (the older, out-of-order registration event must not win)", fullName)
	}
}
