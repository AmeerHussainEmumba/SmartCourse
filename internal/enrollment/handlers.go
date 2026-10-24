// Relay handler registrations (RFC §6.5's consumer table). Same shape as
// internal/analytics/handlers.go: each function has the
// internal/platform/relay.HandlerFunc signature, receiving the transaction
// the idempotency marker will commit in, so the effect and "I've processed
// this" become atomic together (RFC §6.6).
package enrollment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
)

const consumerName = "enrollment-course-archival"

func Handlers() []relay.Handler {
	return []relay.Handler{
		relay.NewHandler(consumerName, []string{"course.course_archived"}, handleCourseArchived),
	}
}

// handleCourseArchived withdraws every active enrollment in the archived
// course — "if a course is archived, all active students of it should be
// outrolled." One bulk UPDATE rather than loading rows and calling
// Withdraw per student: there's no per-student business rule left to
// apply (ownership, "is this my enrollment" — none of that is meaningful
// for a system-triggered, course-wide withdrawal), so the single-student
// path's machinery doesn't fit here.
//
// The course row is still locked first, exactly like Enroll/Withdraw
// (RFC §8.5) — not because anything here races against a new enrollment
// (Enroll already refuses once archived_at is set), but because this
// handler also decrements enrollment_count, and that counter has exactly
// one lock-ordering rule for every transaction that touches it.
//
// This does not also emit enrollment.student_withdrawn per enrollment.
// analytics.RecomputeCourseStats/RecomputePlatformSnapshot both
// recompute from live `enrollments` rows rather than tracking deltas, and
// ADR-0017's scheduled reconciliation is the documented mechanism for
// closing any gap until something else touches that course or the
// platform snapshot — not a gap this handler needs to also close.
func handleCourseArchived(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CourseID uuid.UUID `json:"course_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode course.course_archived payload: %w", err)
	}

	if _, err := lockCourseForEnrollment(ctx, tx, payload.CourseID); err != nil {
		if errors.Is(err, ErrCourseNotFound) {
			return nil // courses are never hard-deleted, but a missing row has nothing left to withdraw
		}
		return fmt.Errorf("lock course row: %w", err)
	}

	now := time.Now().UTC()
	result := tx.WithContext(ctx).Model(&Enrollment{}).
		Where("course_id = ? AND status = ?", payload.CourseID, StatusActive).
		Updates(map[string]any{"status": StatusWithdrawn, "withdrawn_at": now})
	if result.Error != nil {
		return fmt.Errorf("withdraw active enrollments for course %s: %w", payload.CourseID, result.Error)
	}
	if result.RowsAffected == 0 {
		return nil
	}
	if err := incrementEnrollmentCount(ctx, tx, payload.CourseID, -int(result.RowsAffected)); err != nil {
		return fmt.Errorf("decrement enrollment_count for course %s: %w", payload.CourseID, err)
	}
	return nil
}
