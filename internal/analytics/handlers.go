// Relay handler registrations (RFC §6.5's consumer table, "analytics-
// aggregator"). Each function has the internal/platform/relay.HandlerFunc
// signature: it receives the transaction the idempotency marker will
// commit in, so its effect and "I've processed this" become atomic
// together (RFC §6.6).
package analytics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
)

const consumerName = "analytics-aggregator"

// Handlers returns every relay.Handler this module registers. Called once
// at worker startup (cmd/smartcourse/modules.go) — see that file for why
// wiring lives there and not here (RFC §4.4: the composition root is the
// only place allowed to see every module).
func Handlers() []relay.Handler {
	return []relay.Handler{
		relay.NewHandler(consumerName, []string{"identity.user_registered", "identity.user_role_changed"}, handlePlatformCounts),
		relay.NewHandler(consumerName, []string{"course.course_published"}, handleCoursePublished),
		relay.NewHandler(consumerName, []string{"enrollment.student_enrolled"}, handleStudentEnrolled),
		relay.NewHandler(consumerName, []string{"enrollment.student_withdrawn"}, handleStudentWithdrawn),
		relay.NewHandler(consumerName, []string{"progress.course_completed"}, handleCourseCompleted),
	}
}

func handlePlatformCounts(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	return RecomputePlatformSnapshot(ctx, tx)
}

func handleCoursePublished(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CourseID uuid.UUID `json:"course_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode course.course_published payload: %w", err)
	}
	if err := EnsureCourseStatsRow(ctx, tx, payload.CourseID); err != nil {
		return err
	}
	return RecomputePlatformSnapshot(ctx, tx)
}

func handleStudentEnrolled(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CourseID uuid.UUID `json:"course_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode enrollment.student_enrolled payload: %w", err)
	}
	if err := RecomputeCourseStats(ctx, tx, payload.CourseID); err != nil {
		return err
	}
	if err := IncrementDailyEnrollment(ctx, tx, payload.CourseID); err != nil {
		return err
	}
	return RecomputePlatformSnapshot(ctx, tx)
}

func handleStudentWithdrawn(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CourseID uuid.UUID `json:"course_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode enrollment.student_withdrawn payload: %w", err)
	}
	if err := RecomputeCourseStats(ctx, tx, payload.CourseID); err != nil {
		return err
	}
	return RecomputePlatformSnapshot(ctx, tx)
}

// handleCourseCompleted looks up course_id via the enrollment, since
// progress.course_completed's payload (internal/progress/service.go)
// carries enrollment_id and student_id, not course_id — the event is
// deliberately minimal at the source; consumers that need more join for
// it, rather than every event growing every possible field "just in
// case."
func handleCourseCompleted(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		EnrollmentID uuid.UUID `json:"enrollment_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode progress.course_completed payload: %w", err)
	}
	var courseID uuid.UUID
	if err := tx.WithContext(ctx).Raw(`SELECT course_id FROM enrollments WHERE id = ?`, payload.EnrollmentID).
		Row().Scan(&courseID); err != nil {
		return fmt.Errorf("look up course_id for enrollment %s: %w", payload.EnrollmentID, err)
	}
	return RecomputeCourseStats(ctx, tx, courseID)
}
