// Relay handler registrations (RFC §6.5's consumer table, "analytics-
// aggregator"). Each function has the internal/platform/relay.HandlerFunc
// signature: it receives the transaction the idempotency marker will
// commit in, so its effect and "I've processed this" become atomic
// together (RFC §6.6).
//
// Schema-per-service (ADR-0024): every handler below upserts into this
// module's own mirror tables (repository.go) before recomputing — never a
// direct query into another service's schema. This is what replaced the
// direct `courses`/`enrollments`/`users` reads this file had before
// ADR-0024.
package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
)

const consumerName = "analytics-aggregator"

// Handlers returns every relay.Handler this module registers. Called once
// at worker startup (internal/platform/bootstrap.BuildRelay, called from
// cmd/worker — ADR-0022) — see that package for why wiring lives there and
// not here (RFC §4.4: the composition root is the only place allowed to
// see every module).
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
	var payload struct {
		UserID uuid.UUID `json:"user_id"`
		Role   string    `json:"role"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode %s payload: %w", evt.EventType, err)
	}
	if err := UpsertUserRoleMirror(ctx, tx, payload.UserID, payload.Role, evt.OutboxID); err != nil {
		return err
	}
	return RecomputePlatformSnapshot(ctx, tx)
}

func handleCoursePublished(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CourseID uuid.UUID `json:"course_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode course.course_published payload: %w", err)
	}
	if err := UpsertCourseMirror(ctx, tx, payload.CourseID, true); err != nil {
		return err
	}
	if err := EnsureCourseStatsRow(ctx, tx, payload.CourseID); err != nil {
		return err
	}
	return RecomputePlatformSnapshot(ctx, tx)
}

func handleStudentEnrolled(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		EnrollmentID uuid.UUID `json:"enrollment_id"`
		StudentID    uuid.UUID `json:"student_id"`
		CourseID     uuid.UUID `json:"course_id"`
		EnrolledAt   time.Time `json:"enrolled_at"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode enrollment.student_enrolled payload: %w", err)
	}
	if err := InsertEnrollmentMirror(ctx, tx, payload.EnrollmentID, payload.CourseID, payload.StudentID, payload.EnrolledAt); err != nil {
		return err
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
		EnrollmentID uuid.UUID `json:"enrollment_id"`
		CourseID     uuid.UUID `json:"course_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode enrollment.student_withdrawn payload: %w", err)
	}
	if err := UpdateEnrollmentMirrorStatus(ctx, tx, payload.EnrollmentID, "withdrawn", nil); err != nil {
		return err
	}
	if err := RecomputeCourseStats(ctx, tx, payload.CourseID); err != nil {
		return err
	}
	return RecomputePlatformSnapshot(ctx, tx)
}

// handleCourseCompleted resolves course_id via this module's own
// enrollment_mirror (mirrored at enroll time by handleStudentEnrolled),
// since progress.course_completed's payload carries enrollment_id and
// student_id, not course_id — the event is deliberately minimal at the
// source; consumers that need more join for it against their own data
// (ADR-0024 — never against course.enrollments directly).
func handleCourseCompleted(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		EnrollmentID uuid.UUID `json:"enrollment_id"`
		CompletedAt  time.Time `json:"completed_at"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode progress.course_completed payload: %w", err)
	}
	courseID, err := CourseIDForEnrollment(ctx, tx, payload.EnrollmentID)
	if err != nil {
		return fmt.Errorf("look up course_id for enrollment %s: %w", payload.EnrollmentID, err)
	}
	completedAt := payload.CompletedAt
	if err := UpdateEnrollmentMirrorStatus(ctx, tx, payload.EnrollmentID, "completed", &completedAt); err != nil {
		return err
	}
	return RecomputeCourseStats(ctx, tx, courseID)
}
