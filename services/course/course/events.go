package course

import (
	"context"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
)

// EventPublishFailed is the outbox event_type written when a version ends in
// publish_failed (RFC §6.5 CoursePublishFailed).
const EventPublishFailed = "course.publish_failed"

// Bounds on the free-text fields of the event, so a pathological activity
// error cannot bloat the outbox, Kafka or the permanent archive.
const (
	maxPublishFailedReason   = 500 // characters
	maxPublishFailedActivity = 100
)

// FailedActivityReconciler is the failed_activity of a publish the stale-publish
// reconciler released, where no workflow activity reported the failure.
const FailedActivityReconciler = "reconciler"

// PublishFailedPayload is course.v1.CoursePublishFailed (libs/events). The
// JSON names are the proto field names; TestPayloadContract_PublishFailed
// (libs/events/eventstest) keeps them in step. This module's production code
// does not import libs/events.
type PublishFailedPayload struct {
	CourseID       uuid.UUID `json:"course_id"`
	VersionID      uuid.UUID `json:"version_id"`
	VersionNumber  int       `json:"version_number"`
	Title          string    `json:"title"`
	InstructorID   uuid.UUID `json:"instructor_id"`
	FailedActivity string    `json:"failed_activity"`
	Reason         string    `json:"reason"`
	FailedAt       time.Time `json:"failed_at"`
}

// truncateRunes cuts s to at most n characters without splitting a UTF-8
// sequence and drops invalid bytes, so the payload stays valid for Protobuf
// string fields.
func truncateRunes(s string, n int) string {
	s = string([]rune(s)) // replaces invalid UTF-8 with U+FFFD
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// writePublishFailed writes course.publish_failed for a version that has just
// moved to publish_failed. It runs on the caller's transaction, the one that
// performed the transition, so the state change and the event commit together
// (ADR-0006) and the event exists exactly once per transition.
func (s *Service) writePublishFailed(ctx context.Context, tx *gorm.DB, versionID uuid.UUID, failedActivity, reason string) error {
	txRepo := s.repo.WithTx(tx)
	v, err := txRepo.GetVersionByID(ctx, versionID)
	if err != nil {
		return err
	}
	c, err := txRepo.GetCourseByID(ctx, v.CourseID)
	if err != nil {
		return err
	}
	return outbox.Write(ctx, tx, outbox.Event{
		AggregateType: "course",
		AggregateID:   v.CourseID,
		EventType:     EventPublishFailed,
		SchemaVersion: 1,
		Payload: PublishFailedPayload{
			CourseID: v.CourseID, VersionID: versionID, VersionNumber: v.VersionNumber,
			Title: v.Title, InstructorID: c.InstructorID,
			FailedActivity: truncateRunes(failedActivity, maxPublishFailedActivity),
			Reason:         truncateRunes(reason, maxPublishFailedReason),
			FailedAt:       time.Now().UTC(),
		},
	})
}
