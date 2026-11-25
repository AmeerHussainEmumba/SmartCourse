// Relay handler registrations for the notification-dispatcher (RFC §6.5).
// Same shape as services/analytics/analytics/handlers.go: each function has
// the libs/relay.HandlerFunc signature and runs in the transaction the
// processed_events marker commits in (RFC §6.6).
//
// A handler here only WRITES ROWS: mirrors and a delivery record. It does
// not call the queue. The relay commits the handler's transaction after the
// handler returns and offers no after-commit hook, so an enqueue made from
// inside a handler could fire for a transaction that then rolls back (or
// race ahead of its own commit). Rows that need enqueuing are found by
// EnqueuePending (dispatcher.go), which is what makes "commit, then crash"
// recoverable instead of lossy.
//
// Which events notify (RFC §4.6 "Path 3" table):
//
//	identity.user_registered     -> welcome to the user
//	enrollment.student_enrolled  -> confirmation to the student
//	progress.course_completed    -> congratulation to the student
//	course.course_published      -> "is published" to the instructor
//	progress.certificate_issued  -> "your certificate" to the student
//	course.publish_failed        -> "publishing failed" to the instructor
//
// course.course_archived has no notification-dispatcher column in the RFC
// table (and carries no recipient), so it is deliberately not consumed.
// identity.user_profile_updated and the enrollment/course mirrors feed only
// the data the templates need.
package notification

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

const consumerName = "notification-dispatcher"

// Handlers returns every relay.Handler this module registers. The worker
// registers them with its relay at startup (services/worker/cmd/worker).
func Handlers() []relay.Handler {
	return []relay.Handler{
		relay.NewHandler(consumerName, []string{"identity.user_registered"}, handleUserRegistered),
		relay.NewHandler(consumerName, []string{"identity.user_profile_updated"}, handleUserProfileUpdated),
		relay.NewHandler(consumerName, []string{"enrollment.student_enrolled"}, handleStudentEnrolled),
		relay.NewHandler(consumerName, []string{"progress.course_completed"}, handleCourseCompleted),
		relay.NewHandler(consumerName, []string{"course.course_published"}, handleCoursePublished),
		relay.NewHandler(consumerName, []string{"progress.certificate_issued"}, handleCertificateIssued),
		relay.NewHandler(consumerName, []string{"course.publish_failed"}, handlePublishFailed),
	}
}

func handleUserRegistered(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		UserID   uuid.UUID `json:"user_id"`
		Email    string    `json:"email"`
		FullName string    `json:"full_name"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode identity.user_registered payload: %w", err)
	}
	var email *string
	if payload.Email != "" {
		email = &payload.Email
	}
	if err := UpsertRecipient(ctx, tx, payload.UserID, email, payload.FullName, evt.OutboxID); err != nil {
		return err
	}
	// Deliveries parked while this user was unknown become sendable now.
	if _, err := PromoteParked(ctx, tx, &payload.UserID); err != nil {
		return err
	}
	_, err := InsertDelivery(ctx, tx, evt.EventID, KindWelcome, payload.UserID, map[string]string{})
	return err
}

func handleUserProfileUpdated(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		UserID   uuid.UUID `json:"user_id"`
		FullName string    `json:"full_name"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode identity.user_profile_updated payload: %w", err)
	}
	return UpsertRecipient(ctx, tx, payload.UserID, nil, payload.FullName, evt.OutboxID)
}

func handleStudentEnrolled(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		EnrollmentID uuid.UUID `json:"enrollment_id"`
		StudentID    uuid.UUID `json:"student_id"`
		CourseID     uuid.UUID `json:"course_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode enrollment.student_enrolled payload: %w", err)
	}
	if err := UpsertEnrollmentCourse(ctx, tx, payload.EnrollmentID, payload.CourseID); err != nil {
		return err
	}
	_, err := InsertDelivery(ctx, tx, evt.EventID, KindEnrollmentConfirm, payload.StudentID,
		map[string]string{"course_id": payload.CourseID.String()})
	return err
}

// handleCourseCompleted: the event carries enrollment_id and student_id but
// no course, so the course is resolved when the mail is rendered, from this
// module's own enrollment mirror (ADR-0024).
func handleCourseCompleted(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		EnrollmentID uuid.UUID `json:"enrollment_id"`
		StudentID    uuid.UUID `json:"student_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode progress.course_completed payload: %w", err)
	}
	_, err := InsertDelivery(ctx, tx, evt.EventID, KindCourseCompleted, payload.StudentID,
		map[string]string{"enrollment_id": payload.EnrollmentID.String()})
	return err
}

// handleCertificateIssued mails the student that the certificate exists (RFC
// §4.6: CertificateIssued -> delivery job). The event carries the course
// title, so the mail needs no mirror lookup. The verification code is in
// neither the event nor the mail.
func handleCertificateIssued(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CertificateID uuid.UUID `json:"certificate_id"`
		StudentID     uuid.UUID `json:"student_id"`
		CourseID      uuid.UUID `json:"course_id"`
		CourseTitle   string    `json:"course_title"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode progress.certificate_issued payload: %w", err)
	}
	if payload.StudentID == uuid.Nil {
		return fmt.Errorf("progress.certificate_issued payload (outbox %d) has no student_id", evt.OutboxID)
	}
	_, err := InsertDelivery(ctx, tx, evt.EventID, KindCertificateIssued, payload.StudentID,
		map[string]string{"course_id": payload.CourseID.String(), "title": payload.CourseTitle})
	return err
}

// handlePublishFailed mails the instructor that a publish failed (RFC §4.6:
// CoursePublishFailed -> instructor job; §7.4: "notify the instructor"). The
// reason is bounded at the source and clipped again when rendered.
func handlePublishFailed(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CourseID       uuid.UUID `json:"course_id"`
		Title          string    `json:"title"`
		InstructorID   string    `json:"instructor_id"`
		FailedActivity string    `json:"failed_activity"`
		Reason         string    `json:"reason"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode course.publish_failed payload: %w", err)
	}
	instructorID, err := uuid.Parse(payload.InstructorID)
	if err != nil {
		return nil // nobody to notify; analytics still counts the event
	}
	_, err = InsertDelivery(ctx, tx, evt.EventID, KindPublishFailed, instructorID, map[string]string{
		"course_id": payload.CourseID.String(), "title": payload.Title,
		"failed_activity": payload.FailedActivity, "reason": payload.Reason,
	})
	return err
}

func handleCoursePublished(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		CourseID     uuid.UUID `json:"course_id"`
		Title        string    `json:"title"`
		InstructorID string    `json:"instructor_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode course.course_published payload: %w", err)
	}
	if err := UpsertCourseTitle(ctx, tx, payload.CourseID, payload.Title, evt.OutboxID); err != nil {
		return err
	}
	// instructor_id was added in Week 3 (ADR-0030); an older event without
	// it simply has nobody to notify, which is not an error.
	instructorID, err := uuid.Parse(payload.InstructorID)
	if err != nil {
		return nil
	}
	_, err = InsertDelivery(ctx, tx, evt.EventID, KindCoursePublished, instructorID,
		map[string]string{"course_id": payload.CourseID.String(), "title": payload.Title})
	return err
}
