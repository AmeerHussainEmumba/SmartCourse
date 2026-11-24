// Relay handler registration (RFC §6.5's consumer table, §8.4). Same shape as
// services/course/enrollment/handlers.go: the function has the
// libs/relay.HandlerFunc signature and receives the transaction the
// idempotency marker will commit in, so the certificate and "I've processed
// this" become atomic together (RFC §6.6).
//
// The caller (services/worker's relay wiring) registers Handlers() alongside
// the other modules' handlers; nothing in this package starts a relay.
package certificate

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

const consumerName = "certificate-issuer"

// Handlers takes no database: every query the issuing path runs goes through
// the tx the relay passes in, so the service is built with a nil-DB
// repository (only the HTTP read paths use Repository.db).
func Handlers() []relay.Handler {
	issuer := NewService(NewRepository(nil))
	return []relay.Handler{
		relay.NewHandler(consumerName, []string{"progress.course_completed"}, handleCourseCompleted(issuer)),
	}
}

// handleCourseCompleted issues one certificate per completed enrollment.
//
// A malformed payload is returned as an error: the relay records it and, once
// retries run out, gives up on the row loudly (no dead-letter store yet,
// Week 3 Phase 4). An event that decodes but can never produce a certificate
// (unknown or withdrawn enrollment) is acknowledged with a logged reason
// instead: retrying cannot help, and failing it would hold the row in the
// relay's retry loop for no gain. Both outcomes leave the stream moving.
func handleCourseCompleted(svc *Service) relay.HandlerFunc {
	return func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		var payload struct {
			EnrollmentID uuid.UUID `json:"enrollment_id"`
			StudentID    uuid.UUID `json:"student_id"`
		}
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			return fmt.Errorf("decode progress.course_completed payload: %w", err)
		}
		if payload.EnrollmentID == uuid.Nil || payload.StudentID == uuid.Nil {
			return fmt.Errorf("progress.course_completed payload (outbox %d) is missing enrollment_id or student_id", evt.OutboxID)
		}
		outcome, err := svc.IssueForCompletion(ctx, tx, payload.EnrollmentID, payload.StudentID)
		if err != nil {
			return err
		}
		slog.Debug("course_completed handled", slog.String("consumer", consumerName),
			slog.Int64("outbox_id", evt.OutboxID), slog.String("outcome", string(outcome)))
		return nil
	}
}
