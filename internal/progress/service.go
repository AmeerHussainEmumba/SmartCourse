// Domain logic for progress (RFC §4.3, §8.3).
package progress

import (
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/outbox"
)

var errNotYourEnrollment = apperrors.New(403, "not_your_enrollment", "This enrollment does not belong to you.")
var errEnrollmentNotActive = apperrors.New(409, "enrollment_not_active", "This enrollment is not active.")

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// MarkComplete is RFC §8.3's transaction: mark the lesson complete
// (idempotent no-op if already done), then check whether every lesson in
// the CURRENT published version is now complete — evaluated in the same
// transaction so completion status can never be read mid-update.
func (s *Service) MarkComplete(ctx context.Context, studentID, enrollmentID, lessonKey uuid.UUID) error {
	return s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		e, err := s.repo.getEnrollment(ctx, tx, enrollmentID)
		if err != nil {
			return apperrors.ErrNotFound
		}
		if e.StudentID != studentID {
			return errNotYourEnrollment
		}
		if e.Status != "active" {
			// Idempotent by design (RFC §8.3): a completed enrollment
			// re-confirming a lesson is a no-op, not an error — but a
			// withdrawn one can't progress.
			if e.Status == "completed" {
				return nil
			}
			return errEnrollmentNotActive
		}
		if e.PublishedVersionID == nil {
			return errEnrollmentNotActive
		}

		if err := s.repo.markLessonComplete(ctx, tx, enrollmentID, lessonKey); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}

		complete, err := s.repo.isFullyComplete(ctx, tx, enrollmentID, *e.PublishedVersionID)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if !complete {
			return nil
		}

		if err := s.repo.markEnrollmentCompleted(ctx, tx, enrollmentID); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		// CourseCompleted triggers certificate issuance via a Kafka
		// consumer — M3 scope (no consumer exists yet; see README.md).
		// The event is still written now, so M3's consumer can pick up
		// completions that happened before it existed, once outbox
		// replay/backfill is in place.
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "enrollment",
			AggregateID:   enrollmentID,
			EventType:     "progress.course_completed",
			SchemaVersion: 1,
			Payload:       map[string]any{"enrollment_id": enrollmentID, "student_id": studentID},
		})
	})
}

type ProgressItem struct {
	LessonKey   string  `json:"lesson_key"`
	CompletedAt *string `json:"completed_at,omitempty"`
}

func (s *Service) GetProgress(ctx context.Context, studentID, enrollmentID uuid.UUID) ([]ProgressItem, error) {
	e, err := s.repo.getEnrollment(ctx, s.repo.DB(), enrollmentID)
	if err != nil {
		return nil, apperrors.ErrNotFound
	}
	if e.StudentID != studentID {
		return nil, errNotYourEnrollment
	}

	rows, err := s.repo.ListProgress(ctx, enrollmentID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	out := make([]ProgressItem, 0, len(rows))
	for _, row := range rows {
		item := ProgressItem{LessonKey: row.LessonKey.String()}
		if row.CompletedAt != nil {
			s := row.CompletedAt.Format("2006-01-02T15:04:05Z07:00")
			item.CompletedAt = &s
		}
		out = append(out, item)
	}
	return out, nil
}
