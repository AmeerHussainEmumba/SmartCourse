// Domain logic for progress (RFC §4.3, §8.3).
package progress

import (
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
)

var errNotYourEnrollment = apperrors.New(403, "not_your_enrollment", "This enrollment does not belong to you.")
var errLessonNotFound = apperrors.New(404, "lesson_not_found", "This lesson is not part of the course you are enrolled in.")
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
		// Row lock: serialises concurrent completions of one enrollment so
		// the last two lessons cannot each miss the other (F-06).
		e, err := s.repo.getEnrollmentForUpdate(ctx, tx, enrollmentID)
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

		inVersion, err := s.repo.lessonInPublishedVersion(ctx, tx, *e.PublishedVersionID, lessonKey)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if !inVersion {
			return errLessonNotFound
		}
		// Lessons added by a republish have no row yet; create them lazily
		// so completion is evaluated against the current version (F-07).
		if err := s.repo.ensureProgressRows(ctx, tx, enrollmentID, *e.PublishedVersionID); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}

		lessonDoneAt, lessonChanged, err := s.repo.markLessonComplete(ctx, tx, enrollmentID, lessonKey)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if lessonChanged {
			// Once per lesson completion: an idempotent repeat changed no row
			// and writes nothing (RFC §8.3).
			if err := outbox.Write(ctx, tx, outbox.Event{
				AggregateType: "enrollment",
				AggregateID:   enrollmentID,
				EventType:     EventLessonCompleted,
				SchemaVersion: 1,
				Payload:       LessonCompletedPayload{EnrollmentID: enrollmentID, StudentID: studentID, LessonKey: lessonKey, CompletedAt: lessonDoneAt},
			}); err != nil {
				return apperrors.ErrInternal.Wrap(err)
			}
		}

		complete, err := s.repo.isFullyComplete(ctx, tx, enrollmentID, *e.PublishedVersionID)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if !complete {
			return nil
		}

		completedAt, changed, err := s.repo.markEnrollmentCompleted(ctx, tx, enrollmentID)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if !changed {
			return nil // already completed by another call: no second event (F-22)
		}
		// CourseCompleted triggers certificate issuance via a Kafka
		// consumer — M3 scope (no consumer exists yet; see README.md).
		// The event is still written now, so M3's consumer can pick up
		// completions that happened before it existed, once outbox
		// replay/backfill is in place.
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "enrollment",
			AggregateID:   enrollmentID,
			EventType:     EventCourseCompleted,
			SchemaVersion: 1,
			Payload:       CourseCompletedPayload{EnrollmentID: enrollmentID, StudentID: studentID, CompletedAt: completedAt},
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
	// Lessons added by a republish have no row until first completed; show
	// them as not yet complete (F-07).
	if e.PublishedVersionID != nil {
		missing, err := s.repo.missingLessonKeys(ctx, s.repo.DB(), enrollmentID, *e.PublishedVersionID)
		if err != nil {
			return nil, apperrors.ErrInternal.Wrap(err)
		}
		for _, k := range missing {
			out = append(out, ProgressItem{LessonKey: k.String()})
		}
	}
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
