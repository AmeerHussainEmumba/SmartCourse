// Domain logic and the one transaction this module exists to get right
// (RFC §4.3, §8.1, ADR-0012).
package enrollment

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/outbox"
)

var (
	errCourseNotFound     = apperrors.New(404, "course_not_found", "Course not found.")
	errCourseNotPublished = apperrors.New(409, "course_not_published", "This course is not currently open for enrollment.")
	errCourseFull         = apperrors.New(409, "course_full", "This course has reached its enrollment limit.")
	errPrerequisiteNotMet = apperrors.New(409, "prerequisite_not_met", "A prerequisite course has not been completed.")
	errAlreadyEnrolled    = apperrors.New(409, "already_enrolled", "You are already enrolled in this course.")
	errNotYourEnrollment  = apperrors.New(403, "not_your_enrollment", "This enrollment does not belong to you.")
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Enroll is RFC §8.1's transaction, verbatim. Step 4 — locking the course
// row before reading enrollment_count — is the correctness linchpin
// (ADR-0012): without it, two students racing for the last seat both read
// count=49 against a limit of 50, both pass the check, and the limit is
// silently violated. TestEnroll_SeatLimit_NeverExceededUnderConcurrency in
// service_integration_test.go is the test that would catch a regression
// here — a regression that no sequential/functional test can catch.
func (s *Service) Enroll(ctx context.Context, studentID, courseID uuid.UUID) (*Enrollment, error) {
	var result *Enrollment

	err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		course, err := lockCourseForEnrollment(ctx, tx, courseID)
		if errors.Is(err, ErrCourseNotFound) {
			return errCourseNotFound
		}
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}

		if course.PublishedVersionID == nil || course.ArchivedAt != nil {
			return errCourseNotPublished
		}
		if course.EnrollmentLimit != nil && course.EnrollmentCount >= *course.EnrollmentLimit {
			return errCourseFull
		}

		ok, err := prerequisitesSatisfied(ctx, tx, studentID, courseID)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if !ok {
			return errPrerequisiteNotMet
		}

		// EnrolledAt is set explicitly rather than left to the column's
		// DEFAULT now(): GORM includes every struct field in the INSERT
		// column list regardless of whether it was "set," so a zero-value
		// time.Time here would send 0001-01-01, not defer to the database
		// default. Caught in this package's own concurrency test log output,
		// not by inspection — see enrolled_at in a failed run's SQL trace.
		e := &Enrollment{
			ID: uuid.Must(uuid.NewV7()), StudentID: studentID, CourseID: courseID,
			Status: StatusActive, EnrolledAt: time.Now().UTC(),
		}
		if err := s.repo.CreateEnrollment(ctx, tx, e); err != nil {
			if isUniqueViolation(err) {
				return errAlreadyEnrolled
			}
			return apperrors.ErrInternal.Wrap(err)
		}

		if err := initializeProgress(ctx, tx, e.ID, *course.PublishedVersionID); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}

		if err := incrementEnrollmentCount(ctx, tx, courseID, 1); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}

		if err := outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "course", // keyed by course_id (ADR-0007): all enrollment activity for one course stays ordered on one partition
			AggregateID:   courseID,
			EventType:     "enrollment.student_enrolled",
			SchemaVersion: 1,
			Payload:       map[string]any{"enrollment_id": e.ID, "student_id": studentID, "course_id": courseID},
		}); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}

		result = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Withdraw locks the course row FIRST, exactly like Enroll — RFC §8.5's
// lock-ordering rule, not an accident of this method's own logic being
// "obviously safe." See repository.go's package doc and RFC §8.5's audit
// table for why this matters even though a single decrement UPDATE would
// be safe in isolation.
func (s *Service) Withdraw(ctx context.Context, studentID, enrollmentID uuid.UUID) error {
	e, err := s.repo.GetByID(ctx, enrollmentID)
	if errors.Is(err, ErrNotFound) {
		return apperrors.ErrNotFound
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if e.StudentID != studentID {
		return errNotYourEnrollment
	}
	if e.Status != StatusActive {
		return nil // idempotent: already withdrawn/completed is not an error
	}

	return s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := lockCourseForEnrollment(ctx, tx, e.CourseID); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if err := s.repo.UpdateStatusWithdrawn(ctx, tx, enrollmentID); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		if err := incrementEnrollmentCount(ctx, tx, e.CourseID, -1); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "course",
			AggregateID:   e.CourseID,
			EventType:     "enrollment.student_withdrawn",
			SchemaVersion: 1,
			Payload:       map[string]any{"enrollment_id": enrollmentID, "student_id": studentID, "course_id": e.CourseID},
		})
	})
}

func (s *Service) GetByID(ctx context.Context, studentID, enrollmentID uuid.UUID) (*Enrollment, error) {
	e, err := s.repo.GetByID(ctx, enrollmentID)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	// Ownership check (RFC §11.4) — loads the resource and compares
	// student_id against the authenticated caller; never trusts that the
	// client would only request their own. A missing check here is the
	// classic IDOR vulnerability.
	if e.StudentID != studentID {
		return nil, errNotYourEnrollment
	}
	return e, nil
}

func (s *Service) ListMine(ctx context.Context, studentID uuid.UUID) ([]Enrollment, error) {
	list, err := s.repo.ListForStudent(ctx, studentID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return list, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
