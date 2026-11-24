// Domain logic for certificates (RFC §4.3, §8.4).
package certificate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"gorm.io/gorm"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
)

// Outcome says what Issue did, so the relay handler and tests can tell a
// first issuance from a replay from a deliberate skip.
type Outcome string

const (
	OutcomeIssued        Outcome = "issued"
	OutcomeAlreadyIssued Outcome = "already_issued"
	// OutcomeSkipped means the event can never produce a certificate (no such
	// enrollment, a withdrawn one, or a student_id that does not match it).
	// Retrying cannot change that, so the handler logs the reason and
	// acknowledges the event instead of failing it.
	OutcomeSkipped Outcome = "skipped"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// IssueForCompletion issues the certificate for one completed enrollment, on
// the caller's transaction. It is idempotent: UNIQUE (enrollment_id) in the
// database is the guarantee (RFC §8.4, F13), so a duplicate or concurrent
// delivery returns OutcomeAlreadyIssued. The relay's processed_events marker
// is only the second line.
//
// RFC §8.3/§8.4: a certificate is never revoked. If the enrollment has since
// moved off `completed` (a republish added lessons, assumption A5) the event
// still stands as the fact it reported, so an `active` enrollment is issued
// to like a `completed` one, and an existing certificate is never touched.
// A `withdrawn` enrollment never completed (the status/timestamp CHECKs and
// Withdraw's guard make completed -> withdrawn unreachable), so an event for
// one is inconsistent and is skipped with a logged reason, not issued.
func (s *Service) IssueForCompletion(ctx context.Context, tx *gorm.DB, enrollmentID, studentID uuid.UUID) (Outcome, error) {
	src, err := s.repo.loadEnrollmentSource(ctx, tx, enrollmentID)
	if errors.Is(err, ErrEnrollmentNotFound) {
		slog.Warn("certificate not issued: enrollment does not exist; event acknowledged",
			slog.String("enrollment_id", enrollmentID.String()))
		return OutcomeSkipped, nil
	}
	if err != nil {
		return "", err
	}
	if src.StudentID != studentID {
		slog.Warn("certificate not issued: event student_id does not match the enrollment; event acknowledged",
			slog.String("enrollment_id", enrollmentID.String()))
		return OutcomeSkipped, nil
	}
	if src.Status == "withdrawn" {
		slog.Warn("certificate not issued: enrollment is withdrawn; event acknowledged",
			slog.String("enrollment_id", enrollmentID.String()))
		return OutcomeSkipped, nil
	}
	if src.Title == nil {
		return "", fmt.Errorf("certificate: course %s has no version to take a title from", src.CourseID)
	}

	// Cheap pre-check so a replay does not burn a random code or a name
	// lookup; the ON CONFLICT below is what makes it correct under races.
	if exists, err := s.exists(ctx, tx, enrollmentID); err != nil {
		return "", err
	} else if exists {
		return OutcomeAlreadyIssued, nil
	}

	// The name is a snapshot (RFC §5.3.4). instructor_mirror is fed by
	// identity events, which can in principle be handled after this one; an
	// error here makes the relay retry rather than issue a nameless certificate.
	name, err := s.repo.studentName(ctx, tx, src.StudentID)
	if err != nil {
		return "", err
	}
	code, err := newVerificationCode()
	if err != nil {
		return "", fmt.Errorf("generate verification code: %w", err)
	}
	cert := &Certificate{
		ID:               uuid.Must(uuid.NewV7()),
		EnrollmentID:     enrollmentID,
		StudentID:        src.StudentID,
		CourseID:         src.CourseID,
		CourseVersionID:  src.VersionID,
		VerificationCode: code,
		StudentName:      name,
		CourseTitle:      *src.Title,
	}
	inserted, err := s.repo.insertIfAbsent(ctx, tx, cert)
	if err != nil {
		return "", err
	}
	if !inserted {
		return OutcomeAlreadyIssued, nil
	}
	return OutcomeIssued, nil
}

func (s *Service) exists(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID) (bool, error) {
	var n int64
	if err := tx.WithContext(ctx).Raw(`SELECT count(*) FROM certificates WHERE enrollment_id = ?`, enrollmentID).
		Row().Scan(&n); err != nil {
		return false, fmt.Errorf("check certificate for enrollment %s: %w", enrollmentID, err)
	}
	return n > 0, nil
}

func (s *Service) ListMine(ctx context.Context, studentID uuid.UUID) ([]Certificate, error) {
	list, err := s.repo.ListByStudent(ctx, studentID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return list, nil
}

// GetByID is owner-only. Another student's certificate answers exactly like
// an unknown id (404 not_found, DR-022), so ids cannot be probed.
func (s *Service) GetByID(ctx context.Context, studentID, id uuid.UUID) (*Certificate, error) {
	c, err := s.repo.GetOwned(ctx, studentID, id)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return c, nil
}

// Verify backs the public endpoint. A malformed code is rejected before any
// query; an unknown well-formed one is a 404.
func (s *Service) Verify(ctx context.Context, code string) (*Certificate, error) {
	if !verificationCodePattern.MatchString(code) {
		return nil, apperrors.ErrValidation.Wrap(errors.New("verification code is malformed"))
	}
	c, err := s.repo.GetByCode(ctx, code)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return c, nil
}
