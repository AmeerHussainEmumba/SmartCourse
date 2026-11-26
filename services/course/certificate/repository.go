// The only file in this module that issues SQL directly (RFC §5.9).
//
// Issuing a certificate reads `enrollments`, `courses`, `course_versions`
// and `instructor_mirror`, tables other modules own, via raw SQL projections
// rather than their Go types (RFC §4.4) — the same arrangement as
// services/course/progress. Every issuing read and the INSERT run on the tx
// the relay hands the handler, so the certificate and the processed_events
// marker commit together (RFC §6.6).
package certificate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository { return &Repository{db: db} }

var ErrNotFound = errors.New("certificate: not found")
var ErrEnrollmentNotFound = errors.New("certificate: enrollment not found")
var ErrStudentNameUnknown = errors.New("certificate: no display name mirrored for student yet")

// enrollmentSource is the minimal projection issuing needs: who completed
// what, plus the title of the version they completed. Not enrollment.Enrollment.
type enrollmentSource struct {
	ID        uuid.UUID
	StudentID uuid.UUID
	CourseID  uuid.UUID
	Status    string
	VersionID *uuid.UUID
	Title     *string
}

// loadEnrollmentSource reads the enrollment and the course version to
// snapshot. The version is the published one (completion is evaluated against
// it, RFC A5); a course with none published falls back to its newest version
// so the title is still the course's own, never invented.
func (r *Repository) loadEnrollmentSource(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID) (*enrollmentSource, error) {
	var s enrollmentSource
	err := tx.WithContext(ctx).Raw(`
		SELECT e.id, e.student_id, e.course_id, e.status::text, v.id, v.title
		FROM enrollments e
		JOIN courses c ON c.id = e.course_id
		LEFT JOIN LATERAL (
			SELECT cv.id, cv.title FROM course_versions cv
			WHERE cv.course_id = c.id
			ORDER BY (cv.id = c.published_version_id) DESC, cv.version_number DESC
			LIMIT 1
		) v ON true
		WHERE e.id = ?`, enrollmentID).
		Row().Scan(&s.ID, &s.StudentID, &s.CourseID, &s.Status, &s.VersionID, &s.Title)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrEnrollmentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load enrollment %s: %w", enrollmentID, err)
	}
	return &s, nil
}

// studentName reads the display name from course's own instructor_mirror
// (ADR-0024), never from the identity schema.
func (r *Repository) studentName(ctx context.Context, tx *gorm.DB, studentID uuid.UUID) (string, error) {
	var name string
	err := tx.WithContext(ctx).Raw(`SELECT full_name FROM instructor_mirror WHERE user_id = ?`, studentID).
		Row().Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrStudentNameUnknown
	}
	if err != nil {
		return "", fmt.Errorf("read display name for %s: %w", studentID, err)
	}
	return name, nil
}

// insertIfAbsent is the exactly-once guarantee: UNIQUE (enrollment_id) plus
// ON CONFLICT DO NOTHING. Concurrent inserts for one enrollment serialise on
// the unique index; exactly one reports inserted=true, the rest see the
// committed row and report false without erroring (and without aborting the
// surrounding transaction, which a bare INSERT-then-catch-23505 would). It
// returns the issued_at the database stamped, so the event carries the row's
// own value.
func (r *Repository) insertIfAbsent(ctx context.Context, tx *gorm.DB, c *Certificate) (time.Time, bool, error) {
	var issuedAt []time.Time
	err := tx.WithContext(ctx).Raw(`
		INSERT INTO certificates
			(id, enrollment_id, student_id, course_id, course_version_id, verification_code, student_name, course_title)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (enrollment_id) DO NOTHING
		RETURNING issued_at`,
		c.ID, c.EnrollmentID, c.StudentID, c.CourseID, c.CourseVersionID, c.VerificationCode, c.StudentName, c.CourseTitle).
		Scan(&issuedAt).Error
	if err != nil {
		return time.Time{}, false, fmt.Errorf("insert certificate for enrollment %s: %w", c.EnrollmentID, err)
	}
	if len(issuedAt) != 1 {
		return time.Time{}, false, nil
	}
	return issuedAt[0].UTC(), true, nil
}

func (r *Repository) ListByStudent(ctx context.Context, studentID uuid.UUID) ([]Certificate, error) {
	var out []Certificate
	err := r.db.WithContext(ctx).
		Where("student_id = ?", studentID).Order("issued_at DESC, id DESC").Find(&out).Error
	return out, err
}

// GetOwned filters on student_id in the query itself, so another student's
// certificate and a nonexistent id are the same ErrNotFound (DR-022).
func (r *Repository) GetOwned(ctx context.Context, studentID, id uuid.UUID) (*Certificate, error) {
	var c Certificate
	err := r.db.WithContext(ctx).Where("id = ? AND student_id = ?", id, studentID).Take(&c).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	return &c, err
}

func (r *Repository) GetByCode(ctx context.Context, code string) (*Certificate, error) {
	var c Certificate
	err := r.db.WithContext(ctx).Where("verification_code = ?", code).Take(&c).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	return &c, err
}
