// The only file in this module that issues SQL directly (RFC §5.9).
//
// Two operations here read/write the `courses` and `lesson_progress`
// tables — schemas "owned" by the course and progress modules
// respectively — via raw SQL rather than GORM models. This is deliberate,
// not a boundary violation: ADR-0012's row lock and the atomic counter
// increment MUST run inside enrollment's own transaction, which is only
// possible if enrollment issues that SQL itself. Calling into another
// module's Go API here would mean a second connection/transaction, which
// defeats the lock entirely. See RFC §8.5 for the lock-ordering rule this
// file follows, and internal/enrollment/README.md for the longer version
// of this rationale.
package enrollment

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

var ErrNotFound = errors.New("enrollment: not found")
var ErrCourseNotFound = errors.New("enrollment: course not found")
var ErrCourseNotPublished = errors.New("enrollment: course is not published")
var ErrCourseFull = errors.New("enrollment: course is full")
var ErrPrerequisiteNotMet = errors.New("enrollment: a prerequisite course has not been completed")
var ErrAlreadyEnrolled = errors.New("enrollment: student is already enrolled")

// courseLockRow is the minimal projection enrollment needs from `courses`
// — not course.Course, so this file has no dependency on internal/course.
type courseLockRow struct {
	ID                 uuid.UUID
	PublishedVersionID *uuid.UUID
	ArchivedAt         *time.Time
	EnrollmentLimit    *int
	EnrollmentCount    int
}

// lockCourseForEnrollment is ADR-0012 and RFC §8.5's rule made concrete:
// every transaction that touches both `courses` and `enrollments` locks
// `courses` FIRST. Called by both Enroll and Withdraw.
func lockCourseForEnrollment(ctx context.Context, tx *gorm.DB, courseID uuid.UUID) (*courseLockRow, error) {
	var row courseLockRow
	err := tx.WithContext(ctx).Raw(
		`SELECT id, published_version_id, archived_at, enrollment_limit, enrollment_count
		 FROM courses WHERE id = ? FOR UPDATE`, courseID,
	).Row().Scan(&row.ID, &row.PublishedVersionID, &row.ArchivedAt, &row.EnrollmentLimit, &row.EnrollmentCount)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCourseNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock course row: %w", err)
	}
	return &row, nil
}

func prerequisitesSatisfied(ctx context.Context, tx *gorm.DB, studentID, courseID uuid.UUID) (bool, error) {
	var unmetCount int64
	err := tx.WithContext(ctx).Raw(`
		SELECT count(*) FROM course_prerequisites cp
		WHERE cp.course_id = ?
		AND NOT EXISTS (
			SELECT 1 FROM enrollments e
			WHERE e.student_id = ? AND e.course_id = cp.prerequisite_id AND e.status = 'completed'
		)`, courseID, studentID).Row().Scan(&unmetCount)
	if err != nil {
		return false, fmt.Errorf("check prerequisites: %w", err)
	}
	return unmetCount == 0, nil
}

func incrementEnrollmentCount(ctx context.Context, tx *gorm.DB, courseID uuid.UUID, delta int) error {
	return tx.WithContext(ctx).Exec(
		`UPDATE courses SET enrollment_count = enrollment_count + ?, updated_at = now() WHERE id = ?`,
		delta, courseID,
	).Error
}

// initializeProgress pre-creates a `lesson_progress` row for every lesson
// in the published version being enrolled into (RFC §8.1 step 9) — a
// deliberate trade of a few extra inserts now for progress reads that
// never handle a missing row later. UUIDv7 is generated here in Go, one per
// row, rather than DB-side, per ADR-0005.
func initializeProgress(ctx context.Context, tx *gorm.DB, enrollmentID, publishedVersionID uuid.UUID) error {
	rows, err := tx.WithContext(ctx).Raw(`
		SELECT l.lesson_key FROM lessons l
		JOIN modules m ON m.id = l.module_id
		WHERE m.course_version_id = ?`, publishedVersionID).Rows()
	if err != nil {
		return fmt.Errorf("list lesson keys: %w", err)
	}
	var lessonKeys []uuid.UUID
	for rows.Next() {
		var key uuid.UUID
		if err := rows.Scan(&key); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan lesson key: %w", err)
		}
		lessonKeys = append(lessonKeys, key)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, key := range lessonKeys {
		if err := tx.WithContext(ctx).Exec(
			`INSERT INTO lesson_progress (id, enrollment_id, lesson_key) VALUES (?, ?, ?)`,
			uuid.Must(uuid.NewV7()), enrollmentID, key,
		).Error; err != nil {
			return fmt.Errorf("insert lesson_progress row: %w", err)
		}
	}
	return nil
}

func (r *Repository) CreateEnrollment(ctx context.Context, tx *gorm.DB, e *Enrollment) error {
	return tx.WithContext(ctx).Create(e).Error
}

func (r *Repository) GetByID(ctx context.Context, id uuid.UUID) (*Enrollment, error) {
	var e Enrollment
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&e).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func (r *Repository) ListForStudent(ctx context.Context, studentID uuid.UUID) ([]Enrollment, error) {
	var out []Enrollment
	err := r.db.WithContext(ctx).Where("student_id = ?", studentID).Order("enrolled_at DESC").Find(&out).Error
	return out, err
}

func (r *Repository) UpdateStatusWithdrawn(ctx context.Context, tx *gorm.DB, id uuid.UUID) error {
	now := time.Now().UTC()
	return tx.WithContext(ctx).Model(&Enrollment{}).Where("id = ? AND status = 'active'", id).
		Updates(map[string]any{"status": StatusWithdrawn, "withdrawn_at": now}).Error
}

func (r *Repository) DB() *gorm.DB { return r.db }
