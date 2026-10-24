// Raw SQL for the completion check (RFC §5.9) — counting total lessons in
// the currently published version against completed rows is a join
// spanning `enrollments`, `courses`, `course_versions`, `modules`, and
// `lessons`; GORM associations would make that shape and cost implicit.
// This is a read against those tables' columns, not an import of
// internal/course or internal/enrollment's Go types (RFC §4.4).
package progress

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository { return &Repository{db: db} }

var ErrNotFound = errors.New("progress: not found")

// enrollmentRow is the minimal projection this module needs to check
// ownership and completion — not enrollment.Enrollment.
type enrollmentRow struct {
	ID                 uuid.UUID
	StudentID          uuid.UUID
	Status             string
	PublishedVersionID *uuid.UUID
}

func (r *Repository) getEnrollment(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID) (*enrollmentRow, error) {
	var row enrollmentRow
	err := tx.WithContext(ctx).Raw(`
		SELECT e.id, e.student_id, e.status, c.published_version_id
		FROM enrollments e JOIN courses c ON c.id = e.course_id
		WHERE e.id = ?`, enrollmentID).
		Row().Scan(&row.ID, &row.StudentID, &row.Status, &row.PublishedVersionID)
	if err != nil {
		return nil, ErrNotFound
	}
	return &row, nil
}

// MarkLessonComplete is idempotent by construction (RFC §8.3): it only sets
// completed_at when currently NULL, so calling it twice on an
// already-completed lesson is a no-op — the WHERE clause makes a second
// call affect zero rows rather than erroring or double-processing.
func (r *Repository) markLessonComplete(ctx context.Context, tx *gorm.DB, enrollmentID, lessonKey uuid.UUID) error {
	now := time.Now().UTC()
	return tx.WithContext(ctx).Exec(`
		UPDATE lesson_progress SET completed_at = ?, updated_at = ?
		WHERE enrollment_id = ? AND lesson_key = ? AND completed_at IS NULL`,
		now, now, enrollmentID, lessonKey).Error
}

// isFullyComplete compares completed lesson_progress rows against the
// total lesson count in the enrollment's currently published version — the
// comparison is against the CURRENT published version (not the version the
// student originally enrolled in, RFC assumption A5), so a republish that
// adds lessons can correctly move a completed student back to incomplete.
func (r *Repository) isFullyComplete(ctx context.Context, tx *gorm.DB, enrollmentID, publishedVersionID uuid.UUID) (bool, error) {
	var totalLessons, completedCount int64
	if err := tx.WithContext(ctx).Raw(`
		SELECT count(*) FROM lessons l JOIN modules m ON m.id = l.module_id
		WHERE m.course_version_id = ?`, publishedVersionID).Row().Scan(&totalLessons); err != nil {
		return false, err
	}
	if err := tx.WithContext(ctx).Raw(`
		SELECT count(*) FROM lesson_progress
		WHERE enrollment_id = ? AND completed_at IS NOT NULL`, enrollmentID).Row().Scan(&completedCount); err != nil {
		return false, err
	}
	return totalLessons > 0 && completedCount >= totalLessons, nil
}

func (r *Repository) markEnrollmentCompleted(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID) error {
	now := time.Now().UTC()
	return tx.WithContext(ctx).Exec(`
		UPDATE enrollments SET status = 'completed', completed_at = ?
		WHERE id = ? AND status = 'active'`, now, enrollmentID).Error
}

func (r *Repository) ListProgress(ctx context.Context, enrollmentID uuid.UUID) ([]LessonProgress, error) {
	var out []LessonProgress
	err := r.db.WithContext(ctx).Where("enrollment_id = ?", enrollmentID).Find(&out).Error
	return out, err
}

func (r *Repository) DB() *gorm.DB { return r.db }
