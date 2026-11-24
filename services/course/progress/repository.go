// Raw SQL for the completion check (RFC §5.9) — counting total lessons in
// the currently published version against completed rows is a join
// spanning `enrollments`, `courses`, `course_versions`, `modules`, and
// `lessons`; GORM associations would make that shape and cost implicit.
// This is a read against those tables' columns, not an import of
// services/course/course or services/course/enrollment's Go types (RFC §4.4).
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
	return r.loadEnrollment(ctx, tx, enrollmentID, "")
}

// getEnrollmentForUpdate takes the enrollment row lock, serialising all
// MarkComplete calls for one enrollment (F-06, F-22). It locks only the
// enrollments row (never courses), so it cannot invert the courses ->
// enrollments order that enrollment.Enroll/Withdraw follow (RFC §8.5).
func (r *Repository) getEnrollmentForUpdate(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID) (*enrollmentRow, error) {
	return r.loadEnrollment(ctx, tx, enrollmentID, " FOR UPDATE OF e")
}

func (r *Repository) loadEnrollment(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID, suffix string) (*enrollmentRow, error) {
	var row enrollmentRow
	err := tx.WithContext(ctx).Raw(`
		SELECT e.id, e.student_id, e.status, c.published_version_id
		FROM enrollments e JOIN courses c ON c.id = e.course_id
		WHERE e.id = ?`+suffix, enrollmentID).
		Row().Scan(&row.ID, &row.StudentID, &row.Status, &row.PublishedVersionID)
	if err != nil {
		return nil, ErrNotFound
	}
	return &row, nil
}

// lessonInPublishedVersion reports whether lessonKey is a lesson of the
// given (published) version.
func (r *Repository) lessonInPublishedVersion(ctx context.Context, tx *gorm.DB, versionID, lessonKey uuid.UUID) (bool, error) {
	var n int64
	err := tx.WithContext(ctx).Raw(`
		SELECT count(*) FROM lessons l JOIN modules m ON m.id = l.module_id
		WHERE m.course_version_id = ? AND l.lesson_key = ?`, versionID, lessonKey).Row().Scan(&n)
	return n > 0, err
}

// missingLessonKeys lists lessons of the published version that the
// enrollment has no lesson_progress row for (F-07: lessons added by a
// republish after the student enrolled).
func (r *Repository) missingLessonKeys(ctx context.Context, db *gorm.DB, enrollmentID, versionID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := db.WithContext(ctx).Raw(`
		SELECT DISTINCT l.lesson_key FROM lessons l JOIN modules m ON m.id = l.module_id
		WHERE m.course_version_id = ?
		AND NOT EXISTS (SELECT 1 FROM lesson_progress p WHERE p.enrollment_id = ? AND p.lesson_key = l.lesson_key)`,
		versionID, enrollmentID).Rows()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var keys []uuid.UUID
	for rows.Next() {
		var k uuid.UUID
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// ensureProgressRows lazily creates the missing lesson_progress rows. The
// caller holds the enrollment row lock; ON CONFLICT keeps it safe anyway.
func (r *Repository) ensureProgressRows(ctx context.Context, tx *gorm.DB, enrollmentID, versionID uuid.UUID) error {
	keys, err := r.missingLessonKeys(ctx, tx, enrollmentID, versionID)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := tx.WithContext(ctx).Exec(`
			INSERT INTO lesson_progress (id, enrollment_id, lesson_key) VALUES (?, ?, ?)
			ON CONFLICT (enrollment_id, lesson_key) DO NOTHING`,
			uuid.Must(uuid.NewV7()), enrollmentID, k).Error; err != nil {
			return err
		}
	}
	return nil
}

// markLessonComplete is idempotent by construction (RFC §8.3): it only sets
// completed_at when currently NULL, so a second call affects zero rows. It
// reports whether a row changed so the caller emits progress.lesson_completed
// once per completion, with the same completed_at the row now holds.
func (r *Repository) markLessonComplete(ctx context.Context, tx *gorm.DB, enrollmentID, lessonKey uuid.UUID) (time.Time, bool, error) {
	now := time.Now().UTC()
	res := tx.WithContext(ctx).Exec(`
		UPDATE lesson_progress SET completed_at = ?, updated_at = ?
		WHERE enrollment_id = ? AND lesson_key = ? AND completed_at IS NULL`,
		now, now, enrollmentID, lessonKey)
	return now, res.RowsAffected > 0, res.Error
}

// isFullyComplete compares completed lesson_progress rows against the
// lessons of the enrollment's currently published version (RFC assumption
// A5). Only rows for lessons that are still in that version count, so rows
// left over from lessons a republish removed cannot inflate the tally.
func (r *Repository) isFullyComplete(ctx context.Context, tx *gorm.DB, enrollmentID, publishedVersionID uuid.UUID) (bool, error) {
	var totalLessons, completedCount int64
	if err := tx.WithContext(ctx).Raw(`
		SELECT count(DISTINCT l.lesson_key) FROM lessons l JOIN modules m ON m.id = l.module_id
		WHERE m.course_version_id = ?`, publishedVersionID).Row().Scan(&totalLessons); err != nil {
		return false, err
	}
	if err := tx.WithContext(ctx).Raw(`
		SELECT count(*) FROM lesson_progress p
		WHERE p.enrollment_id = ? AND p.completed_at IS NOT NULL
		AND EXISTS (SELECT 1 FROM lessons l JOIN modules m ON m.id = l.module_id
		            WHERE m.course_version_id = ? AND l.lesson_key = p.lesson_key)`,
		enrollmentID, publishedVersionID).Row().Scan(&completedCount); err != nil {
		return false, err
	}
	return totalLessons > 0 && completedCount >= totalLessons, nil
}

// markEnrollmentCompleted also reports whether a row changed, so the caller
// emits course_completed exactly once (F-22). It returns the completed_at it wrote so the caller
// can put the exact same value in the progress.course_completed outbox
// event (ADR-0024 — analytics.enrollment_mirror needs completed_at, and a
// second time.Now() call here would drift from what's actually in the row).
func (r *Repository) markEnrollmentCompleted(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID) (time.Time, bool, error) {
	now := time.Now().UTC()
	res := tx.WithContext(ctx).Exec(`
		UPDATE enrollments SET status = 'completed', completed_at = ?
		WHERE id = ? AND status = 'active'`, now, enrollmentID)
	return now, res.RowsAffected > 0, res.Error
}

func (r *Repository) ListProgress(ctx context.Context, enrollmentID uuid.UUID) ([]LessonProgress, error) {
	var out []LessonProgress
	err := r.db.WithContext(ctx).Where("enrollment_id = ?", enrollmentID).Find(&out).Error
	return out, err
}

func (r *Repository) DB() *gorm.DB { return r.db }
