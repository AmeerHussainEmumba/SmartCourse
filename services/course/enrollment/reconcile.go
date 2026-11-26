package enrollment

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// reconcileBatch bounds one ReconcileEnrollmentCounts pass; leftovers wait for
// the next.
const reconcileBatch = 500

// CountDrift is one course whose stored counter disagreed with its enrollments.
type CountDrift struct {
	CourseID uuid.UUID
	Stored   int // enrollment_count before the repair
	Actual   int // the recomputed value now stored
}

// ReconcileResult counts what one ReconcileEnrollmentCounts pass did.
type ReconcileResult struct {
	Checked  int          // courses whose counter looked wrong and were examined
	Repaired []CountDrift // courses whose counter was rewritten
	Errors   int          // courses that could not be repaired; retried next pass
}

// ReconcileEnrollmentCounts recomputes courses.enrollment_count from the
// enrollments table and repairs any course where the two disagree (RFC §9.5
// counter reconciliation). The counter is a denormalised seat count that Enroll,
// Withdraw and handleCourseArchived maintain by delta; a crash cannot drift it
// (each delta commits with its enrollment row), but a manual edit or a bug
// could, and nothing else would ever notice.
//
// A seat is held by an enrollment that is not withdrawn: Enroll counts it when
// the row is created as 'active', and nothing decrements on completion, so
// 'completed' enrollments stay counted exactly as the unique index on
// (student, course) treats them. Counting only 'active' would silently free the
// seats of completed students.
//
// Each repair runs in its own transaction that locks the course row first
// (lockCourseForEnrollment, ADR-0012, RFC §8.5), the same rule Enroll and
// Withdraw follow, and recounts under that lock, so an enrollment that commits
// while the pass runs is neither lost nor double-counted. Candidates are found
// without a lock, so a course whose counter is merely in flight is skipped
// after the recount. Safe to run from several replicas and repeatedly.
func ReconcileEnrollmentCounts(ctx context.Context, db *gorm.DB) (ReconcileResult, error) {
	var res ReconcileResult
	var ids []uuid.UUID
	err := db.WithContext(ctx).Raw(`
		SELECT c.id FROM courses c
		WHERE c.enrollment_count <> (
			SELECT count(*) FROM enrollments e WHERE e.course_id = c.id AND e.status <> 'withdrawn')
		ORDER BY c.id LIMIT ?`, reconcileBatch).Scan(&ids).Error
	if err != nil {
		return res, fmt.Errorf("find drifted enrollment counters: %w", err)
	}

	var errs []error
	for _, id := range ids {
		res.Checked++
		drift, err := reconcileCourse(ctx, db, id)
		switch {
		case err != nil:
			res.Errors++
			errs = append(errs, fmt.Errorf("course %s: %w", id, err))
		case drift != nil:
			res.Repaired = append(res.Repaired, *drift)
		}
	}
	return res, errors.Join(errs...)
}

// reconcileCourse repairs one course under its row lock. It returns nil when,
// by the time the lock is held, the counter already matches.
func reconcileCourse(ctx context.Context, db *gorm.DB, courseID uuid.UUID) (*CountDrift, error) {
	var drift *CountDrift
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, err := lockCourseForEnrollment(ctx, tx, courseID)
		if err != nil {
			return err
		}
		var actual int
		if err := tx.WithContext(ctx).Raw(
			`SELECT count(*) FROM enrollments WHERE course_id = ? AND status <> 'withdrawn'`, courseID,
		).Row().Scan(&actual); err != nil {
			return fmt.Errorf("count enrollments: %w", err)
		}
		if actual == row.EnrollmentCount {
			return nil
		}
		if err := tx.WithContext(ctx).Exec(
			`UPDATE courses SET enrollment_count = ?, updated_at = now() WHERE id = ?`, actual, courseID,
		).Error; err != nil {
			return fmt.Errorf("rewrite enrollment_count: %w", err)
		}
		drift = &CountDrift{CourseID: courseID, Stored: row.EnrollmentCount, Actual: actual}
		return nil
	})
	return drift, err
}
