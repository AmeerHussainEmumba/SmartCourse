// Package analytics owns the aggregate/counter tables (RFC §4.3, §9) and,
// for now, is the first real consumer registered with
// internal/platform/relay — see README.md for why this module exists this
// early relative to the original M3 plan.
//
// Every recompute below reads from source tables rather than maintaining
// fragile increment/decrement pairs (ADR-0017's rebuildability principle,
// applied eagerly here instead of on ADR-0017's 60-second schedule — the
// scheduled reconciliation path itself is still not built; see README.md).
// This is the only file in the module issuing SQL, consistent with every
// other module's repository.go (RFC §5.9).
package analytics

import (
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// RecomputePlatformSnapshot recomputes the single-row platform snapshot
// from source tables (users, courses, enrollments) — correct by
// construction, not by keeping a running total in sync.
func RecomputePlatformSnapshot(ctx context.Context, tx *gorm.DB) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO analytics_platform_snapshot (id, total_students, total_instructors, total_courses_published, avg_courses_per_student, updated_at)
		SELECT
			true,
			(SELECT count(*) FROM users WHERE role = 'student'),
			(SELECT count(*) FROM users WHERE role = 'instructor'),
			(SELECT count(*) FROM courses WHERE published_version_id IS NOT NULL),
			COALESCE(
				(SELECT count(*)::numeric FROM enrollments WHERE status IN ('active','completed')) /
				NULLIF((SELECT count(*) FROM users WHERE role = 'student'), 0),
				0
			),
			now()
		ON CONFLICT (id) DO UPDATE SET
			total_students = EXCLUDED.total_students,
			total_instructors = EXCLUDED.total_instructors,
			total_courses_published = EXCLUDED.total_courses_published,
			avg_courses_per_student = EXCLUDED.avg_courses_per_student,
			updated_at = EXCLUDED.updated_at
	`).Error
}

// RecomputeCourseStats recomputes one course's aggregate row — total and
// active enrollments, completions, completion rate, and average time to
// complete — from enrollments directly.
func RecomputeCourseStats(ctx context.Context, tx *gorm.DB, courseID uuid.UUID) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO analytics_course_stats (course_id, total_enrollments, active_enrollments, completions, completion_rate, avg_completion_s, updated_at)
		SELECT
			?,
			count(*) FILTER (WHERE status IN ('active','completed')),
			count(*) FILTER (WHERE status = 'active'),
			count(*) FILTER (WHERE status = 'completed'),
			CASE WHEN count(*) FILTER (WHERE status IN ('active','completed')) > 0
				THEN count(*) FILTER (WHERE status = 'completed')::numeric / count(*) FILTER (WHERE status IN ('active','completed'))
				ELSE 0
			END,
			(SELECT avg(extract(epoch FROM completed_at - enrolled_at))::bigint
			 FROM enrollments WHERE course_id = ? AND status = 'completed'),
			now()
		FROM enrollments WHERE course_id = ?
		ON CONFLICT (course_id) DO UPDATE SET
			total_enrollments = EXCLUDED.total_enrollments,
			active_enrollments = EXCLUDED.active_enrollments,
			completions = EXCLUDED.completions,
			completion_rate = EXCLUDED.completion_rate,
			avg_completion_s = EXCLUDED.avg_completion_s,
			updated_at = EXCLUDED.updated_at
	`, courseID, courseID, courseID).Error
}

// IncrementDailyEnrollment bumps today's (day, course_id) bucket. Unlike
// the recomputed aggregates above, an incremental bump is correct here:
// "an enrollment happened on this UTC day" is an immutable historical
// fact once true, so there's no drift risk a recompute would guard
// against.
func IncrementDailyEnrollment(ctx context.Context, tx *gorm.DB, courseID uuid.UUID) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO analytics_daily_enrollments (day, course_id, count)
		VALUES (current_date, ?, 1)
		ON CONFLICT (day, course_id) DO UPDATE SET count = analytics_daily_enrollments.count + 1
	`, courseID).Error
}

// EnsureCourseStatsRow gives a just-published course a zeroed stats row
// immediately (SmartCourse.md FR-2's "Analytics initialization" step) —
// so GET /analytics/courses/{id} (not yet built) or the platform snapshot
// never has to special-case "no row yet" versus "a row with all zeros."
func EnsureCourseStatsRow(ctx context.Context, tx *gorm.DB, courseID uuid.UUID) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO analytics_course_stats (course_id) VALUES (?)
		ON CONFLICT (course_id) DO NOTHING
	`, courseID).Error
}

type PlatformSnapshot struct {
	TotalStudents         int64
	TotalInstructors      int64
	TotalCoursesPublished int64
	AvgCoursesPerStudent  float64
}

func GetPlatformSnapshot(ctx context.Context, db *gorm.DB) (*PlatformSnapshot, error) {
	var s PlatformSnapshot
	err := db.WithContext(ctx).Raw(`
		SELECT total_students, total_instructors, total_courses_published, avg_courses_per_student
		FROM analytics_platform_snapshot WHERE id = true
	`).Row().Scan(&s.TotalStudents, &s.TotalInstructors, &s.TotalCoursesPublished, &s.AvgCoursesPerStudent)
	if err != nil {
		return &PlatformSnapshot{}, nil // no row yet (no events processed) — zeros, not an error
	}
	return &s, nil
}
