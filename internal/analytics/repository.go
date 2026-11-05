// Package analytics owns the aggregate/counter tables (RFC §4.3, §9) and,
// for now, is the first real consumer registered with
// internal/platform/relay — see README.md for why this module exists this
// early relative to the original M3 plan.
//
// Schema-per-service (ADR-0024): this module's tables live in the
// `analytics` schema, and nothing here queries course.courses,
// course.enrollments, or identity.users — those are other services' owned
// data. Instead, three mirror tables (user_role_mirror, course_mirror,
// enrollment_mirror) hold exactly the fields this module needs, upserted
// row-by-row from relay events (handlers.go). Every recompute below still
// reads from those mirror "source tables" rather than maintaining fragile
// increment/decrement pairs (ADR-0017's rebuildability principle,
// preserved by ADR-0024 — only the source moved, from another service's
// live tables to this service's own event-derived copy). This is the only
// file in the module issuing SQL, consistent with every other module's
// repository.go (RFC §5.9).
package analytics

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// UpsertUserRoleMirror records (or updates) one user's role, fed by
// identity.user_registered and identity.user_role_changed. Recomputing
// platform counts from this table (rather than incrementing/decrementing a
// running total on every event) means a role that changes twice, or an
// event redelivered after a crash, can never leave the count wrong — the
// next recompute just reflects whatever's in the mirror.
//
// eventID (the outbox row's own id, relay.DecodedEvent.OutboxID) guards
// against the relay's worker pool processing a user's registered and
// role_changed events out of order: the update is only applied if eventID
// is newer than the last one actually applied to this row, so a
// late-finishing older event can never stomp a role a newer event already
// set.
func UpsertUserRoleMirror(ctx context.Context, tx *gorm.DB, userID uuid.UUID, role string, eventID int64) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO user_role_mirror (user_id, role, last_event_id, updated_at) VALUES (?, ?, ?, now())
		ON CONFLICT (user_id) DO UPDATE SET
			role = EXCLUDED.role,
			last_event_id = EXCLUDED.last_event_id,
			updated_at = EXCLUDED.updated_at
		WHERE EXCLUDED.last_event_id > user_role_mirror.last_event_id
	`, userID, role, eventID).Error
}

// UpsertCourseMirror records that a course has been published, fed by
// course.course_published. Never cleared by course.course_archived —
// total_courses_published counts "ever published," matching the behavior
// this had before ADR-0024 (archiving a course never cleared
// courses.published_version_id either).
func UpsertCourseMirror(ctx context.Context, tx *gorm.DB, courseID uuid.UUID, published bool) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO course_mirror (course_id, published, updated_at) VALUES (?, ?, now())
		ON CONFLICT (course_id) DO UPDATE SET published = EXCLUDED.published, updated_at = EXCLUDED.updated_at
	`, courseID, published).Error
}

// InsertEnrollmentMirror records a new enrollment, fed by enrollment.
// student_enrolled — the only event carrying enrolled_at. Idempotent
// against redelivery of the same event (ON CONFLICT DO UPDATE with the
// same values is a no-op in effect, not an error).
func InsertEnrollmentMirror(ctx context.Context, tx *gorm.DB, enrollmentID, courseID, studentID uuid.UUID, enrolledAt time.Time) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO enrollment_mirror (enrollment_id, course_id, student_id, status, enrolled_at, updated_at)
		VALUES (?, ?, ?, 'active', ?, now())
		ON CONFLICT (enrollment_id) DO UPDATE SET
			enrolled_at = EXCLUDED.enrolled_at,
			updated_at = EXCLUDED.updated_at
	`, enrollmentID, courseID, studentID, enrolledAt).Error
}

// UpdateEnrollmentMirrorStatus transitions an existing mirror row — fed by
// enrollment.student_withdrawn and progress.course_completed, neither of
// which carries enrolled_at, so this updates rather than upserts: the row
// must already exist from InsertEnrollmentMirror.
func UpdateEnrollmentMirrorStatus(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID, status string, completedAt *time.Time) error {
	return tx.WithContext(ctx).Exec(`
		UPDATE enrollment_mirror SET status = ?, completed_at = ?, updated_at = now()
		WHERE enrollment_id = ?
	`, status, completedAt, enrollmentID).Error
}

// CourseIDForEnrollment looks up course_id from this module's own mirror —
// never from course.enrollments (ADR-0024). Used by handleCourseCompleted,
// since progress.course_completed's payload carries enrollment_id and
// student_id, not course_id (the event is deliberately minimal at the
// source; consumers that need more join for it against their own data,
// rather than every event growing every possible field "just in case").
func CourseIDForEnrollment(ctx context.Context, tx *gorm.DB, enrollmentID uuid.UUID) (uuid.UUID, error) {
	var courseID uuid.UUID
	err := tx.WithContext(ctx).Raw(`SELECT course_id FROM enrollment_mirror WHERE enrollment_id = ?`, enrollmentID).
		Row().Scan(&courseID)
	return courseID, err
}

// RecomputePlatformSnapshot recomputes the single-row platform snapshot
// from this module's own mirror tables — correct by construction, not by
// keeping a running total in sync.
func RecomputePlatformSnapshot(ctx context.Context, tx *gorm.DB) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO platform_snapshot (id, total_students, total_instructors, total_courses_published, avg_courses_per_student, updated_at)
		SELECT
			true,
			(SELECT count(*) FROM user_role_mirror WHERE role = 'student'),
			(SELECT count(*) FROM user_role_mirror WHERE role = 'instructor'),
			(SELECT count(*) FROM course_mirror WHERE published),
			COALESCE(
				(SELECT count(*)::numeric FROM enrollment_mirror WHERE status IN ('active','completed')) /
				NULLIF((SELECT count(*) FROM user_role_mirror WHERE role = 'student'), 0),
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
// complete — from enrollment_mirror directly.
func RecomputeCourseStats(ctx context.Context, tx *gorm.DB, courseID uuid.UUID) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO course_stats (course_id, total_enrollments, active_enrollments, completions, completion_rate, avg_completion_s, updated_at)
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
			 FROM enrollment_mirror WHERE course_id = ? AND status = 'completed'),
			now()
		FROM enrollment_mirror WHERE course_id = ?
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
		INSERT INTO daily_enrollments (day, course_id, count)
		VALUES (current_date, ?, 1)
		ON CONFLICT (day, course_id) DO UPDATE SET count = daily_enrollments.count + 1
	`, courseID).Error
}

// EnsureCourseStatsRow gives a just-published course a zeroed stats row
// immediately (SmartCourse.md FR-2's "Analytics initialization" step) —
// so GET /analytics/courses/{id} or the platform snapshot never has to
// special-case "no row yet" versus "a row with all zeros."
func EnsureCourseStatsRow(ctx context.Context, tx *gorm.DB, courseID uuid.UUID) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO course_stats (course_id) VALUES (?)
		ON CONFLICT (course_id) DO NOTHING
	`, courseID).Error
}

type PlatformSnapshot struct {
	TotalStudents         int64
	TotalInstructors      int64
	TotalCoursesPublished int64
	AvgCoursesPerStudent  float64
}

// CourseStats mirrors `course_stats` — written by RecomputeCourseStats,
// read by GetCourseStats. AvgCompletionS is a pointer because it's
// genuinely NULL until at least one student has completed the course
// (the underlying `avg(...)` over zero rows), not a zero worth
// distinguishing from "nobody has finished yet."
type CourseStats struct {
	TotalEnrollments  int64
	ActiveEnrollments int64
	Completions       int64
	CompletionRate    float64
	AvgCompletionS    *int64
}

// DailyEnrollmentRow is one (day, count) point in a course's enrollment
// time series — DR-014/FR-5's "New Enrollments Over Time" metric.
type DailyEnrollmentRow struct {
	Day   time.Time
	Count int
}

// Repository is the read side of this module — the only type the HTTP
// handler is allowed to reach data through (via Service), matching every
// other module's handler → service → repository shape (RFC §4.3). The
// write-side Recompute*/Ensure*/Increment*/Upsert* functions above stay
// package-level functions taking tx *gorm.DB directly, not Repository
// methods: they run inside the relay's per-event transaction, the same
// pattern internal/enrollment uses for lockCourseForEnrollment — a
// Repository bound to the base connection wouldn't be the right tool for
// work that must happen inside someone else's transaction.
type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository { return &Repository{db: db} }

func (r *Repository) GetPlatformSnapshot(ctx context.Context) (*PlatformSnapshot, error) {
	var s PlatformSnapshot
	err := r.db.WithContext(ctx).Raw(`
		SELECT total_students, total_instructors, total_courses_published, avg_courses_per_student
		FROM platform_snapshot WHERE id = true
	`).Row().Scan(&s.TotalStudents, &s.TotalInstructors, &s.TotalCoursesPublished, &s.AvgCoursesPerStudent)
	if err != nil {
		return &PlatformSnapshot{}, nil // no row yet (no events processed) — zeros, not an error
	}
	return &s, nil
}

// GetCourseStats reads one course's aggregate row. A missing row returns
// zeros rather than an error, the same tradeoff GetPlatformSnapshot makes
// and for a related reason: this module has no cross-schema way to tell
// "this course doesn't exist" apart from "this course exists but has
// never been published" (ADR-0024 — no query into course.courses). A
// zeroed response is correct for the second case and harmless for the
// first; distinguishing them would need a call into internal/course,
// which this module deliberately never does.
func (r *Repository) GetCourseStats(ctx context.Context, courseID uuid.UUID) (*CourseStats, error) {
	var s CourseStats
	err := r.db.WithContext(ctx).Raw(`
		SELECT total_enrollments, active_enrollments, completions, completion_rate, avg_completion_s
		FROM course_stats WHERE course_id = ?
	`, courseID).Row().Scan(&s.TotalEnrollments, &s.ActiveEnrollments, &s.Completions, &s.CompletionRate, &s.AvgCompletionS)
	if err != nil {
		return &CourseStats{}, nil
	}
	return &s, nil
}

// ListDailyEnrollments returns one course's full enrollment time series,
// oldest first — DR-014/FR-5's "New Enrollments Over Time," previously
// computed (IncrementDailyEnrollment) but entirely unreadable: no
// repository method, no Service method, no route. No date-range
// filtering yet; add one if a real caller needs to bound a long-lived
// course's history, rather than guessing at a window now.
func (r *Repository) ListDailyEnrollments(ctx context.Context, courseID uuid.UUID) ([]DailyEnrollmentRow, error) {
	rows, err := r.db.WithContext(ctx).Raw(`
		SELECT day, count FROM daily_enrollments WHERE course_id = ? ORDER BY day ASC
	`, courseID).Rows()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []DailyEnrollmentRow
	for rows.Next() {
		var row DailyEnrollmentRow
		if err := rows.Scan(&row.Day, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
