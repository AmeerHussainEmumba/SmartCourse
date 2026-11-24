// Scheduled reconciliation (RFC §9.2, ADR-0017). The relay handlers keep
// the aggregates current as events arrive; Reconciler.Run re-derives every
// one of them from this module's own mirror tables so a lost, doubled or
// hand-edited counter heals within one interval, and refreshes the Most
// Popular Courses sorted set in Redis (RFC §9.1).
//
// Run is meant to be scheduled about every 60 seconds by whatever hosts the
// periodic jobs; this package only provides the work, not the clock.
package analytics

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	// PopularCoursesKey is the Redis sorted set holding the ranking:
	// member = course_id, score = enrollments (active plus completed).
	PopularCoursesKey = "analytics:popular_courses"

	// popularRankSize bounds the sorted set; the endpoint's own limit cap
	// (maxPopularLimit) is smaller.
	popularRankSize = 100

	// popularTTL lets the sorted set expire if the Reconciler stops running,
	// so readers fall back to Postgres instead of serving an ever older rank.
	popularTTL = 5 * time.Minute

	// reconcileLockKey is the pg_advisory_xact_lock key serialising Run
	// calls (two replicas, or an overlapping tick).
	reconcileLockKey int64 = 0x5343414e414c // "SCANAL"
)

// Reconciler recomputes the analytics aggregates from the mirrors.
type Reconciler struct {
	db  *gorm.DB
	rdb *redis.Client
}

// NewReconciler returns a Reconciler. rdb may be nil, in which case Run
// repairs Postgres only and skips the sorted set.
func NewReconciler(db *gorm.DB, rdb *redis.Client) *Reconciler {
	return &Reconciler{db: db, rdb: rdb}
}

// Run recomputes platform_snapshot, every course's course_stats row and
// daily_enrollments buckets from the mirror tables, then rebuilds the
// popular-courses sorted set. It is idempotent: with nothing drifted it
// rewrites the same values.
//
// Concurrency, with the relay handlers: each course is recomputed in its own
// short transaction that locks the course_stats row first, exactly as
// RecomputeCourseStats does inside a handler, and the platform snapshot is
// recomputed last in its own transaction. Lock order is therefore always
// course_stats row, then platform_snapshot row, in every writer, so no
// deadlock cycle exists; and because the aggregate is read only after the
// lock is held, whichever writer goes last sees the other's committed mirror
// rows and cannot overwrite a newer total with a stale one. A mirror write a
// handler has not committed yet is not visible to Run, but that handler's
// own recompute follows it and corrects the row, and the next Run would too.
//
// Concurrency, with another Run: a transaction-scoped advisory lock makes a
// second Run wait for the first, then redo the same idempotent work. Run
// holds one pooled connection for that lock while using a second for the
// recomputes, so the pool must allow at least two (the relay pool check
// already requires far more).
//
// Run keeps going past a failed course and returns every error joined, so
// one bad row never leaves the rest of the platform unrepaired.
func (r *Reconciler) Run(ctx context.Context) error {
	return r.db.WithContext(ctx).Transaction(func(lock *gorm.DB) error {
		if err := lock.Exec(`SELECT pg_advisory_xact_lock(?)`, reconcileLockKey).Error; err != nil {
			return fmt.Errorf("take reconcile lock: %w", err)
		}
		courseIDs, err := r.courseIDs(ctx)
		if err != nil {
			return err
		}
		var errs []error
		for _, id := range courseIDs {
			if err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				return recomputeCourse(ctx, tx, id)
			}); err != nil {
				errs = append(errs, fmt.Errorf("reconcile course %s: %w", id, err))
			}
		}
		if err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return RecomputePlatformSnapshot(ctx, tx)
		}); err != nil {
			errs = append(errs, fmt.Errorf("reconcile platform snapshot: %w", err))
		}
		if err := ReconcileWorkflowIssueStats(ctx, r.db); err != nil {
			errs = append(errs, err)
		}
		if err := r.refreshPopular(ctx); err != nil {
			errs = append(errs, fmt.Errorf("refresh popular courses: %w", err))
		}
		return errors.Join(errs...)
	})
}

// courseIDs is every course any aggregate or mirror knows about, in a stable
// order, so a stats row with no mirror left (or a mirror with no stats row)
// is still visited.
func (r *Reconciler) courseIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := r.db.WithContext(ctx).Raw(`
		SELECT course_id FROM course_mirror
		UNION SELECT course_id FROM enrollment_mirror
		UNION SELECT course_id FROM course_stats
		UNION SELECT course_id FROM daily_enrollments
		ORDER BY 1
	`).Rows()
	if err != nil {
		return nil, fmt.Errorf("list courses to reconcile: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// recomputeCourse rebuilds one course's stats row and daily buckets. The
// stats recompute takes the row lock; the daily rebuild then runs under it,
// serialised against IncrementDailyEnrollment, which a handler calls after
// its own RecomputeCourseStats. Buckets are counted from every mirror row
// with a known enrolled_at regardless of current status, matching the event
// path, which counts an enrollment on the day it happened even if the
// student later withdrew.
func recomputeCourse(ctx context.Context, tx *gorm.DB, courseID uuid.UUID) error {
	if err := RecomputeCourseStats(ctx, tx, courseID); err != nil {
		return err
	}
	if err := tx.WithContext(ctx).Exec(`DELETE FROM daily_enrollments WHERE course_id = ?`, courseID).Error; err != nil {
		return err
	}
	return tx.WithContext(ctx).Exec(`
		INSERT INTO daily_enrollments (day, course_id, count)
		SELECT (enrolled_at AT TIME ZONE 'UTC')::date, course_id, count(*)
		FROM enrollment_mirror
		WHERE course_id = ? AND enrolled_at IS NOT NULL
		GROUP BY 1, 2
	`, courseID).Error
}

// refreshPopular replaces the Redis sorted set with the ranking in Postgres.
// The new set is built under a temporary key and swapped in with RENAME in
// the same MULTI, so a reader never sees a half-built ranking. An empty
// ranking deletes the key; readers treat a missing key as "ask Postgres".
// Two Runs cannot interleave here (the advisory lock in Run), and the
// temporary key is unique per call regardless.
func (r *Reconciler) refreshPopular(ctx context.Context) error {
	if r.rdb == nil {
		return nil
	}
	ranking, err := PopularCoursesFromPostgres(ctx, r.db, popularRankSize)
	if err != nil {
		return err
	}
	if len(ranking) == 0 {
		return r.rdb.Del(ctx, PopularCoursesKey).Err()
	}
	members := make([]redis.Z, 0, len(ranking))
	for _, pc := range ranking {
		members = append(members, redis.Z{Score: float64(pc.Enrollments), Member: pc.CourseID.String()})
	}
	tmp := PopularCoursesKey + ":tmp:" + uuid.NewString()
	_, err = r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZAdd(ctx, tmp, members...)
		p.Expire(ctx, tmp, popularTTL)
		p.Rename(ctx, tmp, PopularCoursesKey)
		return nil
	})
	if err != nil {
		_ = r.rdb.Del(context.WithoutCancel(ctx), tmp).Err()
	}
	return err
}
