//go:build integration

// What this file checks (RFC §9.2, ADR-0017), against real Postgres and
// Redis: that Reconciler.Run heals a counter corrupted by hand, rewrites
// nothing when nothing drifted, refreshes the popular-courses sorted set,
// reports a Redis failure without leaving Postgres unrepaired, and is safe
// to run while relay handlers are writing and while a second Run is going.
// Source data always changes through the real handlers (deliver), never by
// writing the aggregates directly except to corrupt them on purpose.
package analytics_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

// feed delivers events through the real handlers, failing the test on error.
func feed(t *testing.T, env *testenv.Env, events ...advEvent) {
	t.Helper()
	for _, e := range events {
		if err := deliver(env, e); err != nil {
			t.Fatalf("deliver %s: %v", e.typ, err)
		}
	}
}

func TestReconciler_HealsCorruptedCounters(t *testing.T) {
	env := testenv.Setup(t)
	feed(t, env, scenario()...)
	want := stateDump(t, env, true)

	// Corrupt every aggregate by hand: the platform row, a course row, a
	// daily bucket, and a bucket for a day that never had an enrollment.
	if err := env.DB.Exec(`UPDATE platform_snapshot SET total_students = 999, total_instructors = 42, total_courses_published = 7, avg_courses_per_student = 12`).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Exec(`UPDATE course_stats SET total_enrollments = 77, active_enrollments = 70, completions = 7, completion_rate = 0.5, avg_completion_s = 1`).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Exec(`UPDATE daily_enrollments SET count = 50`).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Exec(`INSERT INTO daily_enrollments (day, course_id, count) SELECT '2001-01-01', course_id, 9 FROM course_stats`).Error; err != nil {
		t.Fatal(err)
	}
	if got := stateDump(t, env, true); got == want {
		t.Fatal("test setup: corruption did not change the state")
	}

	rec := analytics.NewReconciler(env.DB, env.RDB)
	if err := rec.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := stateDump(t, env, true); got != want {
		t.Fatalf("Run did not restore the aggregates.\nwant:\n%s\ngot:\n%s", want, got)
	}
	assertCountersSane(t, env)

	// Idempotent: a second Run changes nothing.
	if err := rec.Run(context.Background()); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := stateDump(t, env, true); got != want {
		t.Fatalf("second Run changed the state.\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestReconciler_RebuildsMissingAggregates(t *testing.T) {
	env := testenv.Setup(t)
	feed(t, env, scenario()...)
	want := stateDump(t, env, true)

	// Aggregates wiped entirely (a restore, a manual truncate); the mirrors survive.
	if err := env.DB.Exec(`TRUNCATE course_stats, daily_enrollments, platform_snapshot`).Error; err != nil {
		t.Fatal(err)
	}
	if err := analytics.NewReconciler(env.DB, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := stateDump(t, env, true); got != want {
		t.Fatalf("Run did not rebuild the aggregates from the mirrors.\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestReconciler_RefreshesPopularSortedSet(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	c1, c2, c3 := uuid.New(), uuid.New(), uuid.New()
	feed(t, env,
		ev(1, "course.course_published", map[string]any{"course_id": c1}),
		ev(2, "course.course_published", map[string]any{"course_id": c2}),
		ev(3, "course.course_published", map[string]any{"course_id": c3}),
	)
	n := int64(10)
	for i, c := range []uuid.UUID{c1, c1, c1, c2, c2} {
		n++
		feed(t, env, ev(n, "enrollment.student_enrolled", map[string]any{
			"enrollment_id": uuid.New(), "student_id": uuid.New(), "course_id": c, "enrolled_at": time.Now().UTC().Add(time.Duration(i) * time.Second),
		}))
	}
	// A stale member from an earlier run must disappear.
	if err := env.RDB.ZAdd(ctx, analytics.PopularCoursesKey, redis.Z{Score: 500, Member: uuid.NewString()}).Err(); err != nil {
		t.Fatal(err)
	}

	if err := analytics.NewReconciler(env.DB, env.RDB).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	zs, err := env.RDB.ZRevRangeWithScores(ctx, analytics.PopularCoursesKey, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(zs) != 2 || zs[0].Member != c1.String() || zs[0].Score != 3 || zs[1].Member != c2.String() || zs[1].Score != 2 {
		t.Fatalf("sorted set: want [c1:3 c2:2] (c3 has no enrollments), got %+v", zs)
	}
	if ttl := env.RDB.TTL(ctx, analytics.PopularCoursesKey).Val(); ttl <= 0 {
		t.Fatalf("sorted set should expire if the reconciler stops, got ttl %v", ttl)
	}
	keys, _ := env.RDB.Keys(ctx, analytics.PopularCoursesKey+":tmp:*").Result()
	if len(keys) != 0 {
		t.Fatalf("temporary keys left behind: %v", keys)
	}
}

func TestReconciler_EmptyRankingClearsSortedSet(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	if err := env.RDB.ZAdd(ctx, analytics.PopularCoursesKey, redis.Z{Score: 5, Member: uuid.NewString()}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := analytics.NewReconciler(env.DB, env.RDB).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := env.RDB.Exists(ctx, analytics.PopularCoursesKey).Val(); n != 0 {
		t.Fatalf("with no enrolled courses the sorted set should be gone, exists=%d", n)
	}
}

// Redis being down must be reported, but Postgres is still repaired first.
func TestReconciler_RedisDown_StillRepairsPostgres_AndReportsError(t *testing.T) {
	env := testenv.Setup(t)
	feed(t, env, scenario()...)
	want := stateDump(t, env, true)
	if err := env.DB.Exec(`UPDATE platform_snapshot SET total_students = 999`).Error; err != nil {
		t.Fatal(err)
	}

	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer func() { _ = dead.Close() }()
	if err := analytics.NewReconciler(env.DB, dead).Run(context.Background()); err == nil {
		t.Fatal("expected Run to report the Redis failure")
	}
	if got := stateDump(t, env, true); got != want {
		t.Fatalf("Postgres was not repaired despite the Redis failure.\nwant:\n%s\ngot:\n%s", want, got)
	}
}

// TestReconciler_ConcurrentWithHandlersAndSecondRun runs two Reconcilers in
// a loop while four goroutines push enrollments through the real handlers.
// There is deliberately no Run after the writers finish: the aggregates must
// already be right, i.e. no Run may have overwritten a newer handler result
// with a stale recompute, and no deadlock or error may occur.
func TestReconciler_ConcurrentWithHandlersAndSecondRun(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	const courses, perWorker, workers = 2, 10, 4
	courseIDs := make([]uuid.UUID, courses)
	var seed []advEvent
	for i := range courseIDs {
		courseIDs[i] = uuid.New()
		seed = append(seed, ev(int64(i+1), "course.course_published", map[string]any{"course_id": courseIDs[i]}))
	}
	feed(t, env, seed...)

	stop := make(chan struct{})
	runErrs := make(chan error, 64)
	var runners sync.WaitGroup
	for i := 0; i < 2; i++ {
		runners.Add(1)
		go func() {
			defer runners.Done()
			rec := analytics.NewReconciler(env.DB, env.RDB)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := rec.Run(ctx); err != nil {
					runErrs <- err
					return
				}
			}
		}()
	}

	enrolledAt := time.Now().UTC().Truncate(time.Second)
	var writers sync.WaitGroup
	writeErrs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := 0; i < perWorker; i++ {
				e := ev(int64(1000+w*perWorker+i), "enrollment.student_enrolled", map[string]any{
					"enrollment_id": uuid.New(), "student_id": uuid.New(), "course_id": courseIDs[i%courses], "enrolled_at": enrolledAt,
				})
				if err := deliver(env, e); err != nil {
					writeErrs <- err
				}
			}
		}(w)
	}
	writers.Wait()
	close(stop)
	runners.Wait()
	close(runErrs)
	close(writeErrs)
	for err := range runErrs {
		t.Fatalf("Run failed while handlers were writing: %v", err)
	}
	for err := range writeErrs {
		t.Fatalf("handler failed while Run was going: %v", err)
	}

	total := int64(workers * perWorker)
	var sumStats, sumDaily int64
	env.DB.Raw(`SELECT COALESCE(sum(total_enrollments),0) FROM course_stats`).Row().Scan(&sumStats)
	env.DB.Raw(`SELECT COALESCE(sum(count),0) FROM daily_enrollments`).Row().Scan(&sumDaily)
	if sumStats != total || sumDaily != total {
		t.Fatalf("after concurrent Run and handlers: course_stats total %d, daily total %d, want both %d", sumStats, sumDaily, total)
	}
	var published int64
	env.DB.Raw(`SELECT total_courses_published FROM platform_snapshot`).Row().Scan(&published)
	if published != courses {
		t.Fatalf("platform_snapshot.total_courses_published = %d, want %d", published, courses)
	}
	assertCountersSane(t, env)
}

// TestReconciler_WaitsForInFlightHandlerAndDoesNotOverwriteItsResult pins the
// lock-then-read discipline deterministically (the race test above can only
// hit it by luck). A handler-shaped transaction has written a mirror row and
// recomputed the course's stats under the row lock but not committed; Run
// starts and must block on that lock, then read the mirror after the commit.
// If Run computed from a snapshot taken before waiting (a single
// INSERT..SELECT..ON CONFLICT), it would write back a stale total of 0.
func TestReconciler_WaitsForInFlightHandlerAndDoesNotOverwriteItsResult(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	course := uuid.New()
	feed(t, env, ev(1, "course.course_published", map[string]any{"course_id": course}))

	tx := env.DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if err := analytics.InsertEnrollmentMirror(ctx, tx, uuid.New(), course, uuid.New(), time.Now().UTC(), 50); err != nil {
		t.Fatal(err)
	}
	if err := analytics.RecomputeCourseStats(ctx, tx, course); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- analytics.NewReconciler(env.DB, nil).Run(ctx) }()
	select {
	case err := <-done:
		_ = tx.Rollback()
		t.Fatalf("Run finished (%v) while a handler transaction held the course lock", err)
	case <-time.After(700 * time.Millisecond): // Run is parked on the row lock
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	var total int64
	env.DB.Raw(`SELECT total_enrollments FROM course_stats WHERE course_id = ?`, course).Row().Scan(&total)
	if total != 1 {
		t.Fatalf("Run overwrote the handler's committed result with a stale total: got %d, want 1", total)
	}
}
