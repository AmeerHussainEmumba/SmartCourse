//go:build integration

// The jobs against real Postgres and Redis (RFC §18.2): each DB job does its
// work and leaves what it must keep, and a scheduled job fires once per tick
// even with two worker replicas (each with its own scheduler and servers).
package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/jobs"
)

func count(t *testing.T, env *testenv.Env, q string, args ...any) int {
	t.Helper()
	var n int
	if err := env.DB.Raw(q, args...).Row().Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func TestPruneIdempotencyKeysJob_RemovesOnlyExpiredKeys(t *testing.T) {
	env := testenv.Setup(t)
	for _, k := range []struct {
		key     string
		expires string
	}{{"expired-key", "now() - interval '1 hour'"}, {"live-key", "now() + interval '1 hour'"}} {
		if err := env.DB.Exec(`INSERT INTO idempotency_keys (key, user_id, endpoint, request_hash, expires_at) VALUES (?, ?, 'POST /x', '\x00', `+k.expires+`)`,
			k.key, uuid.New()).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := jobs.PruneIdempotencyKeys(env.DB, "0 * * * *").Run(context.Background()); err != nil {
		t.Fatalf("job: %v", err)
	}
	if n := count(t, env, `SELECT count(*) FROM idempotency_keys WHERE key = 'expired-key'`); n != 0 {
		t.Error("the expired key survived")
	}
	if n := count(t, env, `SELECT count(*) FROM idempotency_keys WHERE key = 'live-key'`); n != 1 {
		t.Error("the live key was deleted")
	}
}

func TestPruneOutboxJob_UsesRetentionAndKeepsUnpublished(t *testing.T) {
	env := testenv.Setup(t)
	insert := func(published string) {
		t.Helper()
		if err := env.DB.Exec(`INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, published_at)
			VALUES (?, 'x', ?, 'x.y', 1, '{}', `+published+`)`, uuid.New(), uuid.New()).Error; err != nil {
			t.Fatal(err)
		}
	}
	insert("now() - interval '10 days'") // past the 7 day retention
	insert("now() - interval '1 day'")   // inside it
	insert("NULL")                       // unpublished
	job, err := jobs.PruneOutbox(env.DB, 7*24*time.Hour, "0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("job: %v", err)
	}
	if n := count(t, env, `SELECT count(*) FROM outbox`); n != 2 {
		t.Fatalf("%d rows left, want 2 (the recent one and the unpublished one)", n)
	}
	if n := count(t, env, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 1 {
		t.Fatal("the unpublished row was deleted")
	}
}

func TestReconcileEnrollmentCountsJob_HealsCorruptedCounter(t *testing.T) {
	env := testenv.Setup(t)
	course, student := uuid.New(), uuid.New()
	if err := env.DB.Exec(`INSERT INTO courses (id, instructor_id, slug, enrollment_count) VALUES (?, ?, 'drift', 7)`, course, uuid.New()).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Exec(`INSERT INTO enrollments (id, student_id, course_id, status) VALUES (?, ?, ?, 'active')`, uuid.New(), student, course).Error; err != nil {
		t.Fatal(err)
	}
	if err := jobs.ReconcileEnrollmentCounts(env.DB, "0 * * * *").Run(context.Background()); err != nil {
		t.Fatalf("job: %v", err)
	}
	if n := count(t, env, `SELECT enrollment_count FROM courses WHERE id = ?`, course); n != 1 {
		t.Fatalf("enrollment_count = %d after the job, want 1", n)
	}
}

type countingRunner struct{ runs atomic.Int32 }

func (c *countingRunner) Run(context.Context) error { c.runs.Add(1); return nil }

// Two worker replicas share one Redis and register the same job. Over one
// cron tick both schedulers try to enqueue, Redis holds one task, and exactly
// one handler run happens. Waits for a real minute boundary (up to about a
// minute): cron has no finer tick and none of this can be sped up honestly.
func TestScheduledJobFiresOncePerTickAcrossTwoReplicas(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)
	runner := &countingRunner{} // shared: counts runs across both replicas

	var attempts atomic.Int32
	var schedulers []*asynqx.Scheduler
	for range 2 {
		sc, err := asynqx.NewScheduler(asynqx.SchedulerConfig{Redis: redis, OnAttempt: func(string, string) { attempts.Add(1) }})
		if err != nil {
			t.Fatal(err)
		}
		ss, err := asynqx.NewServers(asynqx.ServersConfig{Redis: redis, Pools: map[string]int{asynqx.QueueLow: 2}, DelayedTaskCheckInterval: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := jobs.Register(sc, ss, 1, jobs.ReconcileAnalytics(runner, "* * * * *")); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{}, 2)
		go func() { _ = ss.Run(ctx); done <- struct{}{} }()
		go func() { _ = sc.Run(ctx); done <- struct{}{} }()
		t.Cleanup(func() { cancel(); <-done; <-done })
		schedulers = append(schedulers, sc)
	}

	deadline := time.Now().Add(100 * time.Second)
	for runner.runs.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the scheduled job never ran")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Bounded absence check: both replicas' enqueues for this tick are in
	// within the skew tolerance; a second run would show up well before this.
	time.Sleep(4 * time.Second)
	if got := runner.runs.Load(); got != 1 {
		t.Fatalf("job ran %d times for one tick across two replicas, want 1", got)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("scheduler enqueue attempts = %d, want 2 (one per replica)", got)
	}
	if dup := schedulers[0].DuplicateCount() + schedulers[1].DuplicateCount(); dup != 1 {
		t.Fatalf("duplicate enqueues = %d, want 1 (the second replica's)", dup)
	}
}
