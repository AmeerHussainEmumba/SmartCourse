//go:build integration

// Resource-leak attacks: requests whose context is cancelled at random points
// mid-query / mid-transaction must not leak pooled connections, open
// transactions, goroutines or Redis connections.
package course_test

import (
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAdvResources_CancelledRequests_NoPoolConnectionTransactionOrGoroutineLeak(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	r := newTestEngine(svc)
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 40; i++ {
		insertPublished(t, env, instr, "leak-"+string(rune('a'+i/26))+string(rune('a'+i%26)), base.Add(time.Duration(i)*time.Second))
	}
	// warm pools / lazily started goroutines so the baseline is honest
	if _, err := svc.GetPublishedDetail(context.Background(), "leak-aa"); err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := env.DB.DB()
	baseline := goleak.IgnoreCurrent()

	var wg sync.WaitGroup
	const workers, iterations = 40, 40
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < iterations; i++ {
				// deadline between 0 and ~3ms: cancels land at every stage (before
				// acquire, mid-query, between statements of a multi-step read).
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rng.Intn(3000))*time.Microsecond)
				switch i % 4 {
				case 0:
					req := httptest.NewRequest(http.MethodGet, "/api/v1/courses?limit=30", nil).WithContext(ctx)
					r.ServeHTTP(httptest.NewRecorder(), req)
				case 1:
					req := httptest.NewRequest(http.MethodGet, "/api/v1/courses/leak-aa", nil).WithContext(ctx)
					r.ServeHTTP(httptest.NewRecorder(), req)
				case 2:
					_, _ = svc.SearchCourses(ctx, "Title", 20)
				case 3:
					_, _ = svc.ListCatalogue(ctx, 10, "", "", "", "Title")
				}
				cancel()
			}
		}(w)
	}
	wg.Wait()

	waitUntil(t, 10*time.Second, "all pooled connections returned", func() bool { return sqlDB.Stats().InUse == 0 })
	if st := sqlDB.Stats(); st.OpenConnections > 10 {
		t.Fatalf("pool exceeded MaxOpenConns: %+v", st)
	}
	// No transaction was left open server-side by a cancelled request.
	waitUntil(t, 10*time.Second, "no idle-in-transaction sessions", func() bool {
		var n int64
		env.DB.Raw(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state LIKE 'idle in transaction%'`).Row().Scan(&n)
		return n == 0
	})
	// The pool still serves normal traffic promptly (not exhausted).
	start := time.Now()
	for i := 0; i < 50; i++ {
		if _, err := svc.ListCatalogue(context.Background(), 5, "", "", "", ""); err != nil {
			t.Fatalf("post-storm request failed: %v", err)
		}
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("pool is starved after the cancellation storm: 50 requests took %s", d)
	}
	if err := goleak.Find(baseline, goleak.IgnoreTopFunction("database/sql.(*DB).connectionCleaner")); err != nil {
		t.Fatalf("goroutines leaked after cancelled requests: %v", err)
	}
}

func TestAdvResources_RedisConnectionsReusedAndBounded(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	courseID, _ := newCourse(t, svc, instr, "redis-reuse", "Redis Reuse")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)
	ctx := context.Background()
	if _, err := svc.GetPublishedDetail(ctx, "redis-reuse"); err != nil {
		t.Fatal(err)
	}

	before := env.RDB.PoolStats()
	for i := 0; i < 300; i++ { // sequential: one connection should serve everything
		if _, err := svc.GetPublishedDetail(ctx, "redis-reuse"); err != nil {
			t.Fatal(err)
		}
	}
	after := env.RDB.PoolStats()
	if after.Misses-before.Misses > 1 {
		t.Fatalf("sequential reads dialed %d new Redis connections instead of reusing one", after.Misses-before.Misses)
	}
	if after.Hits-before.Hits < 290 {
		t.Fatalf("pool hits grew by only %d for 300 reads", after.Hits-before.Hits)
	}

	// Concurrent + randomly cancelled: bounded and recoverable.
	var wg sync.WaitGroup
	for w := 0; w < 50; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 40; i++ {
				c, cancel := context.WithTimeout(ctx, time.Duration(rng.Intn(2000))*time.Microsecond)
				_, _ = svc.GetPublishedDetail(c, "redis-reuse")
				cancel()
			}
		}(w)
	}
	wg.Wait()
	st := env.RDB.PoolStats()
	if st.Timeouts != 0 {
		t.Fatalf("Redis pool timeouts: %+v", st)
	}
	if st.TotalConns > 10*uint32(maxProcs()) { // go-redis default PoolSize
		t.Fatalf("Redis pool grew past its bound: %+v", st)
	}
	waitUntil(t, 5*time.Second, "redis conns idle", func() bool { s := env.RDB.PoolStats(); return s.TotalConns == s.IdleConns })
	if err := env.RDB.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis unusable after cancelled calls: %v", err)
	}
}

func maxProcs() int { return runtime.GOMAXPROCS(0) }
