//go:build integration

// Queue namespaces against a real Redis: two services share one Redis database
// and the same logical queue names, and must never see each other's tasks (the
// defect: a worker dequeued another service's task, found no handler and
// archived it). Shares the helpers of asynqx_integration_test.go.
package asynqx_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestNamespace(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)
	sub := func(name string, fn func(t *testing.T, redis asynqx.RedisOpt)) {
		t.Run(name, func(t *testing.T) {
			if err := env.RDB.FlushAll(context.Background()).Err(); err != nil {
				t.Fatalf("flush redis: %v", err)
			}
			fn(t, redis)
		})
	}
	sub("TaskOfOneNamespaceIsNeverConsumedByAnother", testNamespaceIsolation)
	sub("InspectorAndGaugesReportNamespacedQueues", testNamespaceInspector)
	sub("SchedulersOfTwoNamespacesDoNotDedupeEachOther", testNamespaceSchedulers)
}

func nsClient(t *testing.T, redis asynqx.RedisOpt, ns string) *asynqx.Client {
	t.Helper()
	c := asynqx.NewClient(redis, asynqx.WithNamespace(ns))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func nsInspector(t *testing.T, redis asynqx.RedisOpt, ns string) *asynqx.Inspector {
	t.Helper()
	i := asynqx.NewInspector(redis, asynqx.WithNamespace(ns))
	t.Cleanup(func() { _ = i.Close() })
	return i
}

// Service "beta" runs with no handler for the task type (as identity-worker had
// none for notification:deliver). If it consumed alpha's task it would archive
// it: NoRetry makes that visible at once. The task must stay pending in
// alpha:critical until alpha's own server runs it.
func testNamespaceIsolation(t *testing.T, redis asynqx.RedisOpt) {
	alpha := nsClient(t, redis, "alpha")
	alphaInsp, betaInsp := nsInspector(t, redis, "alpha"), nsInspector(t, redis, "beta")
	plainInsp := newInspector(t, redis)

	pools := map[string]int{asynqx.QueueCritical: 1, asynqx.QueueDefault: 1, asynqx.QueueLow: 1}
	beta := newServers(t, asynqx.ServersConfig{Redis: redis, Namespace: "beta", Pools: pools})
	mustRegister(t, beta, "beta:only", asynqx.QueueLow, func(context.Context, []byte) error { return nil })
	runServers(t, beta)
	// An unprefixed server (a service that never set a namespace) is also blind
	// to alpha's queues.
	plain := newServers(t, asynqx.ServersConfig{Redis: redis, Pools: pools})
	mustRegister(t, plain, "plain:only", asynqx.QueueLow, func(context.Context, []byte) error { return nil })
	runServers(t, plain)

	res := mustEnqueue(t, alpha, asynqx.Job{Type: "alpha:work", ID: "w1", Queue: asynqx.QueueCritical, NoRetry: true})
	if res.Queue != "alpha:critical" {
		t.Fatalf("Result.Queue = %q, want alpha:critical", res.Queue)
	}

	// Give the foreign servers ample time to (wrongly) pull it.
	time.Sleep(2 * time.Second)
	if got := stats(t, alphaInsp, asynqx.QueueCritical); got.Pending != 1 || got.Archived != 0 || got.Active != 0 {
		t.Fatalf("alpha:critical = %+v; a foreign server touched the task", got)
	}
	for _, insp := range []*asynqx.Inspector{betaInsp, plainInsp} {
		if got := stats(t, insp, asynqx.QueueCritical); got.Pending+got.Archived+got.Active != 0 {
			t.Fatalf("%s = %+v, want empty: the task leaked into another namespace", got.Queue, got)
		}
	}

	var ran atomic.Int64
	alphaSrv := newServers(t, asynqx.ServersConfig{Redis: redis, Namespace: "alpha", Pools: pools})
	mustRegister(t, alphaSrv, "alpha:work", asynqx.QueueCritical, func(context.Context, []byte) error { ran.Add(1); return nil })
	runServers(t, alphaSrv)
	eventually(t, 10*time.Second, "alpha's own server ran the task", func() bool { return ran.Load() == 1 })
	if got := stats(t, alphaInsp, asynqx.QueueCritical); got.Archived != 0 {
		t.Errorf("alpha:critical archived = %d, want 0", got.Archived)
	}
}

func testNamespaceInspector(t *testing.T, redis asynqx.RedisOpt) {
	alpha, beta := nsClient(t, redis, "alpha"), nsClient(t, redis, "beta")
	m, err := telemetry.NewMetrics(telemetry.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, alpha, asynqx.Job{Type: "t", ID: "a1", Queue: asynqx.QueueCritical})
	mustEnqueue(t, alpha, asynqx.Job{Type: "t", ID: "a2", Queue: asynqx.QueueCritical})
	mustEnqueue(t, alpha, asynqx.Job{Type: "t", ID: "a3", Queue: asynqx.QueueLow})
	mustEnqueue(t, beta, asynqx.Job{Type: "t", ID: "b1", Queue: asynqx.QueueCritical})

	insp := nsInspector(t, redis, "alpha")
	all, err := insp.Stats()
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		queue   string
		pending int
	}{{"alpha:critical", 2}, {"alpha:default", 0}, {"alpha:low", 1}}
	if len(all) != len(want) {
		t.Fatalf("Stats = %+v", all)
	}
	for i, w := range want {
		if all[i].Queue != w.queue || all[i].Pending != w.pending {
			t.Errorf("Stats[%d] = %s pending %d, want %s pending %d", i, all[i].Queue, all[i].Pending, w.queue, w.pending)
		}
	}
	if state(t, insp, asynqx.QueueCritical, "a1") != "pending" || state(t, insp, asynqx.QueueCritical, "b1") != "absent" {
		t.Error("TaskState is not scoped to the namespace")
	}
	if _, err := insp.Stats("beta:critical"); err == nil {
		t.Error("Stats accepted another namespace's queue name")
	}

	if err := insp.Gauges(m).Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for q, w := range map[string]float64{"alpha:critical": 2, "alpha:default": 0, "alpha:low": 1} {
		if got := testutil.ToFloat64(m.AsynqQueueDepth.WithLabelValues(q)); got != w {
			t.Errorf("asynq_queue_depth{queue=%s} = %v, want %v", q, got, w)
		}
	}
	// The label set stays the service's three queues (bounded, no foreign ones).
	if n := testutil.CollectAndCount(m.AsynqQueueDepth); n != 3 {
		t.Errorf("asynq_queue_depth has %d series, want 3", n)
	}
	reg := telemetry.NewRegistry()
	m2, err := telemetry.NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := insp.Gauges(m2).Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := telemetry.CheckLabels(reg); err != nil {
		t.Errorf("CheckLabels: %v", err)
	}

	if _, err := nsClient(t, redis, "A:b").Enqueue(context.Background(), asynqx.Job{Type: "t", ID: "x"}); !errors.Is(err, asynqx.ErrInvalidJob) {
		t.Errorf("invalid namespace on the Client: err = %v, want ErrInvalidJob", err)
	}
}

// Both services schedule the same shared job name ("maintenance:outbox-prune")
// every minute. Each must run it: neither may swallow the other's tick.
func testNamespaceSchedulers(t *testing.T, redis asynqx.RedisOpt) {
	const job = "maintenance:outbox-prune"
	var mu sync.Mutex
	tickIDs := map[string]string{} // namespace -> tick task ID
	runs := map[string]*atomic.Int64{"alpha": {}, "beta": {}}
	for _, ns := range []string{"alpha", "beta"} {
		ss := newServers(t, asynqx.ServersConfig{Redis: redis, Namespace: ns, Pools: map[string]int{asynqx.QueueLow: 1}})
		counter := runs[ns]
		mustRegister(t, ss, job, asynqx.QueueLow, func(context.Context, []byte) error { counter.Add(1); return nil })
		runServers(t, ss)

		sc, err := asynqx.NewScheduler(asynqx.SchedulerConfig{
			Redis: redis, Namespace: ns,
			OnAttempt: func(_, id string) { mu.Lock(); tickIDs[ns] = id; mu.Unlock() },
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := sc.Register(asynqx.Entry{Name: job, Spec: "* * * * *", Type: job}); err != nil {
			t.Fatal(err)
		}
		if err := sc.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(sc.Shutdown)
	}

	eventually(t, 100*time.Second, "each namespace ran the shared job", func() bool {
		return runs["alpha"].Load() >= 1 && runs["beta"].Load() >= 1
	})
	mu.Lock()
	defer mu.Unlock()
	if tickIDs["alpha"] == tickIDs["beta"] {
		t.Errorf("both namespaces used tick ID %s: they would dedupe against each other", tickIDs["alpha"])
	}
	// Exactly one run per namespace for the tick (no double run, no steal).
	time.Sleep(time.Second)
	if a, b := runs["alpha"].Load(), runs["beta"].Load(); a != 1 || b != 1 {
		t.Errorf("runs alpha=%d beta=%d, want 1 each", a, b)
	}
}
