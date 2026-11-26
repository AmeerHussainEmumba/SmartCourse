//go:build integration

package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/jobs"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// recordingScheduler forwards to a real scheduler and keeps the entries.
type recordingScheduler struct {
	*asynqx.Scheduler
	entries []asynqx.Entry
}

func (r *recordingScheduler) Register(e asynqx.Entry) error {
	r.entries = append(r.entries, e)
	return r.Scheduler.Register(e)
}

// A job registered through jobs.Register runs on the namespaced low queue of the
// scheduler/servers it was given, and a server of another namespace on the same
// Redis does not take it.
func TestRegister_JobRunsOnTheNamespacedLowQueue(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)
	const ns, name = "course", "maintenance:outbox-prune"

	servers, err := asynqx.NewServers(asynqx.ServersConfig{
		Redis: redis, Namespace: ns, Pools: map[string]int{asynqx.QueueLow: 1},
		DelayedTaskCheckInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := asynqx.NewScheduler(asynqx.SchedulerConfig{Redis: redis, Namespace: ns})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingScheduler{Scheduler: sc}
	t.Cleanup(sc.Shutdown)

	var ran atomic.Int64
	err = jobs.Register(rec, servers, 1, jobs.Job{Name: name, Spec: "0 0 1 1 *", Run: func(context.Context) error { ran.Add(1); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if got := servers.Queues(); len(got) != 1 || got[0] != "course:low" {
		t.Fatalf("servers.Queues = %v, want [course:low]", got)
	}
	if len(rec.entries) != 1 || rec.entries[0].Queue != asynqx.QueueLow {
		t.Fatalf("entries = %+v, want one on the logical low queue", rec.entries)
	}

	// A server of another service, no handler for the job: it must not see it.
	other, err := asynqx.NewServers(asynqx.ServersConfig{
		Redis: redis, Namespace: "identity", Pools: map[string]int{asynqx.QueueLow: 1},
		DelayedTaskCheckInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 2)
	for _, s := range []*asynqx.Servers{servers, other} {
		go func() { _ = s.Run(ctx); done <- struct{}{} }()
	}
	t.Cleanup(func() { cancel(); <-done; <-done })

	c := asynqx.NewClient(redis, asynqx.WithNamespace(ns))
	t.Cleanup(func() { _ = c.Close() })
	res, err := c.Enqueue(context.Background(), asynqx.Job{Type: name, ID: "t1", Queue: asynqx.QueueLow, NoRetry: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Queue != "course:low" {
		t.Fatalf("queue = %q, want course:low", res.Queue)
	}
	deadline := time.Now().Add(10 * time.Second)
	for ran.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if ran.Load() != 1 {
		t.Fatalf("job ran %d times, want 1", ran.Load())
	}
	insp := asynqx.NewInspector(redis, asynqx.WithNamespace("identity"))
	t.Cleanup(func() { _ = insp.Close() })
	st, err := insp.Stats(asynqx.QueueLow)
	if err != nil || st[0].Archived != 0 {
		t.Fatalf("identity:low = %+v, err %v: the other namespace touched the task", st, err)
	}
}
