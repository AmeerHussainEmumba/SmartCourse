package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

type fakeScheduler struct {
	entries []asynqx.Entry
	err     error
}

func (f *fakeScheduler) Register(e asynqx.Entry) error {
	if f.err != nil {
		return f.err
	}
	f.entries = append(f.entries, e)
	return nil
}

type fakeRegistry struct {
	handlers map[string]func(context.Context, []byte) error
	queues   map[string]string
	err      error
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{handlers: map[string]func(context.Context, []byte) error{}, queues: map[string]string{}}
}

func (f *fakeRegistry) Register(typ, queue string, fn func(context.Context, []byte) error) error {
	if f.err != nil {
		return f.err
	}
	f.handlers[typ], f.queues[typ] = fn, queue
	return nil
}

type fakeRunner struct {
	calls int
	err   error
}

func (f *fakeRunner) Run(context.Context) error { f.calls++; return f.err }

type fakePublishes struct {
	calls int
	res   course.ReconcileResult
	err   error
}

func (f *fakePublishes) ReconcileStalePublishes(context.Context) (course.ReconcileResult, error) {
	f.calls++
	return f.res, f.err
}

func TestRegister_SchedulesEachJobOnTheLowQueueAndRunsItsHandler(t *testing.T) {
	sc, reg := &fakeScheduler{}, newFakeRegistry()
	an, pub := &fakeRunner{}, &fakePublishes{}
	js := []Job{ReconcileStalePublishes(pub, "* * * * *"), ReconcileAnalytics(an, "*/5 * * * *")}
	if err := Register(sc, reg, 4, js...); err != nil {
		t.Fatal(err)
	}
	if len(sc.entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(sc.entries))
	}
	want := []asynqx.Entry{
		{Name: TypeReconcileStalePublishes, Type: TypeReconcileStalePublishes, Spec: "* * * * *", Queue: asynqx.QueueLow, MaxRetry: 4, Timeout: everyMinuteTimeout},
		{Name: TypeAnalyticsReconcile, Type: TypeAnalyticsReconcile, Spec: "*/5 * * * *", Queue: asynqx.QueueLow, MaxRetry: 4, Timeout: everyMinuteTimeout},
	}
	for i, w := range want {
		if got := sc.entries[i]; got.Name != w.Name || got.Type != w.Type || got.Spec != w.Spec || got.Queue != w.Queue || got.MaxRetry != w.MaxRetry || got.Timeout != w.Timeout {
			t.Errorf("entry %d = %+v, want %+v", i, got, w)
		}
		if reg.queues[w.Type] != asynqx.QueueLow {
			t.Errorf("handler for %s on queue %q, want low", w.Type, reg.queues[w.Type])
		}
	}
	// The scheduled task type must be one a handler is registered for.
	for _, e := range sc.entries {
		if err := reg.handlers[e.Type](context.Background(), nil); err != nil {
			t.Errorf("%s: %v", e.Type, err)
		}
	}
	if an.calls != 1 || pub.calls != 1 {
		t.Fatalf("runner calls analytics=%d publishes=%d, want 1 each", an.calls, pub.calls)
	}
}

func TestRegister_HandlerPropagatesTheJobsError(t *testing.T) {
	boom := errors.New("boom")
	reg := newFakeRegistry()
	if err := Register(&fakeScheduler{}, reg, 1, ReconcileAnalytics(&fakeRunner{err: boom}, "* * * * *")); err != nil {
		t.Fatal(err)
	}
	if err := reg.handlers[TypeAnalyticsReconcile](context.Background(), nil); !errors.Is(err, boom) {
		t.Fatalf("handler error = %v, want the runner's error so Asynq retries", err)
	}
}

func TestRegister_Failures(t *testing.T) {
	ok := ReconcileAnalytics(&fakeRunner{}, "* * * * *")
	if err := Register(&fakeScheduler{}, newFakeRegistry(), 1, Job{Name: "x", Spec: "* * * * *"}); err == nil {
		t.Error("a job without Run was accepted")
	}
	if err := Register(&fakeScheduler{err: errors.New("bad spec")}, newFakeRegistry(), 1, ok); err == nil {
		t.Error("scheduler failure swallowed")
	}
	if err := Register(&fakeScheduler{}, &fakeRegistry{err: errors.New("dup")}, 1, ok); err == nil {
		t.Error("registry failure swallowed")
	}
}

func TestReconcileStalePublishes_ReturnsTheServiceError(t *testing.T) {
	boom := errors.New("temporal down")
	pub := &fakePublishes{res: course.ReconcileResult{Checked: 2, Errors: 2}, err: boom}
	if err := ReconcileStalePublishes(pub, "* * * * *").Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if pub.calls != 1 {
		t.Fatalf("calls = %d", pub.calls)
	}
}

func TestReconcileAnalytics_NilRunnerFailsTheJobInsteadOfPanicking(t *testing.T) {
	if err := ReconcileAnalytics(nil, "* * * * *").Run(context.Background()); err == nil {
		t.Fatal("want an error for a nil Runner")
	}
}

func TestPruneOutbox_RejectsNonPositiveRetention(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Hour} {
		if _, err := PruneOutbox(nil, d, "0 * * * *"); err == nil {
			t.Errorf("retention %v accepted", d)
		}
	}
	j, err := PruneOutbox(nil, time.Hour, "0 * * * *")
	if err != nil || j.Name != TypeOutboxPrune || j.Timeout != hourlyTimeout {
		t.Fatalf("job = %+v, err = %v", j, err)
	}
}

func TestJobNamesAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for _, n := range []string{TypeReconcileStalePublishes, TypeAnalyticsReconcile, TypeEnrollmentCounts, TypeIdempotencyPrune, TypeOutboxPrune} {
		if seen[n] {
			t.Errorf("duplicate job name %s", n)
		}
		seen[n] = true
	}
}
