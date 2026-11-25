package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
)

type fakeScheduler struct{ entries []asynqx.Entry }

func (f *fakeScheduler) Register(e asynqx.Entry) error { f.entries = append(f.entries, e); return nil }

type fakeRegistry struct{ handlers map[string]func(context.Context, []byte) error }

func (f *fakeRegistry) Register(t, q string, fn func(context.Context, []byte) error) error {
	if f.handlers == nil {
		f.handlers = map[string]func(context.Context, []byte) error{}
	}
	if q != asynqx.QueueLow {
		return errors.New("jobs must run on the low queue")
	}
	f.handlers[t] = fn
	return nil
}

func TestRegister_SchedulesEveryJobAndRunsItsFunction(t *testing.T) {
	ran := 0
	sc, reg := &fakeScheduler{}, &fakeRegistry{}
	j := Job{Name: "maintenance:x", Spec: "* * * * *", Timeout: time.Second, Run: func(context.Context) error { ran++; return nil }}
	if err := Register(sc, reg, 3, j); err != nil {
		t.Fatal(err)
	}
	if len(sc.entries) != 1 || sc.entries[0].MaxRetry != 3 || sc.entries[0].Queue != asynqx.QueueLow {
		t.Fatalf("entries = %+v", sc.entries)
	}
	if err := reg.handlers["maintenance:x"](context.Background(), nil); err != nil || ran != 1 {
		t.Fatalf("handler err=%v ran=%d", err, ran)
	}
}

func TestRegister_RejectsAJobWithoutRun(t *testing.T) {
	if err := Register(&fakeScheduler{}, &fakeRegistry{}, 1, Job{Name: "x"}); err == nil {
		t.Fatal("a job with no Run must be refused")
	}
}

func TestRunnerJob_NilRunnerFailsTheJobInsteadOfPanicking(t *testing.T) {
	if err := RunnerJob("x", "* * * * *", time.Second, nil).Run(context.Background()); err == nil {
		t.Fatal("want an error for a nil Runner")
	}
}

func TestPruneJobs_RejectNonPositiveRetention(t *testing.T) {
	if _, err := PruneOutbox(nil, 0, "* * * * *"); err == nil {
		t.Error("outbox retention 0 must be refused")
	}
	if _, err := PruneProcessedEvents(nil, -time.Hour, "* * * * *"); err == nil {
		t.Error("processed_events retention <= 0 must be refused")
	}
}
