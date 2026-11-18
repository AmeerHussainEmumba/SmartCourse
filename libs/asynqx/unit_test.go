package asynqx

import (
	"errors"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

func TestBackoff_DoublesThenCaps(t *testing.T) {
	b := BackoffConfig{Base: time.Second, Max: 10 * time.Second}
	want := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	for n, w := range want {
		if got := b.Delay(n); got != w*time.Second {
			t.Errorf("Delay(%d) = %v, want %v", n, got, w*time.Second)
		}
	}
}

func TestBackoff_HugeRetryCountDoesNotOverflow(t *testing.T) {
	b := BackoffConfig{Base: time.Second, Max: time.Minute}
	if got := b.Delay(1 << 30); got != time.Minute {
		t.Fatalf("Delay(huge) = %v, want the cap", got)
	}
}

func TestBackoff_JitterOnlyShortens(t *testing.T) {
	b := BackoffConfig{Base: time.Second, Max: 8 * time.Second, Jitter: 0.5}
	for range 200 {
		d := b.Delay(10)
		if d > 8*time.Second || d < 4*time.Second {
			t.Fatalf("jittered capped delay %v outside [4s, 8s]", d)
		}
	}
}

func TestDeterministicID(t *testing.T) {
	first, second := DeterministicID("a", "b"), DeterministicID("a", "b")
	if first != second {
		t.Fatal("not deterministic")
	}
	if DeterministicID("ab", "c") == DeterministicID("a", "bc") {
		t.Fatal("part boundaries must matter")
	}
}

func TestJobOptions_Validation(t *testing.T) {
	bad := map[string]Job{
		"no type":       {ID: "x"},
		"no id":         {Type: "t"},
		"unknown queue": {Type: "t", ID: "x", Queue: "vip"},
		"negative":      {Type: "t", ID: "x", MaxRetry: -1},
	}
	for name, j := range bad {
		if _, _, err := j.options(); !errors.Is(err, ErrInvalidJob) {
			t.Errorf("%s: err = %v, want ErrInvalidJob", name, err)
		}
	}
	if _, q, err := (Job{Type: "t", ID: "x"}).options(); err != nil || q != QueueDefault {
		t.Errorf("defaults: queue=%q err=%v", q, err)
	}
}

func TestNewServer_Validation(t *testing.T) {
	r := RedisClient("127.0.0.1:1", "", 0)
	for name, cfg := range map[string]ServerConfig{
		"no redis":       {},
		"neg concurrent": {Redis: r, Concurrency: -1},
		"bad weight":     {Redis: r, Queues: map[string]int{QueueLow: 0}},
		"unknown queue":  {Redis: r, Queues: map[string]int{"vip": 1}},
		"neg shutdown":   {Redis: r, ShutdownTimeout: -1},
	} {
		if _, err := NewServer(cfg); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestTickSlot_ReplicasWithinToleranceAgree(t *testing.T) {
	s, err := cron.ParseStandard("* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	tick := time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC)
	want := tickSlot(s, tick)
	if !want.Equal(tick) {
		t.Fatalf("slot of an exact tick = %v, want %v", want, tick)
	}
	for _, skew := range []time.Duration{0, time.Millisecond, 2 * time.Second, TickSkewTolerance - time.Millisecond} {
		if got := tickSlot(s, tick.Add(skew)); !got.Equal(want) {
			t.Errorf("skew %v: slot %v, want %v", skew, got, want)
		}
	}
	if got := tickSlot(s, tick.Add(time.Minute)); got.Equal(want) {
		t.Error("the next tick must have a different slot")
	}
	if TickID("a", want) == TickID("b", want) {
		t.Error("entries must have distinct IDs for the same tick")
	}
}

func TestScheduler_RegisterValidation(t *testing.T) {
	sc, err := NewScheduler(SchedulerConfig{Redis: RedisClient("127.0.0.1:1", "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.Register(Entry{Name: "a", Spec: "@every 60s", Type: "t"}); !errors.Is(err, ErrInvalidJob) {
		t.Errorf("@every: %v", err)
	}
	if err := sc.Register(Entry{Name: "a", Spec: "nonsense", Type: "t"}); !errors.Is(err, ErrInvalidJob) {
		t.Errorf("bad spec: %v", err)
	}
	if err := sc.Register(Entry{Name: "a", Spec: "* * * * *", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := sc.Register(Entry{Name: "a", Spec: "* * * * *", Type: "t"}); !errors.Is(err, ErrDuplicateEntry) {
		t.Errorf("duplicate name: %v", err)
	}
}
