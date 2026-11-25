package asynqx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

// A Redis address nothing listens on: NewServers must not dial.
var unusedRedis = RedisClient("127.0.0.1:1", "", 0)

func TestNewServers_DefaultsAreTheRFCPools(t *testing.T) {
	ss, err := NewServers(ServersConfig{Redis: unusedRedis})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(ss.Queues(), ","); got != "critical,default,low" {
		t.Fatalf("queues = %s", got)
	}
	want := map[string]int{QueueCritical: 10, QueueDefault: 5, QueueLow: 3}
	for q, n := range want {
		if got := ss.byQueue[q].cfg.Concurrency; got != n {
			t.Errorf("pool %s = %d, want %d", q, got, n)
		}
		// One server per queue, serving only that queue: that is what bounds
		// the pools separately.
		if qs := ss.byQueue[q].cfg.Queues; len(qs) != 1 || qs[q] == 0 {
			t.Errorf("server for %s serves %v", q, qs)
		}
	}
}

// Asynq's own default check interval (5s) made every retry gap about 5s
// whatever the backoff said (E07). Unset means one second on every server.
func TestNewServers_DelayedTaskCheckIntervalDefaultsToOneSecond(t *testing.T) {
	ss, err := NewServers(ServersConfig{Redis: unusedRedis})
	if err != nil {
		t.Fatal(err)
	}
	for q, s := range ss.byQueue {
		if got := s.cfg.DelayedTaskCheckInterval; got != time.Second {
			t.Errorf("queue %s: DelayedTaskCheckInterval = %v, want 1s", q, got)
		}
	}
	ss, err = NewServers(ServersConfig{Redis: unusedRedis, DelayedTaskCheckInterval: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if got := ss.byQueue[QueueCritical].cfg.DelayedTaskCheckInterval; got != 250*time.Millisecond {
		t.Errorf("an explicit interval was overridden: %v", got)
	}
	if _, err := NewServer(ServerConfig{Redis: unusedRedis, DelayedTaskCheckInterval: -time.Second}); err == nil {
		t.Error("a negative interval must be rejected")
	}
}

func TestNewServers_ConfiguredPoolsOnly(t *testing.T) {
	ss, err := NewServers(ServersConfig{Redis: unusedRedis, Pools: map[string]int{QueueLow: 2, QueueCritical: 7}})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(ss.Queues(), ","); got != "critical,low" {
		t.Fatalf("queues = %s", got)
	}
	if ss.byQueue[QueueLow].cfg.Concurrency != 2 || ss.byQueue[QueueCritical].cfg.Concurrency != 7 {
		t.Fatal("pool sizes not applied")
	}
}

func TestNewServers_Validation(t *testing.T) {
	cases := map[string]ServersConfig{
		"no redis":        {},
		"empty pools":     {Redis: unusedRedis, Pools: map[string]int{}},
		"unknown queue":   {Redis: unusedRedis, Pools: map[string]int{"urgent": 1}},
		"zero pool":       {Redis: unusedRedis, Pools: map[string]int{QueueLow: 0}},
		"negative pool":   {Redis: unusedRedis, Pools: map[string]int{QueueLow: -1}},
		"max below base":  {Redis: unusedRedis, Backoff: BackoffConfig{Base: time.Minute, Max: time.Second}},
		"jitter of one":   {Redis: unusedRedis, Backoff: BackoffConfig{Jitter: 1}},
		"negative base":   {Redis: unusedRedis, Backoff: BackoffConfig{Base: -time.Second}},
		"negative jitter": {Redis: unusedRedis, Backoff: BackoffConfig{Jitter: -0.1}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewServers(cfg); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}

func TestBackoffValidate_ZeroValueAndDefaultAreValid(t *testing.T) {
	for _, b := range []BackoffConfig{{}, DefaultBackoff, {Base: time.Second}, {Max: time.Minute}} {
		if err := b.Validate(); err != nil {
			t.Errorf("%+v: %v", b, err)
		}
	}
}

func TestServersRegister_RoutesPayloadToTheQueueServer(t *testing.T) {
	ss, err := NewServers(ServersConfig{Redis: unusedRedis})
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	if err := ss.Register("t:one", QueueCritical, func(_ context.Context, p []byte) error { got = p; return nil }); err != nil {
		t.Fatal(err)
	}
	// Dispatch through the critical server's mux, as a pulled task would be.
	if err := ss.byQueue[QueueCritical].mux.ProcessTask(context.Background(), asynq.NewTask("t:one", []byte("hello"))); err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("payload = %q", got)
	}
	// The same type is not routable on another queue's server.
	if err := ss.byQueue[QueueLow].mux.ProcessTask(context.Background(), asynq.NewTask("t:one", nil)); err == nil {
		t.Fatal("task type leaked onto the low queue's server")
	}
}

func TestServersRegister_HandlerErrorAndPanicBecomeErrors(t *testing.T) {
	ss, _ := NewServers(ServersConfig{Redis: unusedRedis})
	boom := errors.New("boom")
	_ = ss.Register("t:err", QueueDefault, func(context.Context, []byte) error { return boom })
	_ = ss.Register("t:panic", QueueDefault, func(context.Context, []byte) error { panic("x") })
	mux := ss.byQueue[QueueDefault].mux
	if err := mux.ProcessTask(context.Background(), asynq.NewTask("t:err", nil)); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if err := mux.ProcessTask(context.Background(), asynq.NewTask("t:panic", nil)); !errors.Is(err, ErrPanic) {
		t.Fatalf("panic err = %v", err)
	}
	if ss.PanicCount() != 1 {
		t.Fatalf("PanicCount = %d", ss.PanicCount())
	}
}

func TestServersRegister_Rejections(t *testing.T) {
	ss, _ := NewServers(ServersConfig{Redis: unusedRedis, Pools: map[string]int{QueueLow: 1}})
	ok := func(context.Context, []byte) error { return nil }
	if err := ss.Register("t:a", QueueCritical, ok); !errors.Is(err, ErrUnknownQueue) {
		t.Errorf("queue without a pool: %v", err)
	}
	if err := ss.Register("", QueueLow, ok); err == nil {
		t.Error("empty type accepted")
	}
	if err := ss.Register("t:a", QueueLow, nil); err == nil {
		t.Error("nil handler accepted")
	}
	if err := ss.Register("t:a", QueueLow, ok); err != nil {
		t.Fatal(err)
	}
	if err := ss.Register("t:a", QueueLow, ok); err == nil {
		t.Error("duplicate task type accepted")
	}
}
