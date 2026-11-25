package main

// Wiring guards for identity-worker (ported in spirit from the old worker's
// wire_test.go and wire_asynq_test.go): each is a place where dropping one line
// would leave every other test green and the feature silently off.

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/jobs"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

func testConfig(redisAddr string) *config.Config {
	return &config.Config{
		Redis: config.RedisConfig{Addr: redisAddr},
		Relay: config.RelayConfig{Workers: 3},
		Asynq: config.AsynqConfig{
			CriticalConcurrency: 10, DefaultConcurrency: 5, LowConcurrency: 3,
			MaxRetry: 5, BackoffBase: time.Second, BackoffMax: 5 * time.Second, ShutdownTimeout: 5 * time.Second,
		},
		Jobs: config.JobsConfig{OutboxPruneInterval: time.Hour, OutboxRetention: 7 * 24 * time.Hour},
	}
}

type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, relay.DecodedEvent) error { return nil }

func field(v reflect.Value, names ...string) reflect.Value {
	for _, n := range names {
		for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
			v = v.Elem()
		}
		v = v.FieldByName(n)
	}
	return v
}

// The identity relay only publishes: it carries a publisher and not one handler
// (identity consumes nothing), so it can never run another service's code.
func TestBuildRelay_IsPublishOnly(t *testing.T) {
	rl := buildRelay(nil, testConfig("x"), nopPublisher{}, false)
	if hs := rl.Handlers(); len(hs) != 0 {
		t.Fatalf("identity relay has %d handlers, want none", len(hs))
	}
	if field(reflect.ValueOf(rl), "publisher").IsNil() {
		t.Fatal("identity relay has no publisher")
	}
}

// RELAY_WORKERS must reach the relay (Load sized the Postgres pool against it).
func TestBuildRelay_UsesConfiguredWorkerCount(t *testing.T) {
	cfg := testConfig("x")
	cfg.Relay.Workers = 7
	rl := buildRelay(nil, cfg, nopPublisher{}, false)
	if got := field(reflect.ValueOf(rl), "workerCount").Int(); got != 7 {
		t.Fatalf("relay workerCount = %d, want 7", got)
	}
}

func jobNames(js []jobs.Job) []string {
	var out []string
	for _, j := range js {
		out = append(out, j.Name+"@"+j.Spec)
	}
	sort.Strings(out)
	return out
}

// Identity has no consumers (no processed_events) and no idempotency table:
// the only housekeeping is PruneOutbox.
func TestBuildJobs_IsPruneOutboxOnly(t *testing.T) {
	js, err := buildJobs(nil, testConfig("x"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := jobNames(js), []string{jobs.TypeOutboxPrune + "@0 * * * *"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("jobs = %v, want %v", got, want)
	}
}

func TestBuildJobs_UsesConfiguredIntervalAndRefusesUnusableOnes(t *testing.T) {
	cfg := testConfig("x")
	cfg.Jobs.OutboxPruneInterval = 6 * time.Hour
	js, err := buildJobs(nil, cfg)
	if err != nil || js[0].Spec != "0 */6 * * *" {
		t.Fatalf("jobs = %v, %v", js, err)
	}
	cfg.Jobs.OutboxPruneInterval = 7 * time.Minute
	if _, err := buildJobs(nil, cfg); err == nil {
		t.Error("a 7 minute interval has no common cron tick and must be refused")
	}
	cfg = testConfig("x")
	cfg.Jobs.OutboxRetention = 0
	if _, err := buildJobs(nil, cfg); err == nil {
		t.Error("a zero outbox retention must be refused, not delete everything")
	}
}

// The pool sizes of RFC §10.2 reach the servers and every job has a handler on
// the low queue.
func TestBuildAsynq_PoolsPerQueueAndJobHandlers(t *testing.T) {
	cfg := testConfig("127.0.0.1:1")
	cfg.Asynq.CriticalConcurrency, cfg.Asynq.DefaultConcurrency, cfg.Asynq.LowConcurrency = 7, 4, 2
	js, err := buildJobs(nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	servers, scheduler, err := buildAsynq(cfg, nil, js)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Shutdown()

	if got := servers.Queues(); !reflect.DeepEqual(got, []string{"identity:critical", "identity:default", "identity:low"}) {
		t.Fatalf("queues = %v", got)
	}
	if servers.Namespace() != "identity" || scheduler.Namespace() != "identity" {
		t.Errorf("namespaces: servers %q, scheduler %q, want identity", servers.Namespace(), scheduler.Namespace())
	}
	by := field(reflect.ValueOf(servers), "byQueue")
	for q, want := range map[string]int64{asynqx.QueueCritical: 7, asynqx.QueueDefault: 4, asynqx.QueueLow: 2} {
		srv := by.MapIndex(reflect.ValueOf(q))
		if got := field(srv, "cfg", "Concurrency").Int(); got != want {
			t.Errorf("pool %s = %d, want %d", q, got, want)
		}
	}
	types := field(reflect.ValueOf(servers), "types")
	if types.Len() != len(js) {
		t.Fatalf("%d task types registered, want %d", types.Len(), len(js))
	}
	for _, j := range js {
		if q := types.MapIndex(reflect.ValueOf(j.Name)); !q.IsValid() || q.String() != asynqx.QueueLow {
			t.Errorf("job %s has no handler on the low queue", j.Name)
		}
	}
}

func TestBuildAsynq_RejectsABrokenPoolConfig(t *testing.T) {
	cfg := testConfig("127.0.0.1:1")
	cfg.Asynq.LowConcurrency = 0
	if _, _, err := buildAsynq(cfg, nil, nil); err == nil {
		t.Fatal("a zero-size pool must be refused")
	}
}
