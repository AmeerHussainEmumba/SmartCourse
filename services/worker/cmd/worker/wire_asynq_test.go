package main

// Wiring guards for the Asynq side and the pieces main.go depends on, in the
// same spirit as wire_test.go: each of these is a place where dropping one line
// would leave every other test green and the feature silently off.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/jobs"
)

// relayTestConfig is the part of the config buildRelay reads.
func relayTestConfig() *config.Config {
	return &config.Config{Relay: config.RelayConfig{Workers: 3}}
}

// asynqTestConfig carries the defaults Load applies, with Redis pointed at addr.
func asynqTestConfig(redisAddr string) *config.Config {
	return &config.Config{
		Redis: config.RedisConfig{Addr: redisAddr},
		Asynq: config.AsynqConfig{
			CriticalConcurrency: 10, DefaultConcurrency: 5, LowConcurrency: 3,
			MaxRetry: 5, BackoffBase: time.Second, BackoffMax: 5 * time.Second, ShutdownTimeout: 5 * time.Second,
		},
		Jobs: config.JobsConfig{
			ReconcilePublishesInterval: time.Minute, AnalyticsReconcileInterval: time.Minute,
			EnrollmentCountInterval: time.Hour, IdempotencyPruneInterval: time.Hour, OutboxPruneInterval: time.Hour,
			OutboxRetention: 7 * 24 * time.Hour,
		},
		Temporal: config.TemporalConfig{HostPort: "127.0.0.1:1", Namespace: "default", TaskQueue: "wiring-test-queue"},
	}
}

func field(v reflect.Value, names ...string) reflect.Value {
	for _, n := range names {
		for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
			v = v.Elem()
		}
		v = v.FieldByName(n)
	}
	return v
}

// RELAY_WORKERS must reach the relay: Load validates it and sizes the Postgres
// pool against it, so a relay left at its own default would be a silent mismatch.
func TestBuildRelay_UsesConfiguredWorkerCount(t *testing.T) {
	rl := buildRelay(&infra.Dependencies{}, relayTestConfig())
	if got := field(reflect.ValueOf(rl), "workerCount").Int(); got != 3 {
		t.Fatalf("relay workerCount = %d, want RELAY_WORKERS (3)", got)
	}
}

// The Temporal worker's course service must have the asset store attached, or
// VerifyAssets fails every publish with an asset (fail closed, as in course-api).
// With bad MinIO settings the store is left off and the service is still built.
func TestNewCourseService_AttachesTheAssetStore(t *testing.T) {
	cl, err := temporalclient.NewLazyClient(temporalclient.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	cfg := &config.Config{MinIO: config.MinIOConfig{Endpoint: "http://minio:9000", BucketAssets: "b", AccessKeyID: "k", SecretAccessKey: "s"}}
	svc := newCourseService(&infra.Dependencies{}, cfg, cl)
	if field(reflect.ValueOf(svc), "assets").IsNil() {
		t.Error("asset store not attached with valid MinIO settings")
	}
	if svc := newCourseService(&infra.Dependencies{}, &config.Config{}, cl); svc == nil || !field(reflect.ValueOf(svc), "assets").IsNil() {
		t.Error("with unusable MinIO settings the store must be absent (VerifyAssets then fails closed), not half-built")
	}
}

// buildPublishWorker must get its course service from newCourseService; building
// one by hand there would silently skip the asset store again.
func TestBuildPublishWorker_UsesNewCourseService(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(filepath.Dir(thisFile), "wire.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var calls, direct bool
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "buildPublishWorker" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				calls = calls || fun.Name == "newCourseService"
			case *ast.SelectorExpr:
				direct = direct || fun.Sel.Name == "NewService"
			}
			return true
		})
	}
	if !calls || direct {
		t.Fatalf("buildPublishWorker: calls newCourseService=%v, builds course.NewService itself=%v; want true/false", calls, direct)
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

type stubRunner struct{}

func (stubRunner) Run(context.Context) error { return nil }

type stubPublishes struct{}

func (stubPublishes) ReconcileStalePublishes(context.Context) (course.ReconcileResult, error) {
	return course.ReconcileResult{}, nil
}

// All of RFC §9.5's scheduled jobs, on the specs the default intervals give.
func TestBuildJobs_SchedulesEveryJobWhenEverythingIsWired(t *testing.T) {
	js, err := buildJobs(&infra.Dependencies{}, asynqTestConfig("x"), stubPublishes{}, stubRunner{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		jobs.TypeAnalyticsReconcile + "@* * * * *",
		jobs.TypeEnrollmentCounts + "@0 * * * *",
		jobs.TypeIdempotencyPrune + "@0 * * * *",
		jobs.TypeOutboxPrune + "@0 * * * *",
		jobs.TypeReconcileStalePublishes + "@* * * * *",
	}
	sort.Strings(want)
	if got := jobNames(js); !reflect.DeepEqual(got, want) {
		t.Fatalf("jobs = %v\nwant %v", got, want)
	}
}

// An unwired dependency drops only its own job: the analytics Runner is not
// plugged in yet, and that must not stop pruning and the other reconciliations.
func TestBuildJobs_OmitsJobsWhoseDependencyIsMissing(t *testing.T) {
	js, err := buildJobs(&infra.Dependencies{}, asynqTestConfig("x"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		jobs.TypeEnrollmentCounts + "@0 * * * *",
		jobs.TypeIdempotencyPrune + "@0 * * * *",
		jobs.TypeOutboxPrune + "@0 * * * *",
	}
	sort.Strings(want)
	if got := jobNames(js); !reflect.DeepEqual(got, want) {
		t.Fatalf("jobs = %v\nwant %v", got, want)
	}
}

func TestBuildJobs_UsesConfiguredIntervals(t *testing.T) {
	cfg := asynqTestConfig("x")
	cfg.Jobs.ReconcilePublishesInterval = 5 * time.Minute
	cfg.Jobs.OutboxPruneInterval = 6 * time.Hour
	js, err := buildJobs(&infra.Dependencies{}, cfg, stubPublishes{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, j := range js {
		got[j.Name] = j.Spec
	}
	if got[jobs.TypeReconcileStalePublishes] != "*/5 * * * *" || got[jobs.TypeOutboxPrune] != "0 */6 * * *" {
		t.Fatalf("specs = %v", got)
	}
}

func TestBuildJobs_RejectsAnUnusableInterval(t *testing.T) {
	cfg := asynqTestConfig("x")
	cfg.Jobs.IdempotencyPruneInterval = 7 * time.Minute
	if _, err := buildJobs(&infra.Dependencies{}, cfg, nil, nil); err == nil {
		t.Fatal("a 7 minute interval has no common cron tick and must be refused")
	}
	cfg = asynqTestConfig("x")
	cfg.Jobs.OutboxRetention = 0
	if _, err := buildJobs(&infra.Dependencies{}, cfg, nil, nil); err == nil {
		t.Fatal("a zero outbox retention must be refused, not delete everything")
	}
}

// The pool sizes of RFC §10.2 reach the servers, one pool per queue, and every
// job's task type has a handler on the low queue.
func TestBuildAsynq_PoolsPerQueueAndJobHandlers(t *testing.T) {
	cfg := asynqTestConfig("127.0.0.1:1")
	cfg.Asynq.CriticalConcurrency, cfg.Asynq.DefaultConcurrency, cfg.Asynq.LowConcurrency = 7, 4, 2
	js, err := buildJobs(&infra.Dependencies{}, cfg, stubPublishes{}, stubRunner{})
	if err != nil {
		t.Fatal(err)
	}
	servers, scheduler, err := buildAsynq(cfg, nil, js)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Shutdown()

	if got := servers.Queues(); !reflect.DeepEqual(got, []string{asynqx.QueueCritical, asynqx.QueueDefault, asynqx.QueueLow}) {
		t.Fatalf("queues = %v", got)
	}
	by := field(reflect.ValueOf(servers), "byQueue")
	for q, want := range map[string]int64{asynqx.QueueCritical: 7, asynqx.QueueDefault: 4, asynqx.QueueLow: 2} {
		srv := by.MapIndex(reflect.ValueOf(q))
		if got := field(srv, "cfg", "Concurrency").Int(); got != want {
			t.Errorf("pool %s = %d, want %d", q, got, want)
		}
		if got := field(srv, "cfg", "ShutdownTimeout").Int(); got != int64(cfg.Asynq.ShutdownTimeout) {
			t.Errorf("pool %s shutdown timeout = %v", q, time.Duration(got))
		}
	}
	types := field(reflect.ValueOf(servers), "types")
	if types.Len() != len(js) {
		t.Fatalf("%d task types registered, want %d (one per job)", types.Len(), len(js))
	}
	for _, j := range js {
		if q := types.MapIndex(reflect.ValueOf(j.Name)); !q.IsValid() || q.String() != asynqx.QueueLow {
			t.Errorf("job %s has no handler on the low queue", j.Name)
		}
	}
}

func TestBuildAsynq_RejectsABrokenPoolConfig(t *testing.T) {
	cfg := asynqTestConfig("127.0.0.1:1")
	cfg.Asynq.LowConcurrency = 0
	if _, _, err := buildAsynq(cfg, nil, nil); err == nil {
		t.Fatal("a zero-size pool must be refused")
	}
}
