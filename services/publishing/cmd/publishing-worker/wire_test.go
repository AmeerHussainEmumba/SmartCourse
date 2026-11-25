// What this checks: the publishing-worker's wiring, through runtime.Run with the
// binary's own options. The process needs no database setting, serves its
// internal listener (health, metrics), registers PublishCourseWorkflow under the
// contract name on the workflow queue, retries until Temporal is reachable, and
// shows an unreachable Temporal as degraded readiness while staying up.
package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
	runtime "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

// fakeClient is a Temporal client whose only working calls are the two the
// wiring makes.
type fakeClient struct {
	temporalclient.Client
	healthy *atomic.Bool
	closed  *atomic.Int32
}

func (f fakeClient) CheckHealth(context.Context, *temporalclient.CheckHealthRequest) (*temporalclient.CheckHealthResponse, error) {
	if !f.healthy.Load() {
		return nil, net.ErrClosed
	}
	return &temporalclient.CheckHealthResponse{}, nil
}
func (f fakeClient) Close() { f.closed.Add(1) }

type fakeWorker struct {
	queue      string
	mu         *sync.Mutex
	registered map[string]string // workflow name -> queue
	failStart  *atomic.Bool
	started    *atomic.Int32
	stopped    *atomic.Int32
}

func (w fakeWorker) RegisterWorkflowWithOptions(_ any, o workflow.RegisterOptions) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.registered[o.Name] = w.queue
}

func (w fakeWorker) Start() error {
	if w.failStart.Load() {
		return startFailure{}
	}
	w.started.Add(1)
	return nil
}
func (w fakeWorker) Stop() { w.stopped.Add(1) }

type fakes struct {
	b          backend
	mu         sync.Mutex
	registered map[string]string
	failStart  atomic.Bool
	started    atomic.Int32
	stopped    atomic.Int32
	closed     atomic.Int32
	healthy    atomic.Bool
	connects   atomic.Int32
}

type startFailure struct{}

func (startFailure) Error() string { return "temporal unreachable" }

func newFakes(failStart bool) *fakes {
	f := &fakes{registered: map[string]string{}}
	f.healthy.Store(true)
	f.failStart.Store(failStart)
	f.b = backend{
		connect: func(*config.Config) (temporalclient.Client, error) {
			f.connects.Add(1)
			return fakeClient{healthy: &f.healthy, closed: &f.closed}, nil
		},
		newWorker: func(_ temporalclient.Client, queue string) worker {
			return fakeWorker{queue: queue, mu: &f.mu, registered: f.registered, failStart: &f.failStart, started: &f.started, stopped: &f.stopped}
		},
	}
	return f
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// start runs the binary's wiring under runtime.Run with its own options and
// returns the internal listener's address and a stop function that returns Run's
// error.
func start(t *testing.T, b backend) (addr string, stop func() error) {
	t.Helper()
	addr = freeAddr(t)
	// Everything required by the old config.Load is deliberately unset.
	for _, k := range []string{"POSTGRES_DSN", "POSTGRES_SEARCH_PATH", "REDIS_ADDR", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY_ID", "MINIO_SECRET_ACCESS_KEY", "JWT_SIGNING_KEY"} {
		t.Setenv(k, "")
	}
	t.Setenv("METRICS_HTTP_ADDR", addr)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	prev := initialBackoff
	initialBackoff = 20 * time.Millisecond
	t.Cleanup(func() { initialBackoff = prev })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, "publishing-worker", func(p *runtime.Process) error { return wireWith(p, b) }, options()...)
	}()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				t.Error("runtime.Run did not return after cancel")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return addr, stop
}

func get(t *testing.T, addr, path string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func readyBody(t *testing.T, addr string) (int, string) {
	t.Helper()
	return get(t, addr, "/health/ready")
}

func TestWire_RegistersTheWorkflowUnderTheContractNameOnTheWorkflowQueue(t *testing.T) {
	f := newFakes(false)
	t.Setenv("TEMPORAL_TASK_QUEUE", "")
	addr, _ := start(t, f.b)
	eventually(t, "worker start", func() bool { return f.started.Load() == 1 })

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.registered) != 1 {
		t.Fatalf("registered workflows = %v, want exactly %s", f.registered, publishing.WorkflowName)
	}
	if q, ok := f.registered[publishing.WorkflowName]; !ok || q != publishing.WorkflowQueue {
		t.Fatalf("workflow %q registered on %q (all: %v), want queue %q", publishing.WorkflowName, q, f.registered, publishing.WorkflowQueue)
	}
	if code, _ := get(t, addr, "/health/live"); code != http.StatusOK {
		t.Errorf("/health/live = %d", code)
	}
}

func TestWire_WorkflowQueueFollowsTheConfiguration(t *testing.T) {
	f := newFakes(false)
	t.Setenv("TEMPORAL_TASK_QUEUE", "my-workflows")
	start(t, f.b)
	eventually(t, "worker start", func() bool { return f.started.Load() == 1 })
	f.mu.Lock()
	defer f.mu.Unlock()
	if q := f.registered[publishing.WorkflowName]; q != "my-workflows" {
		t.Fatalf("queue = %q, want my-workflows", q)
	}
}

func TestWire_ReadyWhenTemporalIsReachable_DegradedWhenItIsNot(t *testing.T) {
	f := newFakes(false)
	addr, _ := start(t, f.b)
	eventually(t, "ready", func() bool {
		code, body := readyBody(t, addr)
		return code == 200 && strings.Contains(body, "ready") && !strings.Contains(body, "degraded")
	})

	f.healthy.Store(false)
	code, body := readyBody(t, addr)
	if code != http.StatusOK || !strings.Contains(body, "degraded") || !strings.Contains(body, "temporal") {
		t.Fatalf("temporal down: ready = %d %s, want 200 degraded naming temporal", code, body)
	}
	if code, _ := get(t, addr, "/health/live"); code != http.StatusOK {
		t.Errorf("/health/live while Temporal is down = %d, want 200", code)
	}
}

// A Temporal outage at start does not end the process: it shows as degraded,
// the worker retries with a fresh client each time, and recovers by itself.
func TestWire_TemporalOutageAtStart_DegradedAndRetriesUntilConnected(t *testing.T) {
	f := newFakes(true)
	addr, stop := start(t, f.b)

	eventually(t, "three connection attempts", func() bool { return f.connects.Load() >= 3 })
	code, body := readyBody(t, addr)
	if code != http.StatusOK || !strings.Contains(body, "degraded") {
		t.Fatalf("outage: ready = %d %s, want 200 degraded", code, body)
	}
	if c := get2(t, addr, "/health/live"); c != http.StatusOK {
		t.Fatalf("/health/live during the outage = %d", c)
	}
	if f.closed.Load() < 2 {
		t.Errorf("failed attempts closed %d clients, want each failed client closed", f.closed.Load())
	}

	f.failStart.Store(false) // Temporal comes up
	eventually(t, "recovery", func() bool { return f.started.Load() == 1 })
	eventually(t, "ready after recovery", func() bool {
		code, body := readyBody(t, addr)
		return code == 200 && !strings.Contains(body, "degraded")
	})

	if err := stop(); err != nil {
		t.Fatalf("clean shutdown returned %v", err)
	}
	if f.stopped.Load() != 1 {
		t.Errorf("worker stopped %d times on shutdown, want 1", f.stopped.Load())
	}
}

func get2(t *testing.T, addr, path string) int { c, _ := get(t, addr, path); return c }

func TestWire_ServesMetricsOnTheInternalListener(t *testing.T) {
	f := newFakes(false)
	addr, _ := start(t, f.b)
	eventually(t, "metrics", func() bool { code, _ := get(t, addr, "/metrics"); return code == http.StatusOK })
}

// The real backend against an address nothing listens on: the process must come
// up, serve, and report degraded, never exit.
func TestWire_RealBackend_UnreachableTemporal_StaysUpDegraded(t *testing.T) {
	t.Setenv("TEMPORAL_HOST_PORT", "127.0.0.1:1")
	addr, stop := start(t, realBackend())
	eventually(t, "serving", func() bool { code, _ := get(t, addr, "/health/live"); return code == http.StatusOK })
	eventually(t, "degraded", func() bool {
		code, body := readyBody(t, addr)
		return code == http.StatusOK && strings.Contains(body, "degraded")
	})
	if err := stop(); err != nil {
		t.Fatalf("shutdown with Temporal unreachable returned %v", err)
	}
}

// The binary's config requires no database, Redis, MinIO or JWT setting.
func TestOptions_LoadWithoutAnyRequiredEnvironment(t *testing.T) {
	for _, k := range []string{"POSTGRES_DSN", "POSTGRES_SEARCH_PATH", "REDIS_ADDR", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY_ID", "MINIO_SECRET_ACCESS_KEY", "JWT_SIGNING_KEY"} {
		t.Setenv(k, "")
	}
	if _, err := config.Load(); err == nil {
		t.Fatal("precondition: config.Load must fail with this environment")
	}
	f := newFakes(false)
	_, stop := start(t, f.b)
	eventually(t, "worker start", func() bool { return f.started.Load() == 1 })
	if err := stop(); err != nil {
		t.Fatalf("run: %v", err)
	}
}
