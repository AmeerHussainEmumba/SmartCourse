package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

// freeAddr is a loopback address nothing listens on right now.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func testConfig(addr string, metrics bool) func() (*config.Config, error) {
	return func() (*config.Config, error) {
		return &config.Config{Env: "test", Telemetry: config.TelemetryConfig{MetricsAddr: addr, MetricsEnabled: metrics, SampleRatio: 1}}, nil
	}
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// harness runs Run in the background and exposes the internal listener.
type harness struct {
	t      *testing.T
	addr   string
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func start(t *testing.T, wire func(*runtime.Process) error, opts ...runtime.Option) *harness {
	t.Helper()
	h := &harness{t: t, addr: freeAddr(t), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	all := append([]runtime.Option{runtime.WithConfig(testConfig(h.addr, true)), runtime.WithoutDependencies(), runtime.WithLogger(quiet)}, opts...)
	go func() { h.err = runtime.Run(ctx, "test-svc", wire, all...); close(h.done) }()
	t.Cleanup(func() { cancel(); <-h.done })
	return h
}

func (h *harness) wait(d time.Duration) bool {
	select {
	case <-h.done:
		return true
	case <-time.After(d):
		return false
	}
}

func (h *harness) get(path string) (int, string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + h.addr + path)
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return resp.StatusCode, string(b)
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("GET %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *harness) ready() (int, map[string]any) {
	h.t.Helper()
	code, body := h.get("/health/ready")
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		h.t.Fatalf("ready body %q: %v", body, err)
	}
	return code, m
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRun_StartsComponentsAndStopsThemOnCancel(t *testing.T) {
	var running, stopped atomic.Int32
	h := start(t, func(p *runtime.Process) error {
		for _, n := range []string{"a", "b", "c"} {
			p.Go(n, func(ctx context.Context) error {
				running.Add(1)
				<-ctx.Done()
				stopped.Add(1)
				return ctx.Err()
			})
		}
		return nil
	})
	eventually(t, "components running", func() bool { return running.Load() == 3 })
	h.cancel()
	if !h.wait(5 * time.Second) {
		t.Fatal("Run did not return after cancel")
	}
	if h.err != nil {
		t.Fatalf("clean shutdown returned %v", h.err)
	}
	if stopped.Load() != 3 {
		t.Fatalf("%d components stopped, want 3", stopped.Load())
	}
}

// Independence, non-critical: one component fails, another panics; the third
// keeps running and the process stays up and ready (degraded), naming both.
func TestRun_NonCriticalFailureDoesNotStopTheOthers(t *testing.T) {
	var survivor atomic.Bool
	var stopped atomic.Bool
	h := start(t, func(p *runtime.Process) error {
		p.Go("fails", func(context.Context) error { return errors.New("boom: secret-host:5432") })
		p.Go("panics", func(context.Context) error { panic("kaboom") })
		p.Go("survivor", func(ctx context.Context) error {
			survivor.Store(true)
			<-ctx.Done()
			stopped.Store(true)
			return nil
		})
		return nil
	})
	eventually(t, "survivor running", survivor.Load)
	eventually(t, "both failures reported", func() bool {
		_, m := h.ready()
		checks, _ := m["checks"].(map[string]any)
		return checks["component:fails"] == "failed" && checks["component:panics"] == "failed"
	})
	code, m := h.ready()
	if code != http.StatusOK || m["status"] != "degraded" {
		t.Fatalf("readiness = %d %v, want 200 degraded (a non-critical failure is reported, not fatal)", code, m)
	}
	select {
	case <-h.done:
		t.Fatal("a non-critical failure stopped the process")
	case <-time.After(100 * time.Millisecond):
	}
	if stopped.Load() {
		t.Fatal("the survivor was cancelled")
	}
	_, body := h.get("/health/ready")
	if strings.Contains(body, "secret-host") || strings.Contains(body, "kaboom") {
		t.Fatalf("readiness body leaks error text: %s", body)
	}
}

func TestRun_CriticalFailureStopsTheProcess(t *testing.T) {
	for name, fn := range map[string]func(context.Context) error{
		"error":        func(context.Context) error { return errors.New("lost the database") },
		"panic":        func(context.Context) error { panic("nil map") },
		"early return": func(context.Context) error { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			var other atomic.Bool
			h := start(t, func(p *runtime.Process) error {
				p.Go("other", func(ctx context.Context) error { <-ctx.Done(); other.Store(true); return nil })
				p.Go("core", fn, runtime.Critical())
				return nil
			})
			if !h.wait(5 * time.Second) {
				t.Fatal("a critical component's failure did not stop the process")
			}
			if h.err == nil || !strings.Contains(h.err.Error(), "core") {
				t.Fatalf("Run returned %v, want an error naming the critical component", h.err)
			}
			if !other.Load() {
				t.Fatal("the other component was not given the chance to stop")
			}
		})
	}
}

func TestRun_WiringErrorStartsNothing(t *testing.T) {
	var started, hookRan atomic.Bool
	h := start(t, func(p *runtime.Process) error {
		p.Go("never", func(context.Context) error { started.Store(true); return nil })
		p.OnShutdown("cleanup", func(context.Context) error { hookRan.Store(true); return nil })
		return errors.New("cannot reach kafka")
	})
	if !h.wait(5 * time.Second) {
		t.Fatal("Run did not return")
	}
	if h.err == nil || !strings.Contains(h.err.Error(), "cannot reach kafka") {
		t.Fatalf("Run returned %v", h.err)
	}
	if started.Load() {
		t.Fatal("a component started although wiring failed")
	}
	if !hookRan.Load() {
		t.Fatal("shutdown hooks did not run after a wiring failure (a half-built producer would leak)")
	}
}

func TestRun_ConfigurationErrorIsReturned(t *testing.T) {
	err := runtime.Run(context.Background(), "x", func(*runtime.Process) error { return nil },
		runtime.WithConfig(func() (*config.Config, error) { return nil, errors.New("POSTGRES_DSN is required") }))
	if err == nil || !strings.Contains(err.Error(), "POSTGRES_DSN is required") {
		t.Fatalf("got %v", err)
	}
}

// A component that ignores cancellation cannot hold the process past the
// bounded shutdown.
func TestRun_ShutdownIsBounded(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	h := start(t, func(p *runtime.Process) error {
		p.Go("stuck", func(context.Context) error { <-release; return nil })
		return nil
	}, runtime.WithShutdownTimeout(150*time.Millisecond))
	_, _ = h.ready() // up
	h.cancel()
	begin := time.Now()
	if !h.wait(5 * time.Second) {
		t.Fatal("Run never returned")
	}
	if h.err == nil || !strings.Contains(h.err.Error(), "shutdown did not finish") {
		t.Fatalf("Run returned %v, want a shutdown timeout error", h.err)
	}
	if time.Since(begin) > 3*time.Second {
		t.Fatalf("shutdown took %s with a 150ms bound", time.Since(begin))
	}
}

func TestRun_ShutdownHooksRunLastToFirstAfterComponentsStop(t *testing.T) {
	var mu sync.Mutex
	var order []string
	h := start(t, func(p *runtime.Process) error {
		p.Go("c", func(ctx context.Context) error {
			<-ctx.Done()
			mu.Lock()
			order = append(order, "component")
			mu.Unlock()
			return nil
		})
		for _, n := range []string{"first", "second"} {
			p.OnShutdown(n, func(context.Context) error {
				mu.Lock()
				order = append(order, n)
				mu.Unlock()
				if n == "second" {
					panic("a hook panics") // must not stop the next hook
				}
				return nil
			})
		}
		return nil
	})
	_, _ = h.ready()
	h.cancel()
	if !h.wait(5*time.Second) || h.err != nil {
		t.Fatalf("Run: %v", h.err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(order, ",") != "component,second,first" {
		t.Fatalf("order = %v, want the component, then hooks last-registered-first", order)
	}
}

func TestInternalListener_ServesMetricsAndProbesAndNothingElse(t *testing.T) {
	h := start(t, func(p *runtime.Process) error {
		p.Go("idle", func(ctx context.Context) error { <-ctx.Done(); return nil })
		return nil
	})
	for path, want := range map[string]int{"/health/live": 200, "/metrics": 200, "/": 404, "/admin": 404, "/debug/pprof/": 404, "/api/v1/admin/failed-events": 404} {
		if code, _ := h.get(path); code != want {
			t.Errorf("GET %s = %d, want %d", path, code, want)
		}
	}
	if _, body := h.get("/metrics"); !strings.Contains(body, "go_goroutines") {
		t.Errorf("/metrics has no Go runtime metrics: %.200s", body)
	}
	resp, err := http.Post("http://"+h.addr+"/metrics", "text/plain", nil)
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Error("POST /metrics succeeded")
		}
	}
}

func TestInternalListener_NoMetricsRouteWhenMetricsAreOff(t *testing.T) {
	h := &harness{t: t, addr: freeAddr(t), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		h.err = runtime.Run(ctx, "x", func(p *runtime.Process) error { return nil },
			runtime.WithConfig(testConfig(h.addr, false)), runtime.WithoutDependencies(), runtime.WithLogger(quiet))
		close(h.done)
	}()
	t.Cleanup(func() { cancel(); <-h.done })
	if code, _ := h.get("/metrics"); code != 404 {
		t.Errorf("/metrics with METRICS_ENABLED=false = %d, want 404", code)
	}
	if code, _ := h.get("/health/live"); code != 200 {
		t.Errorf("/health/live = %d, want 200 (probes do not depend on metrics)", code)
	}
}

func TestReadiness_FatalNonFatalAndShutdown(t *testing.T) {
	var db, kafka atomic.Bool // true = failing
	release := make(chan struct{})
	var once sync.Once
	closeRelease := func() { once.Do(func() { close(release) }) }
	defer closeRelease() // a failed assertion must not leave the component blocking the cleanup
	h := start(t, func(p *runtime.Process) error {
		p.Ready("postgres", func(context.Context) error {
			if db.Load() {
				return errors.New("dial tcp 10.0.0.5:5432: refused")
			}
			return nil
		})
		p.Ready("kafka", func(context.Context) error {
			if kafka.Load() {
				return errors.New("broker down")
			}
			return nil
		}, runtime.NonFatal())
		p.Go("slow-stop", func(ctx context.Context) error { <-ctx.Done(); <-release; return nil })
		return nil
	})

	code, m := h.ready()
	if code != 200 || m["status"] != "ready" {
		t.Fatalf("healthy: %d %v", code, m)
	}

	kafka.Store(true) // an outage the outbox buffers: degraded, still ready
	code, m = h.ready()
	if code != 200 || m["status"] != "degraded" || m["checks"].(map[string]any)["kafka"] != "failed" {
		t.Fatalf("non-fatal failure: %d %v, want 200 degraded", code, m)
	}

	db.Store(true)
	code, m = h.ready()
	if code != http.StatusServiceUnavailable || m["status"] != "not_ready" {
		t.Fatalf("fatal failure: %d %v, want 503 not_ready", code, m)
	}
	_, body := h.get("/health/ready")
	if strings.Contains(body, "10.0.0.5") {
		t.Fatalf("body leaks the driver error: %s", body)
	}
	if code, _ := h.get("/health/live"); code != 200 {
		t.Fatalf("liveness followed a dependency failure: %d (it would restart the pod for an outage)", code)
	}

	db.Store(false)
	kafka.Store(false)
	h.cancel() // the slow component keeps the process draining
	eventually(t, "readiness to report shutting_down", func() bool {
		c, m := h.ready()
		return c == http.StatusServiceUnavailable && m["status"] == "shutting_down"
	})
	if code, _ := h.get("/health/live"); code != 200 {
		t.Fatalf("liveness during drain = %d", code)
	}
	closeRelease()
	if !h.wait(5 * time.Second) {
		t.Fatal("Run did not finish")
	}
}

func TestReadiness_StartingUntilComponentsAreLaunched(t *testing.T) {
	// A wiring function that takes a while: the internal listener is already up
	// (liveness), readiness says "starting".
	gate := make(chan struct{})
	h := start(t, func(p *runtime.Process) error { <-gate; return nil })
	if code, _ := h.get("/health/live"); code != 200 {
		t.Fatalf("liveness during start-up = %d", code)
	}
	code, m := h.ready()
	if code != http.StatusServiceUnavailable || m["status"] != "starting" {
		t.Fatalf("during wiring: %d %v, want 503 starting", code, m)
	}
	close(gate)
	eventually(t, "ready", func() bool { c, _ := h.ready(); return c == 200 })
}

// A port that is already taken must not stop the process: telemetry never does.
func TestInternalListener_BindFailureDoesNotStopTheProcess(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	var ran atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, "x", func(p *runtime.Process) error {
			p.Go("work", func(ctx context.Context) error { ran.Store(true); <-ctx.Done(); return nil })
			return nil
		}, runtime.WithConfig(testConfig(l.Addr().String(), true)), runtime.WithoutDependencies(), runtime.WithLogger(quiet))
	}()
	eventually(t, "component running despite the failed bind", ran.Load)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
