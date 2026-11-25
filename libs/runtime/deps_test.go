package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra/observe"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

// fakeDeps is a Dependencies with no database: it records what the runtime asked.
type fakeDeps struct {
	observed atomic.Bool
	closed   atomic.Bool
	checkErr error
}

func (f *fakeDeps) Observe(ctx context.Context, cfg *config.Config, o observe.Options) *observe.Observability {
	f.observed.Store(true)
	return observe.New(ctx, cfg, o)
}

func (f *fakeDeps) ReadinessChecks() map[string]func(context.Context) error {
	return map[string]func(context.Context) error{"fake": func(context.Context) error { return f.checkErr }}
}

func (f *fakeDeps) Close() { f.closed.Store(true) }

// WithDependencies connects through the binary's own constructor: the wiring
// function sees the value, its checks appear on /health/ready, its Observe builds
// the telemetry, and it is closed when the process stops.
func TestWithDependencies_ConnectsObservesChecksAndCloses(t *testing.T) {
	f := &fakeDeps{}
	var saw runtime.Dependencies
	h := start(t, func(p *runtime.Process) error {
		saw = p.Deps
		return nil
	}, runtime.WithDependencies(func(*config.Config) (*fakeDeps, error) { return f, nil }))

	eventually(t, "ready with the fake dependency", func() bool {
		code, m := h.ready()
		checks, _ := m["checks"].(map[string]any)
		return code == 200 && checks["fake"] == "ok"
	})
	if saw != runtime.Dependencies(f) {
		t.Errorf("Process.Deps = %v, want the constructor's value", saw)
	}
	if !f.observed.Load() {
		t.Error("the dependencies' Observe was not used to build telemetry")
	}
	if f.closed.Load() {
		t.Error("dependencies closed while the process was running")
	}
	h.cancel()
	if !h.wait(5*time.Second) || h.err != nil {
		t.Fatalf("Run: %v", h.err)
	}
	if !f.closed.Load() {
		t.Error("dependencies not closed at shutdown")
	}
}

// A failing readiness check of a dependency makes /health/ready answer 503.
func TestWithDependencies_FailingCheckIsReportedNotReady(t *testing.T) {
	f := &fakeDeps{checkErr: errors.New("down")}
	h := start(t, func(*runtime.Process) error { return nil },
		runtime.WithDependencies(func(*config.Config) (*fakeDeps, error) { return f, nil }))
	eventually(t, "503 with the dependency down", func() bool {
		code, m := h.ready()
		checks, _ := m["checks"].(map[string]any)
		return code == 503 && checks["fake"] == "failed"
	})
}

// A constructor error stops start-up before the wiring function runs.
func TestWithDependencies_ConstructorErrorStopsStartUp(t *testing.T) {
	wired := false
	err := runtime.Run(context.Background(), "x", func(*runtime.Process) error { wired = true; return nil },
		runtime.WithConfig(testConfig(freeAddr(t), false)), runtime.WithLogger(quiet),
		runtime.WithDependencies(func(*config.Config) (*fakeDeps, error) { return nil, errors.New("no postgres") }))
	if err == nil || wired {
		t.Fatalf("err = %v, wired = %v; want a start-up error and no wiring", err, wired)
	}
}

// Without the option there are no dependencies: Process.Deps is nil, Obs is still
// built and only the process's own checks exist.
func TestWithoutWithDependencies_NoDeps(t *testing.T) {
	var depsNil, obsOK bool
	h := start(t, func(p *runtime.Process) error {
		depsNil, obsOK = p.Deps == nil, p.Obs != nil
		return nil
	})
	eventually(t, "ready", func() bool { code, _ := h.ready(); return code == 200 })
	if !depsNil || !obsOK {
		t.Errorf("Deps nil = %v, Obs non-nil = %v; want true, true", depsNil, obsOK)
	}
}
