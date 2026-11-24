package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

func okWorkflow(ctx workflow.Context) error { return nil }

func failingWorkflow(ctx workflow.Context) error {
	return temporal.NewNonRetryableApplicationError("boom", "Boom", errors.New("boom"))
}

func runWorkflow(t *testing.T, wf any, ints ...worker.Options) error {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	if len(ints) > 0 {
		env.SetWorkerOptions(ints[0])
	}
	env.ExecuteWorkflow(wf)
	return env.GetWorkflowError()
}

func newMetrics(t *testing.T) *telemetry.Metrics {
	t.Helper()
	m, err := telemetry.NewMetrics(telemetry.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func workerOpts(t *testing.T, o Observability) worker.Options {
	t.Helper()
	Observe(o)
	t.Cleanup(func() { Observe(Observability{}) })
	ints, err := interceptors()
	if err != nil {
		t.Fatal(err)
	}
	// A client's interceptors that also implement the worker side apply to
	// workers built on it; the test environment takes them as worker options.
	var opts worker.Options
	for _, i := range ints {
		wi, ok := i.(interceptor.WorkerInterceptor)
		if !ok {
			t.Fatalf("%T is not a worker interceptor", i)
		}
		opts.Interceptors = append(opts.Interceptors, wi)
	}
	return opts
}

func TestWorkflowMetrics_CountStartedCompletedFailed(t *testing.T) {
	m := newMetrics(t)
	opts := workerOpts(t, Observability{Metrics: m})

	if err := runWorkflow(t, okWorkflow, opts); err != nil {
		t.Fatalf("ok workflow: %v", err)
	}
	if err := runWorkflow(t, failingWorkflow, opts); err == nil {
		t.Fatal("failing workflow returned no error")
	}
	if err := runWorkflow(t, okWorkflow, opts); err != nil {
		t.Fatalf("ok workflow: %v", err)
	}

	for name, want := range map[string]float64{"started": 3, "completed": 2, "failed": 1} {
		var got float64
		switch name {
		case "started":
			got = testutil.ToFloat64(m.WorkflowStartedTotal)
		case "completed":
			got = testutil.ToFloat64(m.WorkflowCompletedTotal)
		case "failed":
			got = testutil.ToFloat64(m.WorkflowFailedTotal)
		}
		if got != want {
			t.Errorf("workflow_%s_total = %v, want %v", name, got, want)
		}
	}
}

func TestObserve_ZeroValueAddsNoInterceptors(t *testing.T) {
	Observe(Observability{})
	ints, err := interceptors()
	if err != nil || len(ints) != 0 {
		t.Fatalf("interceptors = %d, %v; want none (disabled means zero overhead)", len(ints), err)
	}
}

func TestTracing_WorkflowRunProducesSpans(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp, err := telemetry.SetupWithExporter(telemetry.Config{Enabled: true, ServiceName: "test", SampleRatio: 1}, exp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	opts := workerOpts(t, Observability{Tracing: true})
	if err := runWorkflow(t, okWorkflow, opts); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range exp.GetSpans() {
		names = append(names, s.Name)
	}
	if !strings.Contains(strings.Join(names, ","), "RunWorkflow") {
		t.Errorf("no RunWorkflow span among %v", names)
	}
}
