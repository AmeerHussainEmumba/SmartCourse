package temporal

import (
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/propagation"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

// Observability selects the interceptors Connect adds to every client. A
// client's interceptors also apply to any worker built on it, so one setting
// covers course-api (client side: the StartWorkflow span, whose context the
// workflow continues) and the worker (workflow and activity spans, and the
// workflow counters).
type Observability struct {
	// Tracing adds the OpenTelemetry tracing interceptor
	// (go.temporal.io/sdk/contrib/opentelemetry, MIT). The tracer comes from
	// the global provider, so enable it only after telemetry.Setup ran and
	// only when tracing is on: left false there is no interceptor and no
	// overhead.
	Tracing bool
	// Metrics, when non-nil, counts workflow_started_total,
	// workflow_completed_total and workflow_failed_total from the worker's
	// workflow executions.
	Metrics *telemetry.Metrics
}

var (
	obsMu sync.RWMutex
	obs   Observability
)

// Observe sets the observability Connect applies to clients built afterwards.
// Call it once at start-up, before Connect; the zero Observability (the
// default) adds nothing. It is process-wide state because Connect is called
// from several places (the worker builds more than one client) and a
// per-call option would have to be threaded through all of them.
func Observe(o Observability) {
	obsMu.Lock()
	defer obsMu.Unlock()
	obs = o
}

// interceptors builds the interceptor list for the current Observability.
func interceptors() ([]interceptor.ClientInterceptor, error) {
	obsMu.RLock()
	o := obs
	obsMu.RUnlock()

	var out []interceptor.ClientInterceptor
	if o.Tracing {
		t, err := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{
			// W3C trace context only: baggage is not carried anywhere else.
			TextMapPropagator: propagation.TraceContext{},
			DisableBaggage:    true,
		})
		if err != nil {
			return nil, fmt.Errorf("build temporal tracing interceptor: %w", err)
		}
		out = append(out, t)
	}
	if o.Metrics != nil {
		out = append(out, &workflowMetrics{m: o.Metrics})
	}
	return out, nil
}

// workflowMetrics counts workflow executions on the worker. It is a client
// interceptor (so it can be passed in client.Options) that also implements
// the worker side.
type workflowMetrics struct {
	interceptor.InterceptorBase
	m *telemetry.Metrics
}

func (w *workflowMetrics) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &workflowCounter{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, m: w.m}
}

type workflowCounter struct {
	interceptor.WorkflowInboundInterceptorBase
	m *telemetry.Metrics
}

// ExecuteWorkflow counts a workflow when it first runs on this worker (not
// when history is replayed, which happens after a cache eviction or a restart
// and would double count) and counts its outcome once, on the live
// completion. A cancelled or terminated-by-error run counts as failed; a
// workflow that continues as new counts as failed too (the publish workflow
// never does).
func (w *workflowCounter) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	if !workflow.IsReplaying(ctx) {
		w.m.WorkflowStartedTotal.Inc()
	}
	out, err := w.Next.ExecuteWorkflow(ctx, in)
	if !workflow.IsReplaying(ctx) {
		if err != nil {
			w.m.WorkflowFailedTotal.Inc()
		} else {
			w.m.WorkflowCompletedTotal.Inc()
		}
	}
	return out, err
}
