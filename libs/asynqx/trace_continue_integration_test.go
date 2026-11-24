//go:build integration

// The trace crosses the queue: an enqueue made under a trace carries its
// traceparent in the task headers, the job span is the enqueue span's child in
// the same trace, the payload and the deterministic task ID are untouched, and
// an enqueue with no trace (the scheduler, an old caller) still runs and starts
// its own trace.
package asynqx_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestJobContinuesTheEnqueuersTrace(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)
	exp := tracetest.NewInMemoryExporter()
	tp, err := telemetry.SetupWithExporter(telemetry.Config{Enabled: true, ServiceName: "test", SampleRatio: 1}, exp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ss := newServers(t, asynqx.ServersConfig{Redis: redis, Pools: map[string]int{asynqx.QueueDefault: 2}})
	got := make(chan string, 4)
	mustRegister(t, ss, "traced", asynqx.QueueDefault, func(_ context.Context, payload []byte) error {
		got <- string(payload)
		return nil
	})
	runServers(t, ss)
	c := newClient(t, redis)

	// An enqueue under a trace, as the notification sweeper does it: the context
	// is continued from a stored traceparent.
	const tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	ctx := telemetry.ContinueFromEvent(context.Background(), telemetry.TraceparentFromIDs(tid, sid))
	job := asynqx.Job{Type: "traced", Payload: []byte(`{"n":1}`), ID: "traced-1", Queue: asynqx.QueueDefault}
	if _, err := c.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	// And one with no trace in its context, as the scheduler's enqueues are.
	if _, err := c.Enqueue(context.Background(), asynqx.Job{Type: "traced", Payload: []byte(`{"n":2}`), ID: "plain-1", Queue: asynqx.QueueDefault}); err != nil {
		t.Fatal(err)
	}
	payloads := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case p := <-got:
			payloads[p] = true
		case <-time.After(10 * time.Second):
			t.Fatalf("task %d did not run", i+1)
		}
	}
	if !payloads[`{"n":1}`] || !payloads[`{"n":2}`] {
		t.Errorf("payloads changed on the way: %v", payloads)
	}
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}

	byJob := map[string]tracetest.SpanStub{}
	var enqueues []tracetest.SpanStub
	for _, s := range exp.GetSpans() {
		switch s.Name {
		case "traced process":
			for _, a := range s.Attributes {
				if a.Key == "smartcourse.job.id" {
					byJob[a.Value.AsString()] = s
				}
			}
		case "traced enqueue":
			enqueues = append(enqueues, s)
		}
	}
	if len(enqueues) != 1 {
		t.Fatalf("want exactly one enqueue span (only the traced enqueue gets one), got %d", len(enqueues))
	}
	enq := enqueues[0]
	if enq.SpanContext.TraceID().String() != tid || enq.Parent.SpanID().String() != sid {
		t.Errorf("enqueue span trace=%s parent=%s, want the continued trace %s under %s", enq.SpanContext.TraceID(), enq.Parent.SpanID(), tid, sid)
	}
	traced, ok := byJob["traced-1"]
	if !ok {
		t.Fatalf("no job span for traced-1: %v", byJob)
	}
	if traced.SpanContext.TraceID().String() != tid {
		t.Errorf("job span trace = %s, want the enqueuer's trace %s", traced.SpanContext.TraceID(), tid)
	}
	if traced.Parent.SpanID() != enq.SpanContext.SpanID() {
		t.Errorf("job span parent = %s, want the enqueue span %s", traced.Parent.SpanID(), enq.SpanContext.SpanID())
	}
	plain, ok := byJob["plain-1"]
	if !ok {
		t.Fatalf("no job span for plain-1: %v", byJob)
	}
	if plain.SpanContext.TraceID().String() == tid || plain.Parent.IsValid() {
		t.Errorf("an enqueue with no trace must start its own trace, got trace %s parent valid=%v", plain.SpanContext.TraceID(), plain.Parent.IsValid())
	}
}

// The task ID is a function of the payload only: tracing must not change it,
// or an enqueue made under a trace would stop deduplicating against one made
// without.
func TestTracedEnqueueDeduplicatesAgainstUntraced(t *testing.T) {
	env := testenv.Setup(t)
	redis := asynqx.RedisClient(env.RDB.Options().Addr, "", 0)
	exp := tracetest.NewInMemoryExporter()
	tp, err := telemetry.SetupWithExporter(telemetry.Config{Enabled: true, ServiceName: "test", SampleRatio: 1}, exp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	c := newClient(t, redis)

	id := asynqx.DeterministicID("traced", `{"n":1}`)
	job := asynqx.Job{Type: "traced", Payload: []byte(`{"n":1}`), ID: id, Queue: asynqx.QueueDefault}
	if res := mustEnqueue(t, c, job); res.Duplicate {
		t.Fatal("first enqueue reported a duplicate")
	}
	ctx := telemetry.ContinueFromEvent(context.Background(), telemetry.TraceparentFromIDs("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"))
	res, err := c.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Error("a traced enqueue of the same job must be a duplicate of the untraced one")
	}
}
