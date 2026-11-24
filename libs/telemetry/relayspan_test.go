package telemetry

import (
	"context"
	"testing"
)

func TestTraceparentFromIDs(t *testing.T) {
	const tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	want := "00-" + tid + "-" + sid + "-01"
	if got := TraceparentFromIDs(tid, sid); got != want {
		t.Fatalf("TraceparentFromIDs = %q, want %q", got, want)
	}
	for name, ids := range map[string][2]string{
		"empty":       {"", ""},
		"short trace": {tid[:30], sid},
		"short span":  {tid, sid[:14]},
		"not hex":     {"zz" + tid[2:], sid},
		"zero trace":  {"00000000000000000000000000000000", sid},
		"zero span":   {tid, "0000000000000000"},
		"upper case":  {"4BF92F3577B34DA6A3CE929D0E0E4736", sid},
	} {
		if got := TraceparentFromIDs(ids[0], ids[1]); got != "" {
			t.Errorf("%s: got %q, want empty", name, got)
		}
	}
}

// The relay span is a child of the span whose ids the row stored, in the same
// trace: that is how a row written under an HTTP request continues its trace.
func TestStartRelayContinuesFromRowIDs(t *testing.T) {
	p, exp := newTestProvider(t, 1)
	const tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	ctx, span := StartRelay(context.Background(), TraceparentFromIDs(tid, sid), "publish", "course.published", "evt-1")
	span.End()
	if got, _ := TraceIDsFromContext(ctx); got != tid {
		t.Fatalf("relay trace id = %q, want %q", got, tid)
	}
	flush(t, p)
	s := spanByName(t, exp.GetSpans(), "outbox relay publish")
	if s.Parent.SpanID().String() != sid {
		t.Errorf("relay span parent = %s, want the stored span id %s", s.Parent.SpanID(), sid)
	}
	// A row with no ids starts a fresh trace instead of failing.
	ctx2, span2 := StartRelay(context.Background(), TraceparentFromIDs("", ""), "process", "x", "y")
	span2.End()
	if got, _ := TraceIDsFromContext(ctx2); got == "" || got == tid {
		t.Errorf("a row without ids must start its own trace, got %q", got)
	}
}
