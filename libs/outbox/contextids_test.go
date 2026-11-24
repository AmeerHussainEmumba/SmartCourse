package outbox

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
)

const (
	testTraceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanHex  = "00f067aa0ba902b7"
)

func ctxWithSpan(t *testing.T) context.Context {
	t.Helper()
	tid, err := trace.TraceIDFromHex(testTraceHex)
	if err != nil {
		t.Fatal(err)
	}
	sid, err := trace.SpanIDFromHex(testSpanHex)
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

func TestContextIDs_FillsFromContext(t *testing.T) {
	user := uuid.New()
	ctx := auth.WithIdentity(ctxWithSpan(t), auth.Identity{UserID: user, Role: "student"})

	traceID, spanID, actor := contextIDs(ctx, Event{})
	if traceID != testTraceHex || spanID != testSpanHex {
		t.Errorf("trace/span = %s/%s, want %s/%s", traceID, spanID, testTraceHex, testSpanHex)
	}
	if actor == nil || *actor != user {
		t.Errorf("actor = %v, want %s", actor, user)
	}
}

func TestContextIDs_CallerValuesWin(t *testing.T) {
	user, caller := uuid.New(), uuid.New()
	ctx := auth.WithIdentity(ctxWithSpan(t), auth.Identity{UserID: user})

	traceID, spanID, actor := contextIDs(ctx, Event{TraceID: "aaaa", SpanID: "bbbb", ActorID: &caller})
	if traceID != "aaaa" || spanID != "bbbb" {
		t.Errorf("caller trace overwritten: %s/%s", traceID, spanID)
	}
	if actor == nil || *actor != caller {
		t.Errorf("caller actor overwritten: %v", actor)
	}
}

func TestContextIDs_NothingInContextStaysEmpty(t *testing.T) {
	traceID, spanID, actor := contextIDs(context.Background(), Event{})
	if traceID != "" || spanID != "" || actor != nil {
		t.Errorf("got %q %q %v, want all empty", traceID, spanID, actor)
	}
	// A zero user id is not an actor.
	_, _, actor = contextIDs(auth.WithIdentity(context.Background(), auth.Identity{}), Event{})
	if actor != nil {
		t.Errorf("zero identity produced actor %v", actor)
	}
}
