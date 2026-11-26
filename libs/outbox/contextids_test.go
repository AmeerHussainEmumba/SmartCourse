package outbox

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

const (
	testTraceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanHex  = "00f067aa0ba902b7"
)

type userKey struct{}

// useResolver installs a resolver that reads a uuid stored under userKey and
// removes it when the test ends.
func useResolver(t *testing.T) {
	t.Helper()
	SetActorResolver(func(ctx context.Context) (uuid.UUID, bool) {
		id, ok := ctx.Value(userKey{}).(uuid.UUID)
		return id, ok
	})
	t.Cleanup(func() { SetActorResolver(nil) })
}

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
	useResolver(t)
	user := uuid.New()
	ctx := context.WithValue(ctxWithSpan(t), userKey{}, user)

	traceID, spanID, actor := contextIDs(ctx, Event{})
	if traceID != testTraceHex || spanID != testSpanHex {
		t.Errorf("trace/span = %s/%s, want %s/%s", traceID, spanID, testTraceHex, testSpanHex)
	}
	if actor == nil || *actor != user {
		t.Errorf("actor = %v, want %s", actor, user)
	}
}

func TestContextIDs_CallerValuesWin(t *testing.T) {
	useResolver(t)
	user, caller := uuid.New(), uuid.New()
	ctx := context.WithValue(ctxWithSpan(t), userKey{}, user)

	traceID, spanID, actor := contextIDs(ctx, Event{TraceID: "aaaa", SpanID: "bbbb", ActorID: &caller})
	if traceID != "aaaa" || spanID != "bbbb" {
		t.Errorf("caller trace overwritten: %s/%s", traceID, spanID)
	}
	if actor == nil || *actor != caller {
		t.Errorf("caller actor overwritten: %v", actor)
	}
}

func TestContextIDs_NothingInContextStaysEmpty(t *testing.T) {
	useResolver(t)
	traceID, spanID, actor := contextIDs(context.Background(), Event{})
	if traceID != "" || spanID != "" || actor != nil {
		t.Errorf("got %q %q %v, want all empty", traceID, spanID, actor)
	}
	// A zero user id is not an actor.
	_, _, actor = contextIDs(context.WithValue(context.Background(), userKey{}, uuid.Nil), Event{})
	if actor != nil {
		t.Errorf("zero identity produced actor %v", actor)
	}
}

// With no resolver installed (a binary that does not link libs/infra) the
// actor stays as the caller set it; the trace still comes from the context.
func TestContextIDs_NoResolverLeavesActorAlone(t *testing.T) {
	SetActorResolver(nil)
	ctx := context.WithValue(ctxWithSpan(t), userKey{}, uuid.New())
	traceID, _, actor := contextIDs(ctx, Event{})
	if actor != nil {
		t.Errorf("actor = %v with no resolver", actor)
	}
	if traceID != testTraceHex {
		t.Errorf("trace not taken from the context: %q", traceID)
	}
}
