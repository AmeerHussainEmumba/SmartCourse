//go:build integration

package outbox_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func lastRow(t *testing.T, env *testenv.Env) outbox.Row {
	t.Helper()
	var r outbox.Row
	if err := env.DB.Order("id DESC").First(&r).Error; err != nil {
		t.Fatalf("read outbox row: %v", err)
	}
	return r
}

type userKey struct{}

// useResolver reads the actor from a context value, standing in for the
// libs/auth resolver libs/infra installs.
func useResolver(t *testing.T) {
	t.Helper()
	outbox.SetActorResolver(func(ctx context.Context) (uuid.UUID, bool) {
		id, ok := ctx.Value(userKey{}).(uuid.UUID)
		return id, ok
	})
	t.Cleanup(func() { outbox.SetActorResolver(nil) })
}

func newEvent() outbox.Event {
	return outbox.Event{AggregateType: "x", AggregateID: uuid.New(), EventType: "x.y", SchemaVersion: 1, Payload: map[string]any{}}
}

func TestWrite_FillsTraceAndActorFromContext(t *testing.T) {
	env := testenv.Setup(t)
	useResolver(t)
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	user := uuid.New()
	ctx = context.WithValue(ctx, userKey{}, user)

	if err := outbox.Write(ctx, env.DB, newEvent()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r := lastRow(t, env)
	if r.TraceID == nil || *r.TraceID != tid.String() || r.SpanID == nil || *r.SpanID != sid.String() {
		t.Errorf("trace/span = %v/%v", r.TraceID, r.SpanID)
	}
	if r.ActorID == nil || *r.ActorID != user {
		t.Errorf("actor = %v, want %s", r.ActorID, user)
	}
}

func TestWrite_ExplicitValuesWinAndEmptyContextIsNull(t *testing.T) {
	env := testenv.Setup(t)
	useResolver(t)
	caller := uuid.New()
	ctx := context.WithValue(context.Background(), userKey{}, uuid.New())
	evt := newEvent()
	evt.TraceID, evt.SpanID, evt.ActorID = "11111111111111111111111111111111", "2222222222222222", &caller
	if err := outbox.Write(ctx, env.DB, evt); err != nil {
		t.Fatal(err)
	}
	r := lastRow(t, env)
	if *r.TraceID != evt.TraceID || *r.SpanID != evt.SpanID || *r.ActorID != caller {
		t.Errorf("explicit values were overwritten: %+v", r)
	}

	if err := outbox.Write(context.Background(), env.DB, newEvent()); err != nil {
		t.Fatal(err)
	}
	r = lastRow(t, env)
	if r.TraceID != nil || r.SpanID != nil || r.ActorID != nil {
		t.Errorf("no context must stay NULL: %+v", r)
	}
}
