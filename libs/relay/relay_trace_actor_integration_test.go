//go:build integration

// What this file checks, against real Postgres (RFC §6.4, §12.2): that the
// trace_id/span_id/actor_id fields a writer supplies to outbox.Write are
// persisted, that a writer which supplies none (every pre-existing caller)
// leaves them NULL, that the relay hands them to handlers, and that every
// per-service outbox carries the columns and the unpublished-row index.
package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

func writeOne(t *testing.T, db *gorm.DB, evt outbox.Event) {
	t.Helper()
	if err := db.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(context.Background(), tx, evt)
	}); err != nil {
		t.Fatalf("outbox.Write: %v", err)
	}
}

type traceCols struct {
	TraceID *string
	SpanID  *string
	ActorID *uuid.UUID
}

func readTraceCols(t *testing.T, db *gorm.DB, eventType string) traceCols {
	t.Helper()
	var c traceCols
	if err := db.Raw(`SELECT trace_id, span_id, actor_id FROM outbox WHERE event_type = ?`, eventType).Scan(&c).Error; err != nil {
		t.Fatalf("read trace columns: %v", err)
	}
	return c
}

func TestOutbox_TraceAndActor_RoundTrip(t *testing.T) {
	env := setupCourse(t)
	actor := uuid.New()
	writeOne(t, env.DB, outbox.Event{
		AggregateType: "test", AggregateID: uuid.New(), EventType: "test.traced", SchemaVersion: 1,
		Payload: map[string]any{}, TraceID: testTraceID, SpanID: testSpanID, ActorID: &actor,
	})

	c := readTraceCols(t, env.DB, "test.traced")
	if c.TraceID == nil || *c.TraceID != testTraceID {
		t.Errorf("trace_id = %v, want %q", c.TraceID, testTraceID)
	}
	if c.SpanID == nil || *c.SpanID != testSpanID {
		t.Errorf("span_id = %v, want %q", c.SpanID, testSpanID)
	}
	if c.ActorID == nil || *c.ActorID != actor {
		t.Errorf("actor_id = %v, want %s", c.ActorID, actor)
	}
}

func TestOutbox_ExistingCallerPath_LeavesTraceAndActorNull(t *testing.T) {
	env := setupCourse(t)
	writeOne(t, env.DB, outbox.Event{
		AggregateType: "test", AggregateID: uuid.New(), EventType: "test.plain", SchemaVersion: 1,
		Payload: map[string]any{"x": 1},
	})

	c := readTraceCols(t, env.DB, "test.plain")
	if c.TraceID != nil || c.SpanID != nil || c.ActorID != nil {
		t.Errorf("expected NULL trace_id/span_id/actor_id, got %v %v %v", c.TraceID, c.SpanID, c.ActorID)
	}
}

func TestRelay_HandsTraceAndActorToHandler(t *testing.T) {
	env := setupCourse(t)
	actor := uuid.New()
	writeOne(t, env.DB, outbox.Event{
		AggregateType: "test", AggregateID: uuid.New(), EventType: "test.relay.traced", SchemaVersion: 1,
		Payload: map[string]any{}, TraceID: testTraceID, SpanID: testSpanID, ActorID: &actor,
	})
	writeOne(t, env.DB, outbox.Event{
		AggregateType: "test", AggregateID: uuid.New(), EventType: "test.relay.plain", SchemaVersion: 1,
		Payload: map[string]any{},
	})

	got := make(chan relay.DecodedEvent, 2)
	h := func(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
		got <- evt
		return nil
	}
	rl := relay.New(env.DB, relay.WithPollInterval(20*time.Millisecond))
	rl.Register(relay.NewHandler("trace-test", []string{"test.relay.traced", "test.relay.plain"}, h))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { rl.Run(ctx); close(done) }()

	byType := map[string]relay.DecodedEvent{}
	for len(byType) < 2 {
		select {
		case e := <-got:
			byType[e.EventType] = e
		case <-ctx.Done():
			t.Fatalf("timed out; received %d of 2 events", len(byType))
		}
	}
	cancel()
	<-done

	traced := byType["test.relay.traced"]
	if traced.TraceID != testTraceID || traced.SpanID != testSpanID {
		t.Errorf("traced event trace/span = %q/%q, want %q/%q", traced.TraceID, traced.SpanID, testTraceID, testSpanID)
	}
	if traced.ActorID == nil || *traced.ActorID != actor {
		t.Errorf("traced event actor = %v, want %s", traced.ActorID, actor)
	}
	plain := byType["test.relay.plain"]
	if plain.TraceID != "" || plain.SpanID != "" || plain.ActorID != nil {
		t.Errorf("plain event should carry no trace/actor, got %q %q %v", plain.TraceID, plain.SpanID, plain.ActorID)
	}
}

// TestServiceOutboxesCarryTraceActorColumnsAndIndex checks that each per-service
// outbox has the trace/actor columns and the partial index the relay's poll
// query relies on (the platform migrations that added them are history now;
// migrations/cutover removes the platform tables).
func TestServiceOutboxesCarryTraceActorColumnsAndIndex(t *testing.T) {
	env := testenv.Setup(t)
	for _, schema := range []string{"identity", "course"} {
		var names []string
		if err := env.DB.Raw(`SELECT column_name FROM information_schema.columns
			WHERE table_schema = ? AND table_name = 'outbox'`, schema).Scan(&names).Error; err != nil {
			t.Fatalf("list %s.outbox columns: %v", schema, err)
		}
		have := map[string]bool{}
		for _, n := range names {
			have[n] = true
		}
		for _, c := range []string{"event_id", "payload", "trace_id", "span_id", "actor_id"} {
			if !have[c] {
				t.Errorf("%s.outbox lacks column %s", schema, c)
			}
		}
		var idx int64
		env.DB.Raw(`SELECT count(*) FROM pg_indexes WHERE schemaname = ? AND indexname = 'idx_outbox_unpublished'`, schema).Scan(&idx)
		if idx != 1 {
			t.Errorf("%s.outbox lacks idx_outbox_unpublished", schema)
		}
	}
}
