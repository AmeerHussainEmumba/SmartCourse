//go:build integration

// Phase 4 additions: audit records that are idempotent per source event, the
// replay metadata kept with a parked event, and the failed-event summary.
package mongostore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func TestAppendAuditOnce_OneRecordPerEventUnderConcurrency(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	raw := rawClient(t, env.URI)
	ctx := context.Background()

	var wg sync.WaitGroup
	inserted := make(chan bool, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.AppendAuditOnce(ctx, mongostore.AuditEntry{
				EventID: "evt-1", ActorID: "admin-1", Action: "identity.user_role_changed", ResourceType: "user", ResourceID: "u1",
			})
			if err != nil {
				t.Errorf("AppendAuditOnce: %v", err)
			}
			inserted <- ok
		}()
	}
	wg.Wait()
	close(inserted)
	n := 0
	for ok := range inserted {
		if ok {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d callers reported inserted=true, want exactly 1", n)
	}
	if c := count(t, raw, mongostore.CollAuditLog, bson.D{{Key: "event_id", Value: "evt-1"}}); c != 1 {
		t.Errorf("audit records for the event = %d, want 1", c)
	}
	// Plain AppendAudit records carry no event_id and never collide with each other.
	for range 2 {
		if _, err := s.AppendAudit(ctx, mongostore.AuditEntry{ActorID: "a", Action: "x", ResourceType: "user"}); err != nil {
			t.Fatalf("AppendAudit: %v", err)
		}
	}
	if _, err := s.AppendAuditOnce(ctx, mongostore.AuditEntry{ActorID: "a", Action: "x", ResourceType: "user"}); !errors.Is(err, mongostore.ErrInvalid) {
		t.Errorf("AppendAuditOnce without an event id: want ErrInvalid, got %v", err)
	}
}

func TestParkFailedEvent_KeepsReplayMetadataFromFirstParking(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()
	occurred := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	in := mongostore.ParkInput{
		EventID: "e1", Consumer: "c1", EventType: "identity.user_registered", Payload: []byte(`{"a":1}`),
		OutboxID: 42, SchemaID: 7, SchemaVersion: 3, OccurredAt: occurred, TraceID: "tr", SpanID: "sp", ActorID: "act", Err: "boom",
	}
	if _, err := s.ParkFailedEvent(ctx, in); err != nil {
		t.Fatal(err)
	}
	in.OutboxID, in.SchemaID = 99, 99 // a later parking must not overwrite the first
	if _, err := s.ParkFailedEvent(ctx, in); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetFailedEvent(ctx, "e1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if got.OutboxID != 42 || got.SchemaID != 7 || got.SchemaVersion != 3 || !got.OccurredAt.Equal(occurred) ||
		got.TraceID != "tr" || got.SpanID != "sp" || got.ActorID != "act" {
		t.Errorf("replay metadata = %+v", got)
	}
}

func TestSummarizeFailed_CountsBacklogAndOldest(t *testing.T) {
	env := testenv.SetupMongo(t)
	s := openStore(t, env.URI)
	ctx := context.Background()

	sum, err := s.SummarizeFailed(ctx)
	if err != nil || sum.Total != 0 || sum.OldestParkedAt != nil {
		t.Fatalf("empty store: %+v, %v", sum, err)
	}
	old := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Millisecond)
	park := func(id, consumer, typ string, at time.Time) {
		t.Helper()
		if _, err := s.ParkFailedEvent(ctx, mongostore.ParkInput{EventID: id, Consumer: consumer, EventType: typ, Err: "x", FailedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	park("e1", "analytics", "enrollment.student_enrolled", old)
	park("e2", "analytics", "enrollment.student_enrolled", time.Now())
	park("e3", "certificates", "progress.course_completed", time.Now())
	park("e4", "analytics", "identity.user_registered", time.Now())
	if _, err := s.MarkReplayed(ctx, "e4", "analytics", time.Now()); err != nil {
		t.Fatal(err)
	}

	sum, err = s.SummarizeFailed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Total != 4 || sum.Parked != 3 || sum.Replayed != 1 {
		t.Errorf("totals = %+v, want 4/3/1", sum)
	}
	if sum.ByConsumer["analytics"] != 2 || sum.ByConsumer["certificates"] != 1 || len(sum.ByConsumer) != 2 {
		t.Errorf("by consumer (parked only) = %v", sum.ByConsumer)
	}
	if sum.ByEventType["enrollment.student_enrolled"] != 2 || sum.ByEventType["identity.user_registered"] != 0 {
		t.Errorf("by event type (parked only) = %v", sum.ByEventType)
	}
	if sum.OldestParkedAt == nil || !sum.OldestParkedAt.Equal(old) {
		t.Errorf("oldest parked = %v, want %v", sum.OldestParkedAt, old)
	}
}
