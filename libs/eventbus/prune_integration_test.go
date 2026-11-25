//go:build integration

package eventbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

func addMarker(t *testing.T, db *gorm.DB, consumer string, age time.Duration) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if err := db.Exec(`INSERT INTO processed_events (consumer_name, event_id, processed_at) VALUES (?, ?, now() - make_interval(secs => ?))`,
		consumer, id, age.Seconds()).Error; err != nil {
		t.Fatalf("insert marker: %v", err)
	}
	return id
}

func markers(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	if err := db.Raw(`SELECT count(*) FROM processed_events`).Row().Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Retention boundary: a marker older than the retention goes, one younger stays,
// whatever its consumer; a marker just inside the window (one minute short of
// it) is never touched.
func TestPruneProcessed_RespectsTheRetentionBoundary(t *testing.T) {
	env := testenv.Setup(t)
	const retention = 24 * time.Hour
	oldA := addMarker(t, env.DB, "a", retention+time.Minute)
	oldB := addMarker(t, env.DB, "b", 30*24*time.Hour)
	youngA := addMarker(t, env.DB, "a", retention-time.Minute)
	youngB := addMarker(t, env.DB, "b", time.Second)

	n, err := eventbus.PruneProcessed(context.Background(), env.DB, retention)
	if err != nil {
		t.Fatalf("PruneProcessed: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted %d, want 2", n)
	}
	for _, c := range []struct {
		id   uuid.UUID
		want int64
	}{{oldA, 0}, {oldB, 0}, {youngA, 1}, {youngB, 1}} {
		var got int64
		if err := env.DB.Raw(`SELECT count(*) FROM processed_events WHERE event_id = ?`, c.id).Row().Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("marker %s: %d rows remain, want %d", c.id, got, c.want)
		}
	}
}

// A backlog larger than one batch is cleared in several DELETEs, and the young
// rows beside it survive every one of them.
func TestPruneProcessed_ClearsABacklogInBatches(t *testing.T) {
	env := testenv.Setup(t)
	for i := 0; i < 23; i++ {
		addMarker(t, env.DB, "c", 48*time.Hour)
	}
	for i := 0; i < 5; i++ {
		addMarker(t, env.DB, "c", time.Hour)
	}
	n, err := eventbus.PruneProcessed(context.Background(), env.DB, 24*time.Hour, eventbus.WithPruneBatch(4))
	if err != nil {
		t.Fatalf("PruneProcessed: %v", err)
	}
	if n != 23 || markers(t, env.DB) != 5 {
		t.Fatalf("deleted %d (want 23), %d remain (want 5)", n, markers(t, env.DB))
	}
	// Idempotent: nothing left to do.
	if n, err := eventbus.PruneProcessed(context.Background(), env.DB, 24*time.Hour); err != nil || n != 0 {
		t.Fatalf("second run deleted %d (%v), want 0", n, err)
	}
}

func TestPruneProcessed_RefusesANonPositiveRetentionAndBatch(t *testing.T) {
	env := testenv.Setup(t)
	addMarker(t, env.DB, "d", 365*24*time.Hour)
	for _, r := range []time.Duration{0, -time.Hour} {
		if _, err := eventbus.PruneProcessed(context.Background(), env.DB, r); err == nil {
			t.Errorf("retention %v accepted", r)
		}
	}
	if _, err := eventbus.PruneProcessed(context.Background(), env.DB, time.Hour, eventbus.WithPruneBatch(0)); err == nil {
		t.Error("batch 0 accepted")
	}
	if markers(t, env.DB) != 1 {
		t.Fatal("a refused call deleted rows")
	}
}

func TestPruneProcessed_StopsWhenTheContextIsCancelled(t *testing.T) {
	env := testenv.Setup(t)
	for i := 0; i < 10; i++ {
		addMarker(t, env.DB, "e", 48*time.Hour)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := eventbus.PruneProcessed(ctx, env.DB, time.Hour, eventbus.WithPruneBatch(2)); err == nil {
		t.Fatal("a cancelled context must return an error")
	}
}
