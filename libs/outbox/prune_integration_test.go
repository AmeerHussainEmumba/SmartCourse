//go:build integration

package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
)

// insertRow writes one outbox row with the given publication state and
// returns its id. publishedAgo < 0 means unpublished.
func insertRow(t *testing.T, env *testenv.Env, publishedAgo time.Duration, lastError string) int64 {
	t.Helper()
	var publishedAt, errText any
	if publishedAgo >= 0 {
		publishedAt = time.Now().UTC().Add(-publishedAgo)
	}
	if lastError != "" {
		errText = lastError
	}
	var id int64
	err := env.DB.Raw(`
		INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, occurred_at, published_at, last_error)
		VALUES (?, 'x', ?, 'x.y', 1, '{}', now() - interval '30 days', ?, ?) RETURNING id`,
		uuid.New(), uuid.New(), publishedAt, errText).Row().Scan(&id)
	if err != nil {
		t.Fatalf("insert outbox row: %v", err)
	}
	return id
}

func exists(t *testing.T, env *testenv.Env, id int64) bool {
	t.Helper()
	var n int
	if err := env.DB.Raw(`SELECT count(*) FROM outbox WHERE id = ?`, id).Row().Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func TestPrune_DeletesOnlyOldPublishedRows(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	day := 24 * time.Hour

	oldPublished := insertRow(t, env, 10*day, "")
	freshPublished := insertRow(t, env, day, "")
	oldUnpublished := insertRow(t, env, -1, "") // occurred 30 days ago, never published
	oldGivenUp := insertRow(t, env, 10*day, "handler failed 8 times")

	n, err := outbox.Prune(ctx, env.DB, 7*day)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if n != 1 {
		t.Fatalf("Prune removed %d rows, want 1", n)
	}
	if exists(t, env, oldPublished) {
		t.Error("an old published row survived")
	}
	for name, id := range map[string]int64{"recently published": freshPublished, "unpublished": oldUnpublished, "given-up (last_error set)": oldGivenUp} {
		if !exists(t, env, id) {
			t.Errorf("the %s row was deleted", name)
		}
	}
}

func TestPrune_DrainsMoreThanOneBatch(t *testing.T) {
	env := testenv.Setup(t)
	if err := env.DB.Exec(`
		INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, published_at)
		SELECT gen_random_uuid(), 'x', gen_random_uuid(), 'x.y', 1, '{}', now() - interval '10 days'
		FROM generate_series(1, 2500)`).Error; err != nil {
		t.Fatal(err)
	}
	keep := insertRow(t, env, -1, "")

	n, err := outbox.Prune(context.Background(), env.DB, 24*time.Hour)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if n != 2500 {
		t.Fatalf("Prune removed %d rows, want 2500", n)
	}
	if !exists(t, env, keep) {
		t.Error("the unpublished row was deleted")
	}
}

func TestPrune_RejectsNonPositiveRetention(t *testing.T) {
	env := testenv.Setup(t)
	old := insertRow(t, env, 10*24*time.Hour, "")
	for _, d := range []time.Duration{0, -time.Hour} {
		if _, err := outbox.Prune(context.Background(), env.DB, d); err == nil {
			t.Errorf("Prune(%v) succeeded", d)
		}
	}
	if !exists(t, env, old) {
		t.Fatal("a rejected Prune still deleted rows")
	}
}
