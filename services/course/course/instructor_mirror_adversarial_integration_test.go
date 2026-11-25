//go:build integration

package course_test

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

type mirrorEvt struct {
	outboxID int64
	eventID  uuid.UUID
	typ      string
	payload  map[string]any
}

// deliverMirror emulates one relay delivery (same idempotency rule as
// relay.runOne) of e to every course handler subscribed to its type.
func deliverMirror(t *testing.T, env *testenv.Env, e mirrorEvt) {
	t.Helper()
	raw, _ := json.Marshal(e.payload)
	for _, h := range course.Handlers() {
		if !h.EventTypes[e.typ] {
			continue
		}
		err := env.DB.Transaction(func(tx *gorm.DB) error {
			var n int64
			if err := tx.Raw(`SELECT count(*) FROM processed_events WHERE consumer_name = ? AND event_id = ?`, h.ConsumerName, e.eventID).Row().Scan(&n); err != nil {
				return err
			}
			if n > 0 {
				return nil
			}
			if err := h.Handle(context.Background(), tx, relay.DecodedEvent{OutboxID: e.outboxID, EventID: e.eventID, EventType: e.typ, Payload: raw}); err != nil {
				return err
			}
			return tx.Exec(`INSERT INTO processed_events (consumer_name, event_id) VALUES (?, ?)`, h.ConsumerName, e.eventID).Error
		})
		if err != nil {
			t.Fatalf("deliver %s: %v", e.typ, err)
		}
	}
}

// Duplicated and reordered identity events must always leave the name from
// the highest outbox id, and the catalogue must show it.
func TestAdvInstructorMirror_DuplicatedAndReorderedIdentityEvents(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	ctx := context.Background()
	svc := newTestService(env)
	uid := uuid.New()
	events := []mirrorEvt{
		{10, uuid.New(), "identity.user_registered", map[string]any{"user_id": uid, "full_name": "First", "role": "instructor"}},
		{20, uuid.New(), "identity.user_profile_updated", map[string]any{"user_id": uid, "full_name": "Second"}},
		{30, uuid.New(), "identity.user_profile_updated", map[string]any{"user_id": uid, "full_name": "Third"}},
	}
	for seed := int64(0); seed < 8; seed++ {
		if err := env.DB.Exec(`DELETE FROM instructor_mirror`).Error; err != nil || env.DB.Exec(`DELETE FROM processed_events`).Error != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(seed))
		var stream []mirrorEvt
		for _, e := range events {
			for c := 1 + rng.Intn(3); c > 0; c-- {
				stream = append(stream, e)
			}
		}
		rng.Shuffle(len(stream), func(i, j int) { stream[i], stream[j] = stream[j], stream[i] })
		for _, e := range stream {
			deliverMirror(t, env, e)
		}
		var name string
		var rows int64
		env.DB.Raw(`SELECT full_name FROM instructor_mirror WHERE user_id = ?`, uid).Row().Scan(&name)
		env.DB.Raw(`SELECT count(*) FROM instructor_mirror`).Row().Scan(&rows)
		if name != "Third" || rows != 1 {
			t.Fatalf("seed %d: name=%q rows=%d", seed, name, rows)
		}
	}

	cid, _ := newCourse(t, svc, uid, "mirror-cat", "Mirror Cat")
	authorDraft(t, svc, cid, uid, 1, 1)
	runPublishPipeline(t, svc, cid, uid)
	cat, err := svc.ListCatalogue(ctx, 10, "", "", "", "")
	if err != nil || len(cat.Items) != 1 || cat.Items[0].InstructorName != "Third" {
		t.Fatalf("catalogue instructor name: %v %+v", err, cat)
	}
}

// profile_updated arriving before registered for an unknown user must still
// converge (registration has the lower id and loses), and a published course
// whose instructor has no mirror row yet must still list with an empty name.
func TestAdvInstructorMirror_ProfileBeforeRegisteredAndMissingMirror(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	ctx := context.Background()
	svc := newTestService(env)
	uid := uuid.New()

	cid, _ := newCourse(t, svc, uid, "no-mirror", "No Mirror")
	authorDraft(t, svc, cid, uid, 1, 1)
	runPublishPipeline(t, svc, cid, uid)
	cat, err := svc.ListCatalogue(ctx, 10, "", "", "", "")
	if err != nil || len(cat.Items) != 1 || cat.Items[0].InstructorName != "" {
		t.Fatalf("course with no mirror row must still be listed: %v %+v", err, cat)
	}

	deliverMirror(t, env, mirrorEvt{50, uuid.New(), "identity.user_profile_updated", map[string]any{"user_id": uid, "full_name": "Updated"}})
	deliverMirror(t, env, mirrorEvt{40, uuid.New(), "identity.user_registered", map[string]any{"user_id": uid, "full_name": "Original"}})
	var name string
	env.DB.Raw(`SELECT full_name FROM instructor_mirror WHERE user_id = ?`, uid).Row().Scan(&name)
	if name != "Updated" {
		t.Fatalf("name=%q", name)
	}
}
