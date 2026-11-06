//go:build integration

// What this file checks, against real Postgres and the real relay (RFC
// §18.2): that identity.user_registered and identity.user_profile_updated
// actually populate/update instructor_mirror through the real outbox ->
// relay -> handler path production traffic uses — not a direct call to
// UpsertInstructorMirror (that's instructor_mirror_integration_test.go's
// job), which would only prove the upsert works, not that handleUser
// Registered/handleUserProfileUpdated correctly decode a real event's
// payload. Also checks that a malformed payload is treated as a genuine
// handler failure (retried, then given up on) rather than silently
// succeeding with zero effect.
package course_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/outbox"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func TestHandleUserRegistered_PopulatesInstructorMirror(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	userID := uuid.Must(uuid.NewV7())

	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "user", AggregateID: userID, EventType: "identity.user_registered", SchemaVersion: 1,
			Payload: map[string]any{"user_id": userID, "email": "a@example.com", "role": "instructor", "full_name": "Real Handler Name"},
		})
	}); err != nil {
		t.Fatalf("write identity.user_registered event: %v", err)
	}

	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond))
	for _, h := range course.Handlers() {
		rl.Register(h)
	}
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	deadline := time.After(2500 * time.Millisecond)
	for {
		var count int
		env.DB.Raw(`SELECT count(*) FROM instructor_mirror WHERE user_id = ? AND full_name = ?`, userID, "Real Handler Name").
			Row().Scan(&count)
		if count == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("instructor_mirror row for %s never appeared with the registered name", userID)
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
}

func TestHandleUserProfileUpdated_UpdatesInstructorMirror(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	userID := uuid.Must(uuid.NewV7())

	// Seed the mirror row the way handleUserRegistered would have, at
	// last_event_id 0 — lower than any real outbox row's id (BIGSERIAL
	// starts at 1 in this fresh, disposable database) — so the update's
	// last_event_id guard (ADR-0024) doesn't reject it as out of order.
	if err := env.DB.Exec(`INSERT INTO instructor_mirror (user_id, full_name, last_event_id) VALUES (?, 'Old Name', 0)`, userID).Error; err != nil {
		t.Fatalf("seed instructor_mirror: %v", err)
	}

	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "user", AggregateID: userID, EventType: "identity.user_profile_updated", SchemaVersion: 1,
			Payload: map[string]any{"user_id": userID, "full_name": "Updated Via Handler"},
		})
	}); err != nil {
		t.Fatalf("write identity.user_profile_updated event: %v", err)
	}

	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond))
	for _, h := range course.Handlers() {
		rl.Register(h)
	}
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	deadline := time.After(2500 * time.Millisecond)
	for {
		var name string
		env.DB.Raw(`SELECT full_name FROM instructor_mirror WHERE user_id = ?`, userID).Row().Scan(&name)
		if name == "Updated Via Handler" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("instructor_mirror row for %s never reflected the profile_updated name (still %q)", userID, name)
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
}

// TestHandleUserRegistered_MalformedPayload_GivesUpRatherThanSucceeding is
// the regression test for the class of bug this file exists to catch:
// instructor_mirror_integration_test.go calls UpsertInstructorMirror
// directly and would never notice if handleUserRegistered's own
// json.Unmarshal of a real event's payload were broken. A payload whose
// user_id isn't a valid UUID fails to decode into the handler's payload
// struct — the relay must treat that as a genuine failure (retried, then
// given up on, same as relay_integration_test.go's poison-event case), not
// silently succeed with no instructor_mirror row.
func TestHandleUserRegistered_MalformedPayload_GivesUpRatherThanSucceeding(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()
	aggID := uuid.Must(uuid.NewV7())

	if err := env.DB.Transaction(func(tx *gorm.DB) error {
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "user", AggregateID: aggID, EventType: "identity.user_registered", SchemaVersion: 1,
			Payload: map[string]any{"user_id": "not-a-uuid", "full_name": "Should Never Land"},
		})
	}); err != nil {
		t.Fatalf("write malformed identity.user_registered event: %v", err)
	}

	rl := relay.New(env.DB, relay.WithWorkerCount(1), relay.WithPollInterval(50*time.Millisecond), relay.WithMaxAttempts(3))
	for _, h := range course.Handlers() {
		rl.Register(h)
	}
	runCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go rl.Run(runCtx)

	deadline := time.After(2500 * time.Millisecond)
	for {
		_, _, givenUp := rl.Stats()
		if givenUp > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("relay never gave up on the malformed payload; expected 3 failed attempts then a give-up")
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	time.Sleep(100 * time.Millisecond)

	var count int
	env.DB.Raw(`SELECT count(*) FROM instructor_mirror WHERE full_name = ?`, "Should Never Land").Row().Scan(&count)
	if count != 0 {
		t.Fatalf("expected no instructor_mirror row from a malformed payload, got %d", count)
	}

	var unpublished int
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NULL`, aggID).Row().Scan(&unpublished)
	if unpublished != 0 {
		t.Fatal("expected the given-up event to be marked published (relay's documented give-up behavior), not left for endless retry")
	}
}
