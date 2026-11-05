// Relay handler registrations (RFC §6.5's consumer table). Same shape as
// internal/analytics/handlers.go and internal/enrollment/handlers.go: each
// function has the internal/platform/relay.HandlerFunc signature, receiving
// the transaction the idempotency marker will commit in, so the effect and
// "I've processed this" become atomic together (RFC §6.6).
//
// This module's first relay consumer (ADR-0024): ListCatalogue used to
// JOIN directly against identity.users for the instructor's display name —
// a cross-schema join the schema-per-service split rules out. instructor_
// mirror (repository.go) replaces it, kept current from identity's own
// events rather than a live cross-schema read.
package course

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/relay"
)

const consumerName = "course-instructor-mirror"

func Handlers() []relay.Handler {
	return []relay.Handler{
		relay.NewHandler(consumerName, []string{"identity.user_registered"}, handleUserRegistered),
		relay.NewHandler(consumerName, []string{"identity.user_profile_updated"}, handleUserProfileUpdated),
	}
}

func handleUserRegistered(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		UserID   uuid.UUID `json:"user_id"`
		FullName string    `json:"full_name"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode identity.user_registered payload: %w", err)
	}
	return UpsertInstructorMirror(ctx, tx, payload.UserID, payload.FullName, evt.OutboxID)
}

func handleUserProfileUpdated(ctx context.Context, tx *gorm.DB, evt relay.DecodedEvent) error {
	var payload struct {
		UserID   uuid.UUID `json:"user_id"`
		FullName string    `json:"full_name"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("decode identity.user_profile_updated payload: %w", err)
	}
	return UpsertInstructorMirror(ctx, tx, payload.UserID, payload.FullName, evt.OutboxID)
}
