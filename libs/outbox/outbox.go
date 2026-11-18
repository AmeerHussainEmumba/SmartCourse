// Package outbox is the only path from a state change to a published event
// (ADR-0006). No module writes to Kafka directly; a module writes an event
// to this table inside its own transaction, and the relay (M3) publishes it
// later. A Kafka producer imported outside this package is a defect.
//
// The relay, protobuf encoding, and Schema Registry integration are M3
// scope (RFC §21) — the table and this writer exist from M0 because
// retrofitting the outbox onto write paths built without it means touching
// every one of those paths a second time. Event.Payload is JSON for now,
// upgraded to protobuf when the schemas exist (ADR-0008); callers should
// not assume today's wire format is final.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Row mirrors the `outbox` table (migrations/platform/000002_platform.up.sql
// and 000003_outbox_trace_actor.up.sql).
type Row struct {
	ID            int64 `gorm:"primaryKey"`
	EventID       uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	SchemaVersion int
	Payload       []byte
	Headers       json.RawMessage `gorm:"type:jsonb"`
	OccurredAt    time.Time
	PublishedAt   *time.Time
	Attempts      int
	LastError     *string
	TraceID       *string    // NULL when the writer had no trace context (000003)
	SpanID        *string    // NULL likewise
	ActorID       *uuid.UUID // NULL for system-originated events
}

func (Row) TableName() string { return "outbox" }

// Event is the writer-facing shape — deliberately smaller than Row (no
// PublishedAt/Attempts/LastError, which are the relay's concern, not the
// writer's).
type Event struct {
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	SchemaVersion int
	Payload       any // marshalled to JSON; see package doc re: protobuf migration
	// Headers carries only what's set explicitly.
	Headers map[string]any
	// TraceID and SpanID are the W3C trace-context ids (lowercase hex, 32 and
	// 16 chars) of the request that caused the event (RFC §6.4, §12.2). Empty
	// means "no trace context" and is stored as NULL. Callers that predate
	// these fields leave them empty and behave exactly as before. This
	// package does not validate or generate them; libs/telemetry supplies
	// them.
	TraceID string
	SpanID  string
	// ActorID is the authenticated user who caused the event, for audit
	// (RFC §6.4). Nil means system-originated and is stored as NULL.
	ActorID *uuid.UUID
}

// Write inserts evt into the outbox using tx — the caller's transaction,
// not a new one. This is the entire mechanism behind ADR-0006: the event
// row and the state-change rows commit or roll back together, so a crash
// between "state changed" and "event published" is structurally
// impossible — there is no such window.
func Write(ctx context.Context, tx *gorm.DB, evt Event) error {
	payload, err := json.Marshal(evt.Payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	headers, err := json.Marshal(evt.Headers)
	if err != nil {
		return fmt.Errorf("marshal outbox headers: %w", err)
	}

	row := Row{
		EventID:       uuid.New(), // dedup key for at-least-once delivery (RFC §6.4) — random UUID is fine, not UUIDv7; ordering comes from `id`, not this
		AggregateType: evt.AggregateType,
		AggregateID:   evt.AggregateID,
		EventType:     evt.EventType,
		SchemaVersion: evt.SchemaVersion,
		Payload:       payload,
		Headers:       headers,
		OccurredAt:    time.Now().UTC(),
		TraceID:       nullIfEmpty(evt.TraceID),
		SpanID:        nullIfEmpty(evt.SpanID),
		ActorID:       evt.ActorID,
	}
	if err := tx.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("write outbox row: %w", err)
	}
	return nil
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
