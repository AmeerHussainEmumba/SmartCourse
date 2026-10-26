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

// Row mirrors the `outbox` table (migrations/000006_platform.up.sql).
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
	// Headers currently carries only what's set explicitly; trace_id/span_id
	// propagation (RFC §12.2) is wired in when OpenTelemetry lands (M4).
	Headers map[string]any
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
	}
	if err := tx.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("write outbox row: %w", err)
	}
	return nil
}
