// Package archive is the audit-archiver consumer (RFC §6.5, §6.8, ADR-0031):
// it writes every event to the MongoDB `events` archive, keyed by event_id,
// with what a reader needs to interpret it years later, and writes an
// `audit_log` record for the audit-relevant types (RFC §5.4).
//
// The consumer is idempotent by event_id: a duplicate or replayed delivery
// leaves exactly one archive document and one audit record. MongoDB being
// down is an error from the handler (the transport retries, and parks the
// event if the outage outlasts the retry budget), never a panic and never a
// silent drop.
//
// Register Handlers only when events travel over Kafka. In in-process mode
// a failing handler fails the whole relay row, so a MongoDB outage would
// stall every other consumer behind the archiver (see README).
package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/mongostore"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// Consumer is the consumer name: the processed_events marker, the failed_events
// consumer and the Kafka consumer group all use it (RFC §6.5).
const Consumer = "audit-archiver"

// ErrNoStore is returned by the handler when it was built without a store.
var ErrNoStore = errors.New("archive: no MongoDB store configured")

// Store is the part of *mongostore.Store the archiver uses.
type Store interface {
	ArchiveEvent(ctx context.Context, e mongostore.Event) (inserted bool, err error)
	AppendAuditOnce(ctx context.Context, a mongostore.AuditEntry) (inserted bool, err error)
}

// SchemaResolver returns the Schema Registry id of the payload schema an event
// of eventType was written against at schemaVersion, or 0 when unknown. The
// in-process transport has no registry, so by default every id is 0; the Kafka
// transport knows the id from the wire format and should supply it.
type SchemaResolver func(eventType string, schemaVersion int) int32

// Option configures Handlers.
type Option func(*archiver)

// WithSchemaResolver sets how the archiver learns the registry schema id.
func WithSchemaResolver(r SchemaResolver) Option { return func(a *archiver) { a.schemaID = r } }

type archiver struct {
	store    Store
	schemaID SchemaResolver
	now      func() time.Time
}

// Handlers returns the audit-archiver's relay handlers: one that consumes
// every event type in the catalogue (libs/events). A nil store yields a
// handler that fails every event with ErrNoStore instead of panicking.
func Handlers(store Store, opts ...Option) []relay.Handler {
	a := &archiver{store: store, schemaID: func(string, int) int32 { return 0 }, now: func() time.Time { return time.Now().UTC() }}
	for _, o := range opts {
		o(a)
	}
	types := events.Types()
	names := make([]string, 0, len(types))
	for _, t := range types {
		names = append(names, t.EventType)
	}
	return []relay.Handler{relay.NewHandler(Consumer, names, a.handle)}
}

// handle archives one event. The Postgres transaction is not used: MongoDB is
// not part of it, and both writes are idempotent by event_id, so a crash
// between them, or between them and the processed_events marker, is repaired
// by the redelivery.
func (a *archiver) handle(ctx context.Context, _ *gorm.DB, evt relay.DecodedEvent) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("archive: panic archiving %s: %v", evt.EventType, p)
		}
	}()
	if a.store == nil {
		return ErrNoStore
	}
	doc := a.eventDoc(evt)
	if _, err := a.store.ArchiveEvent(ctx, doc); err != nil {
		return fmt.Errorf("archive event %s: %w", evt.EventID, err)
	}
	if entry, ok := auditEntry(evt); ok {
		if _, err := a.store.AppendAuditOnce(ctx, entry); err != nil {
			return fmt.Errorf("audit event %s: %w", evt.EventID, err)
		}
	}
	return nil
}

// eventDoc builds the archive document. Payload is the bytes as delivered
// (JSON in this codebase today); headers say how to read it: the encoding, and
// the registry subject so the schema is resolvable by name even where the
// numeric id is unknown (RFC §6.8).
func (a *archiver) eventDoc(evt relay.DecodedEvent) mongostore.Event {
	h := map[string]string{"payload_encoding": "json"}
	if t, ok := events.Lookup(evt.EventType); ok {
		h["schema_subject"] = t.Subject()
	}
	if evt.SpanID != "" {
		h["span_id"] = evt.SpanID
	}
	if evt.ActorID != nil {
		h["actor_id"] = evt.ActorID.String()
	}
	if evt.OutboxID != 0 {
		h["outbox_id"] = fmt.Sprint(evt.OutboxID)
	}
	return mongostore.Event{
		ID:            evt.EventID.String(),
		EventType:     evt.EventType,
		SchemaID:      a.schemaID(evt.EventType, evt.SchemaVersion),
		SchemaVersion: int32(evt.SchemaVersion),
		AggregateType: evt.AggregateType,
		AggregateID:   evt.AggregateID.String(),
		OccurredAt:    evt.OccurredAt.UTC(),
		TraceID:       evt.TraceID,
		Payload:       []byte(evt.Payload),
		Headers:       h,
		ArchivedAt:    a.now(),
	}
}

// auditSpec says which event types are audit-relevant (RFC §5.4: "role
// changes, publish and archive, enrollment overrides, certificate issuance,
// and administrative deletion") and what they are about. Only types the
// services emit today are listed; a new sensitive event type must be added here.
var auditSpec = map[string]struct {
	resourceType string
	idKey        string // payload field naming the resource; the aggregate id otherwise
}{
	"identity.user_role_changed": {"user", "user_id"},
	"course.course_published":    {"course", "course_id"},
	"course.course_archived":     {"course", "course_id"},
}

// AuditTypes lists the event types that also produce an audit_log record.
func AuditTypes() []string {
	out := make([]string, 0, len(auditSpec))
	for t := range auditSpec {
		out = append(out, t)
	}
	return out
}

// auditEntry derives the audit record for evt, or reports false for an event
// type that is archived only. It records the actor (RFC §5.4: that is what
// makes it an audit trail and not an event log); "system" when the event had none.
func auditEntry(evt relay.DecodedEvent) (mongostore.AuditEntry, bool) {
	spec, ok := auditSpec[evt.EventType]
	if !ok {
		return mongostore.AuditEntry{}, false
	}
	var payload map[string]any
	_ = json.Unmarshal(evt.Payload, &payload) // not JSON: the record just has no "after"
	resource := evt.AggregateID.String()
	if v, ok := payload[spec.idKey].(string); ok && v != "" {
		resource = v
	}
	actor := "system"
	if evt.ActorID != nil {
		actor = evt.ActorID.String()
	}
	e := mongostore.AuditEntry{
		EventID:      evt.EventID.String(),
		ActorID:      actor,
		Action:       evt.EventType,
		ResourceType: spec.resourceType,
		ResourceID:   resource,
		At:           evt.OccurredAt.UTC(),
		TraceID:      evt.TraceID,
	}
	if payload != nil {
		e.After = payload
	}
	return e, true
}
