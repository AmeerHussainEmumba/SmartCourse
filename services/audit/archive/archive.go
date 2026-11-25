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
// of eventType was written against at schemaVersion, or 0 when unknown. It is a
// fallback: over Kafka the runner puts the frame's schema id in the context
// (relay.SourceFrom) and that wins. The in-process transport has no registry, so
// without a resolver the stored id is 0 and the header schema_id_source says
// "none".
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
	doc := a.eventDoc(ctx, evt)
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
//
// Over Kafka the context carries the record's relay.Source: its frame's schema
// id is stored as schema_id (header schema_id_source "registry_frame") and its
// topic, key, partition and offset as top-level fields. In-process delivery has
// none of that: schema_id is the resolver's answer or 0, the header says
// "resolver" or "none", and no Kafka field is stored.
func (a *archiver) eventDoc(ctx context.Context, evt relay.DecodedEvent) mongostore.Event {
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
	doc := mongostore.Event{
		ID:            evt.EventID.String(),
		EventType:     evt.EventType,
		SchemaVersion: int32(evt.SchemaVersion),
		AggregateType: evt.AggregateType,
		AggregateID:   evt.AggregateID.String(),
		OccurredAt:    evt.OccurredAt.UTC(),
		TraceID:       evt.TraceID,
		Payload:       []byte(evt.Payload),
		Headers:       h,
		ArchivedAt:    a.now(),
	}
	src, fromBroker := relay.SourceFrom(ctx)
	if fromBroker && src.SchemaID != 0 {
		doc.SchemaID = int32(src.SchemaID)
		h["schema_id_source"] = "registry_frame"
	} else {
		doc.SchemaID = a.schemaID(evt.EventType, evt.SchemaVersion)
		h["schema_id_source"] = "none"
		if doc.SchemaID != 0 {
			h["schema_id_source"] = "resolver"
		}
	}
	if fromBroker {
		partition, offset := src.Partition, src.Offset
		doc.Topic, doc.Key, doc.Partition, doc.Offset = src.Topic, src.Key, &partition, &offset
	}
	return doc
}

// auditSpec says which event types are audit-relevant and what they are about.
// It follows the RFC's event table (§4.6, "archive + audit" for CoursePublished,
// CoursePublishFailed, CourseArchived and CertificateIssued) and the audit_log
// description in §5.4 ("role changes, publish and archive, enrollment
// overrides, certificate issuance, and administrative deletion"): role change,
// publish, publish failure, archive, certificate issuance. Every other
// catalogue type is "archive" only in the table. "Enrollment overrides" and
// "administrative deletion" have no event type in the catalogue; when one is
// added it must be listed here. Admin replays of failed events are audited by
// the admin API, not through events.
var auditSpec = map[string]struct {
	resourceType string
	idKey        string // payload field naming the resource; the aggregate id otherwise
}{
	"identity.user_role_changed":  {"user", "user_id"},
	"course.course_published":     {"course", "course_id"},
	"course.course_archived":      {"course", "course_id"},
	"course.publish_failed":       {"course", "course_id"},
	"progress.certificate_issued": {"certificate", "certificate_id"},
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
