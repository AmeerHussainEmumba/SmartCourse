package mongostore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Event is an archived domain event (RFC §5.4). Payload is the wire bytes as
// published (Protobuf); SchemaID is the registry id the payload was written
// against, so a years-old event stays interpretable (RFC §6.8).
type Event struct {
	ID            string            `bson:"_id"`
	EventType     string            `bson:"event_type"`
	SchemaID      int32             `bson:"schema_id"`
	SchemaVersion int32             `bson:"schema_version,omitempty"`
	AggregateType string            `bson:"aggregate_type"`
	AggregateID   string            `bson:"aggregate_id"`
	OccurredAt    time.Time         `bson:"occurred_at"`
	TraceID       string            `bson:"trace_id,omitempty"`
	Payload       []byte            `bson:"payload"`
	Headers       map[string]string `bson:"headers,omitempty"`
	ArchivedAt    time.Time         `bson:"archived_at"`
}

// ArchiveEvent stores e once, keyed by e.ID (the event_id). It is idempotent:
// archiving an id that already exists changes nothing and reports
// inserted=false, so a redelivered event is safe. The first write wins.
func (s *Store) ArchiveEvent(ctx context.Context, e Event) (inserted bool, err error) {
	if err := s.ready(); err != nil {
		return false, err
	}
	if e.ID == "" || e.EventType == "" {
		return false, fmt.Errorf("%w: event id and type are required", ErrInvalid)
	}
	if len(e.Payload) > MaxPayloadBytes {
		return false, ErrPayloadTooLarge
	}
	if e.ArchivedAt.IsZero() {
		e.ArchivedAt = time.Now().UTC()
	}
	doc := bson.D{
		{Key: "event_type", Value: e.EventType},
		{Key: "schema_id", Value: e.SchemaID},
		{Key: "schema_version", Value: e.SchemaVersion},
		{Key: "aggregate_type", Value: e.AggregateType},
		{Key: "aggregate_id", Value: e.AggregateID},
		{Key: "occurred_at", Value: e.OccurredAt},
		{Key: "trace_id", Value: e.TraceID},
		{Key: "payload", Value: e.Payload},
		{Key: "headers", Value: e.Headers},
		{Key: "archived_at", Value: e.ArchivedAt},
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var res *mongo.UpdateResult
	for range 3 { // a concurrent upsert of the same id can report a duplicate key; retry as an update
		res, err = s.events.UpdateOne(ctx, bson.D{{Key: "_id", Value: e.ID}},
			bson.D{{Key: "$setOnInsert", Value: doc}}, options.UpdateOne().SetUpsert(true))
		if err == nil || !mongo.IsDuplicateKeyError(err) {
			break
		}
	}
	if err != nil {
		return false, fmt.Errorf("mongostore: archive event: %w", err)
	}
	return res.UpsertedCount > 0, nil
}

// GetEvent returns an archived event or ErrNotFound.
func (s *Store) GetEvent(ctx context.Context, id string) (*Event, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var e Event
	err := s.events.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&e)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("mongostore: get event: %w", err)
	}
	return &e, nil
}

// AuditEntry is one audit_log record (RFC §5.4): who did what to which
// resource. Before and After may be any BSON-marshalable value.
type AuditEntry struct {
	ActorID      string    `bson:"actor_id"`
	ActorRole    string    `bson:"actor_role"`
	Action       string    `bson:"action"`
	ResourceType string    `bson:"resource_type"`
	ResourceID   string    `bson:"resource_id"`
	Before       any       `bson:"before,omitempty"`
	After        any       `bson:"after,omitempty"`
	IP           string    `bson:"ip,omitempty"`
	UserAgent    string    `bson:"user_agent,omitempty"`
	At           time.Time `bson:"at"`
	TraceID      string    `bson:"trace_id,omitempty"`
}

// AppendAudit appends an audit record and returns its id. Audit records are
// append-only: there is no update or delete in this package.
func (s *Store) AppendAudit(ctx context.Context, a AuditEntry) (string, error) {
	if err := s.ready(); err != nil {
		return "", err
	}
	if a.ActorID == "" || a.Action == "" || a.ResourceType == "" {
		return "", fmt.Errorf("%w: actor, action and resource type are required", ErrInvalid)
	}
	if a.At.IsZero() {
		a.At = time.Now().UTC()
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	res, err := s.audit.InsertOne(ctx, a)
	if err != nil {
		return "", fmt.Errorf("mongostore: append audit: %w", err)
	}
	oid, _ := res.InsertedID.(bson.ObjectID)
	return oid.Hex(), nil
}
