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

// Status is a failed event's lifecycle state.
type Status string

const (
	// StatusParked means the event was given up on and awaits an operator.
	StatusParked Status = "parked"
	// StatusReplayed means an operator replayed it. Parking it again (it failed
	// again) moves it back to parked.
	StatusReplayed Status = "replayed"
)

// ErrorEntry is one failure in a failed event's history.
type ErrorEntry struct {
	At      time.Time `bson:"at"`
	Attempt int       `bson:"attempt"`
	Message string    `bson:"message"`
}

// FailedEvent is a parked event, unique per (EventID, Consumer). Payload is
// empty on values returned by ParkFailedEvent and ListFailedEvents; use
// GetFailedEvent for the bytes. ID is the document id, the pagination key.
type FailedEvent struct {
	ID            string `bson:"-"`
	EventID       string `bson:"event_id"`
	Consumer      string `bson:"consumer"`
	EventType     string `bson:"event_type"`
	Topic         string `bson:"topic,omitempty"`
	AggregateType string `bson:"aggregate_type,omitempty"`
	AggregateID   string `bson:"aggregate_id,omitempty"`
	Payload       []byte `bson:"payload,omitempty"`
	// Replay metadata, written once with the payload: what a replayer needs to
	// rebuild the event the consumer saw. Zero on documents parked before these
	// fields existed.
	OutboxID      int64        `bson:"outbox_id,omitempty"`
	SchemaID      int32        `bson:"schema_id,omitempty"`
	SchemaVersion int32        `bson:"schema_version,omitempty"`
	OccurredAt    time.Time    `bson:"occurred_at,omitempty"`
	TraceID       string       `bson:"trace_id,omitempty"`
	SpanID        string       `bson:"span_id,omitempty"`
	ActorID       string       `bson:"actor_id,omitempty"`
	Attempts      int          `bson:"attempts"`
	Errors        []ErrorEntry `bson:"errors"`
	FirstFailedAt time.Time    `bson:"first_failed_at"`
	LastFailedAt  time.Time    `bson:"last_failed_at"`
	Status        Status       `bson:"status"`
	ReplayedAt    *time.Time   `bson:"replayed_at,omitempty"`
}

// failedDoc adds the Mongo id to the decoded shape.
type failedDoc struct {
	OID         bson.ObjectID `bson:"_id"`
	FailedEvent `bson:",inline"`
}

func (d failedDoc) toEvent() FailedEvent {
	f := d.FailedEvent
	f.ID = d.OID.Hex()
	return f
}

// ParkInput describes one give-up. Attempts is how many delivery attempts this
// parking represents (0 counts as 1).
type ParkInput struct {
	EventID       string
	Consumer      string
	EventType     string
	Topic         string
	AggregateType string
	AggregateID   string
	Payload       []byte
	// OutboxID, SchemaID, SchemaVersion, OccurredAt, TraceID, SpanID and ActorID
	// are kept from the first parking, like Payload, so the event can be rebuilt
	// for replay (handlers order mirror updates by OutboxID).
	OutboxID      int64
	SchemaID      int32
	SchemaVersion int32
	OccurredAt    time.Time
	TraceID       string
	SpanID        string
	ActorID       string
	Attempts      int
	Err           string
	FailedAt      time.Time
}

// ParkFailedEvent records that consumer gave up on an event. It is idempotent
// per (EventID, Consumer): the first call creates the document; later calls
// append to its error history and add to attempts instead of duplicating.
// Safe under concurrency. The payload and replay metadata are stored from the first call only.
func (s *Store) ParkFailedEvent(ctx context.Context, in ParkInput) (*FailedEvent, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if in.EventID == "" || in.Consumer == "" {
		return nil, fmt.Errorf("%w: event id and consumer are required", ErrInvalid)
	}
	if len(in.Payload) > MaxPayloadBytes {
		return nil, ErrPayloadTooLarge
	}
	attempts := max(in.Attempts, 1)
	at := in.FailedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	update := bson.D{
		{Key: "$setOnInsert", Value: bson.D{
			{Key: "event_type", Value: in.EventType},
			{Key: "topic", Value: in.Topic},
			{Key: "aggregate_type", Value: in.AggregateType},
			{Key: "aggregate_id", Value: in.AggregateID},
			{Key: "payload", Value: in.Payload},
			{Key: "outbox_id", Value: in.OutboxID},
			{Key: "schema_id", Value: in.SchemaID},
			{Key: "schema_version", Value: in.SchemaVersion},
			{Key: "occurred_at", Value: in.OccurredAt},
			{Key: "trace_id", Value: in.TraceID},
			{Key: "span_id", Value: in.SpanID},
			{Key: "actor_id", Value: in.ActorID},
		}},
		{Key: "$set", Value: bson.D{{Key: "status", Value: StatusParked}}},
		{Key: "$unset", Value: bson.D{{Key: "replayed_at", Value: ""}}},
		{Key: "$inc", Value: bson.D{{Key: "attempts", Value: attempts}}},
		{Key: "$min", Value: bson.D{{Key: "first_failed_at", Value: at}}},
		{Key: "$max", Value: bson.D{{Key: "last_failed_at", Value: at}}},
		{Key: "$push", Value: bson.D{{Key: "errors", Value: bson.D{
			{Key: "$each", Value: bson.A{ErrorEntry{At: at, Attempt: attempts, Message: in.Err}}},
			{Key: "$slice", Value: -MaxErrorHistory},
		}}}},
	}
	filter := bson.D{{Key: "event_id", Value: in.EventID}, {Key: "consumer", Value: in.Consumer}}
	opts := options.FindOneAndUpdate().SetUpsert(true).
		SetReturnDocument(options.After).
		SetProjection(bson.D{{Key: "payload", Value: 0}})

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var (
		doc failedDoc
		err error
	)
	for range 3 { // lost an upsert race: the document exists now, retry as an update
		err = s.failed.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
		if err == nil || !mongo.IsDuplicateKeyError(err) {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("mongostore: park failed event: %w", err)
	}
	f := doc.toEvent()
	return &f, nil
}

// GetFailedEvent returns the full document, payload included, or ErrNotFound.
func (s *Store) GetFailedEvent(ctx context.Context, eventID, consumer string) (*FailedEvent, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var doc failedDoc
	err := s.failed.FindOne(ctx, bson.D{{Key: "event_id", Value: eventID}, {Key: "consumer", Value: consumer}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("mongostore: get failed event: %w", err)
	}
	f := doc.toEvent()
	return &f, nil
}

// MarkReplayed moves a failed event parked -> replayed. It reports
// transitioned=true for exactly one caller however many race: the transition
// is a single conditional update on status=parked. An event already replayed
// returns (false, nil); an unknown one returns ErrNotFound.
func (s *Store) MarkReplayed(ctx context.Context, eventID, consumer string, at time.Time) (transitioned bool, err error) {
	if err := s.ready(); err != nil {
		return false, err
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	res, err := s.failed.UpdateOne(ctx,
		bson.D{{Key: "event_id", Value: eventID}, {Key: "consumer", Value: consumer}, {Key: "status", Value: StatusParked}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: StatusReplayed}, {Key: "replayed_at", Value: at}}}})
	if err != nil {
		return false, fmt.Errorf("mongostore: mark replayed: %w", err)
	}
	if res.ModifiedCount > 0 {
		return true, nil
	}
	n, err := s.failed.CountDocuments(ctx, bson.D{{Key: "event_id", Value: eventID}, {Key: "consumer", Value: consumer}})
	if err != nil {
		return false, fmt.Errorf("mongostore: mark replayed: %w", err)
	}
	if n == 0 {
		return false, ErrNotFound
	}
	return false, nil
}

// FailedFilter narrows ListFailedEvents; zero fields match everything.
type FailedFilter struct {
	Status    Status
	Consumer  string
	EventID   string
	EventType string
	Topic     string
}

// DefaultPageSize and MaxPageSize bound ListFailedEvents.
const (
	DefaultPageSize = 50
	MaxPageSize     = 500
)

// FailedPage is one page of results. NextCursor is empty on the last page.
type FailedPage struct {
	Items      []FailedEvent
	NextCursor string
}

// ListFailedEvents lists failed events newest-created first, without
// payloads. Pagination is keyset on the immutable document id, so pages stay
// stable while events are parked, replayed or re-parked meanwhile (no item is
// skipped or repeated by changes to last_failed_at or status). Pass the
// previous page's NextCursor as cursor; "" starts at the newest.
func (s *Store) ListFailedEvents(ctx context.Context, f FailedFilter, cursor string, limit int) (*FailedPage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	limit = min(limit, MaxPageSize)
	filter := bson.D{}
	add := func(k string, v any, ok bool) {
		if ok {
			filter = append(filter, bson.E{Key: k, Value: v})
		}
	}
	add("status", f.Status, f.Status != "")
	add("consumer", f.Consumer, f.Consumer != "")
	add("event_id", f.EventID, f.EventID != "")
	add("event_type", f.EventType, f.EventType != "")
	add("topic", f.Topic, f.Topic != "")
	if cursor != "" {
		oid, err := bson.ObjectIDFromHex(cursor)
		if err != nil {
			return nil, fmt.Errorf("%w: bad cursor", ErrInvalid)
		}
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: oid}}})
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	cur, err := s.failed.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetLimit(int64(limit+1)).
		SetProjection(bson.D{{Key: "payload", Value: 0}}))
	if err != nil {
		return nil, fmt.Errorf("mongostore: list failed events: %w", err)
	}
	var docs []failedDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("mongostore: list failed events: %w", err)
	}
	page := &FailedPage{Items: make([]FailedEvent, 0, min(len(docs), limit))}
	for i, d := range docs {
		if i == limit {
			page.NextCursor = docs[limit-1].OID.Hex()
			break
		}
		page.Items = append(page.Items, d.toEvent())
	}
	return page, nil
}

// FailedSummary is the failed-event backlog at a glance (RFC §9.1's "Failed
// Events" metric): counts by lifecycle state, consumer and event type, and
// how long the oldest event has waited.
type FailedSummary struct {
	Total    int64
	Parked   int64
	Replayed int64
	// ByConsumer and ByEventType count parked events only: the backlog.
	ByConsumer  map[string]int64
	ByEventType map[string]int64
	// OldestParkedAt is when the longest-waiting parked event first failed;
	// nil when nothing is parked.
	OldestParkedAt *time.Time
}

// SummarizeFailed aggregates failed_events in one pass. It reads only
// status, consumer, event type and first_failed_at, never payloads.
func (s *Store) SummarizeFailed(ctx context.Context) (*FailedSummary, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	cur, err := s.failed.Aggregate(ctx, bson.A{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "status", Value: "$status"}, {Key: "consumer", Value: "$consumer"}, {Key: "event_type", Value: "$event_type"}}},
			{Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "oldest", Value: bson.D{{Key: "$min", Value: "$first_failed_at"}}},
		}}},
	})
	if err != nil {
		return nil, fmt.Errorf("mongostore: summarize failed events: %w", err)
	}
	var groups []struct {
		ID struct {
			Status    Status `bson:"status"`
			Consumer  string `bson:"consumer"`
			EventType string `bson:"event_type"`
		} `bson:"_id"`
		N      int64     `bson:"n"`
		Oldest time.Time `bson:"oldest"`
	}
	if err := cur.All(ctx, &groups); err != nil {
		return nil, fmt.Errorf("mongostore: summarize failed events: %w", err)
	}
	sum := &FailedSummary{ByConsumer: map[string]int64{}, ByEventType: map[string]int64{}}
	for _, g := range groups {
		sum.Total += g.N
		if g.ID.Status != StatusParked {
			sum.Replayed += g.N
			continue
		}
		sum.Parked += g.N
		sum.ByConsumer[g.ID.Consumer] += g.N
		sum.ByEventType[g.ID.EventType] += g.N
		if sum.OldestParkedAt == nil || g.Oldest.Before(*sum.OldestParkedAt) {
			o := g.Oldest
			sum.OldestParkedAt = &o
		}
	}
	return sum, nil
}

// CountFailedSince counts failed events whose last failure is at or after
// since, optionally restricted to a status ("" counts all). It feeds the
// "Failed Events / Workflow Issues" metric (RFC §9.1).
func (s *Store) CountFailedSince(ctx context.Context, since time.Time, status Status) (int64, error) {
	if err := s.ready(); err != nil {
		return 0, err
	}
	filter := bson.D{{Key: "last_failed_at", Value: bson.D{{Key: "$gte", Value: since}}}}
	if status != "" {
		filter = append(bson.D{{Key: "status", Value: status}}, filter...)
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	n, err := s.failed.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("mongostore: count failed events: %w", err)
	}
	return n, nil
}
