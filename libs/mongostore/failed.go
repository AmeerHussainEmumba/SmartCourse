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
	ID            string       `bson:"-"`
	EventID       string       `bson:"event_id"`
	Consumer      string       `bson:"consumer"`
	EventType     string       `bson:"event_type"`
	Topic         string       `bson:"topic,omitempty"`
	AggregateType string       `bson:"aggregate_type,omitempty"`
	AggregateID   string       `bson:"aggregate_id,omitempty"`
	Payload       []byte       `bson:"payload,omitempty"`
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
	Attempts      int
	Err           string
	FailedAt      time.Time
}

// ParkFailedEvent records that consumer gave up on an event. It is idempotent
// per (EventID, Consumer): the first call creates the document; later calls
// append to its error history and add to attempts instead of duplicating.
// Safe under concurrency. The payload is stored from the first call only.
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
