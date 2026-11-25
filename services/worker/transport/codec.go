package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

// Record headers the producer sets next to the framed envelope. event_id and
// event_type duplicate envelope fields so a tool can route or dedupe without
// decoding Protobuf; outbox_id has no envelope field and carries the outbox row
// id the handlers use to guard against out-of-order upserts (analytics and
// course mirrors compare it with their last_event_id).
const (
	HeaderEventID   = "event_id"
	HeaderEventType = "event_type"
	HeaderOutboxID  = "outbox_id"
)

// Schemas is what encoding needs from the registry, read once at start.
type Schemas struct {
	// EnvelopeIDs maps a topic to the registry id of its envelope schema
	// (subject "<topic>-value"), written in the frame header.
	EnvelopeIDs map[string]uint32
	// PayloadVersions maps an event type to the registry version of its payload
	// subject, written in Envelope.schema_version (libs/events README).
	PayloadVersions map[string]int32
}

// Codec encodes outbox rows to Kafka messages. Decoding needs no registry state
// (the frame's schema id is carried but not required to decode), so Decode is a
// plain function.
type Codec struct {
	schemas Schemas
}

// NewCodec returns a Codec over schemas.
func NewCodec(s Schemas) *Codec { return &Codec{schemas: s} }

// ErrUnencodable marks a row that cannot become a message however often it is
// retried: an unknown event type or a payload that does not match its schema.
var ErrUnencodable = errors.New("transport: event cannot be encoded")

// Encode turns an outbox row into the message the relay produces: the topic by
// event-type prefix, the aggregate id as the partition key (per-aggregate order,
// RFC §6.5), and the registry-framed Envelope as the value. The row's JSON
// payload is parsed strictly into the Protobuf message the event type names; an
// unknown event type, a field the schema does not have or a mistyped value is an
// ErrUnencodable error.
func (c *Codec) Encode(evt relay.DecodedEvent) (kafka.Message, error) {
	t, ok := events.Lookup(evt.EventType)
	if !ok {
		return kafka.Message{}, fmt.Errorf("%w: unknown event type %q", ErrUnencodable, evt.EventType)
	}
	topic, err := TopicFor(evt.EventType)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("%w: %w", ErrUnencodable, err)
	}
	if topic != t.Topic {
		// The catalog and the routing table are tested to agree; this guards a
		// future edit to one of them.
		return kafka.Message{}, fmt.Errorf("%w: %s routes to %s but the catalog says %s", ErrUnencodable, evt.EventType, topic, t.Topic)
	}
	schemaID, ok := c.schemas.EnvelopeIDs[topic]
	if !ok {
		return kafka.Message{}, fmt.Errorf("transport: no envelope schema id loaded for topic %s", topic)
	}
	version, ok := c.schemas.PayloadVersions[evt.EventType]
	if !ok {
		return kafka.Message{}, fmt.Errorf("transport: no schema version loaded for %s", evt.EventType)
	}

	payload := t.New()
	if err := (protojson.UnmarshalOptions{}).Unmarshal(evt.Payload, payload); err != nil {
		return kafka.Message{}, fmt.Errorf("%w: payload of %s does not match %s: %w", ErrUnencodable, evt.EventType, t.Subject(), err)
	}
	meta := &events.Envelope{
		EventId:       evt.EventID.String(),
		SchemaVersion: version,
		AggregateType: evt.AggregateType,
		AggregateId:   evt.AggregateID.String(),
		OccurredAt:    timestamppb.New(evt.OccurredAt),
		TraceId:       evt.TraceID,
		SpanId:        evt.SpanID,
	}
	if evt.ActorID != nil {
		meta.ActorId = evt.ActorID.String()
	}
	env, err := events.NewEnvelope(t, meta, payload)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("%w: %w", ErrUnencodable, err)
	}
	value, err := events.Encode(schemaID, env)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("%w: %w", ErrUnencodable, err)
	}
	return kafka.Message{
		Topic: topic,
		Key:   []byte(evt.AggregateID.String()),
		Value: value,
		Headers: []kafka.Header{
			{Key: HeaderEventID, Value: []byte(evt.EventID.String())},
			{Key: HeaderEventType, Value: []byte(evt.EventType)},
			{Key: HeaderOutboxID, Value: []byte(strconv.FormatInt(evt.OutboxID, 10))},
		},
	}, nil
}

// Decode turns a consumed record back into the relay.DecodedEvent the handlers
// take, so the same handlers run under both transports. The payload becomes JSON
// with the proto field names (snake_case), the keys the outbox payloads use.
//
// Differences from the in-process event, all harmless to the handlers in this
// repository but worth knowing: SchemaVersion is the registry version of the
// payload subject, not the outbox row's schema_version; Attempts is 0; fields
// the producer left at their zero value are absent from the JSON; timestamps are
// UTC. A record with no outbox_id header (written by something other than the
// relay) decodes with OutboxID 0, which the mirror handlers' last_event_id guard
// treats as older than anything already applied.
func Decode(rec kafka.Record) (relay.DecodedEvent, error) {
	evt, _, err := decodeFramed(rec)
	return evt, err
}

// SourceOf is the relay.Source of rec: its Kafka coordinates and key, and the
// registry schema id of its frame (the envelope schema of the topic). The
// runner attaches it to the context of every handler call so a handler can
// record where the event came from (the audit archiver does).
func SourceOf(rec kafka.Record, schemaID uint32) relay.Source {
	return relay.Source{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset, Key: string(rec.Key), SchemaID: schemaID}
}

// decodeFramed is Decode that also returns the schema id from the frame header.
func decodeFramed(rec kafka.Record) (relay.DecodedEvent, uint32, error) {
	var env events.Envelope
	schemaID, err := events.DecodeInto(rec.Value, &env)
	if err != nil {
		return relay.DecodedEvent{}, 0, fmt.Errorf("transport: %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
	}
	evt, err := decodeEnvelope(rec, &env)
	return evt, schemaID, err
}

func decodeEnvelope(rec kafka.Record, env *events.Envelope) (relay.DecodedEvent, error) {
	payload, err := events.OpenPayload(env)
	if err != nil {
		return relay.DecodedEvent{}, fmt.Errorf("transport: %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
	}
	js, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(payload)
	if err != nil {
		return relay.DecodedEvent{}, fmt.Errorf("transport: marshal payload to JSON: %w", err)
	}

	eventID, err := uuid.Parse(env.GetEventId())
	if err != nil {
		return relay.DecodedEvent{}, fmt.Errorf("transport: envelope event_id %q: %w", env.GetEventId(), err)
	}
	aggregateID, err := uuid.Parse(env.GetAggregateId())
	if err != nil {
		return relay.DecodedEvent{}, fmt.Errorf("transport: envelope aggregate_id %q: %w", env.GetAggregateId(), err)
	}
	evt := relay.DecodedEvent{
		EventID:       eventID,
		AggregateType: env.GetAggregateType(),
		AggregateID:   aggregateID,
		EventType:     env.GetEventType(),
		SchemaVersion: int(env.GetSchemaVersion()),
		Payload:       json.RawMessage(js),
		OccurredAt:    env.GetOccurredAt().AsTime(),
		TraceID:       env.GetTraceId(),
		SpanID:        env.GetSpanId(),
	}
	if a := env.GetActorId(); a != "" {
		id, err := uuid.Parse(a)
		if err != nil {
			return relay.DecodedEvent{}, fmt.Errorf("transport: envelope actor_id %q: %w", a, err)
		}
		evt.ActorID = &id
	}
	if v, ok := rec.Header(HeaderOutboxID); ok {
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return relay.DecodedEvent{}, fmt.Errorf("transport: header %s %q: %w", HeaderOutboxID, v, err)
		}
		evt.OutboxID = n
	}
	return evt, nil
}
