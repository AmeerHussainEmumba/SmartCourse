package eventbus_test

import (
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka/topics"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
)

func recordOf(msg kafka.Message) kafka.Record {
	return kafka.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value, Headers: msg.Headers, Partition: 2, Offset: 41}
}

// The audit service stores a parked event as the consumer decoded it, plus the
// frame's schema id, and republishes it from that. The record it builds must
// decode to the same event for every event type, and carry the framing a normal
// record has.
func TestEncodeForRetry_DecodesBackToTheSameEvent(t *testing.T) {
	codec := eventbus.NewCodec(testSchemas(t))
	agg := uuid.New()
	for eventType, payload := range samplePayloads(agg) {
		t.Run(eventType, func(t *testing.T) {
			// What a consumer saw, and what the sink stored.
			orig, err := codec.Encode(rowFor(t, eventType, payload, agg))
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			stored, err := eventbus.Decode(recordOf(orig))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			var env events.Envelope
			schemaID, err := events.DecodeInto(orig.Value, &env)
			if err != nil {
				t.Fatal(err)
			}

			retry, err := eventbus.EncodeForRetry("some-consumer", schemaID, stored)
			if err != nil {
				t.Fatalf("EncodeForRetry: %v", err)
			}
			if retry.Topic != topics.Retry("some-consumer") {
				t.Errorf("topic = %q, want %q", retry.Topic, topics.Retry("some-consumer"))
			}
			if string(retry.Key) != agg.String() {
				t.Errorf("key = %q, want the aggregate id %s", retry.Key, agg)
			}
			again, err := eventbus.Decode(recordOf(retry))
			if err != nil {
				t.Fatalf("Decode of the retry record: %v", err)
			}
			if !reflect.DeepEqual(stored, again) {
				t.Fatalf("retry record decodes to a different event:\n stored %+v\n  again %+v", stored, again)
			}
			// The frame and headers are what the producer writes.
			var env2 events.Envelope
			gotID, err := events.DecodeInto(retry.Value, &env2)
			if err != nil || gotID != schemaID {
				t.Errorf("frame schema id = %d (%v), want %d", gotID, err, schemaID)
			}
			h := map[string]string{}
			for _, hd := range retry.Headers {
				h[hd.Key] = string(hd.Value)
			}
			if h[eventbus.HeaderEventID] != stored.EventID.String() || h[eventbus.HeaderEventType] != eventType ||
				h[eventbus.HeaderOutboxID] != strconv.FormatInt(stored.OutboxID, 10) {
				t.Errorf("headers = %v", h)
			}
		})
	}
}

func TestEncodeForRetry_RefusesWhatCannotBeFramed(t *testing.T) {
	codec := eventbus.NewCodec(testSchemas(t))
	agg := uuid.New()
	msg, err := codec.Encode(rowFor(t, "course.course_archived", samplePayloads(agg)["course.course_archived"], agg))
	if err != nil {
		t.Fatal(err)
	}
	good, _ := eventbus.Decode(recordOf(msg))
	var env events.Envelope
	schemaID, _ := events.DecodeInto(msg.Value, &env)

	bad := map[string]func() (kafka.Message, error){
		"no consumer":  func() (kafka.Message, error) { return eventbus.EncodeForRetry("", schemaID, good) },
		"no schema id": func() (kafka.Message, error) { return eventbus.EncodeForRetry("c", 0, good) },
		"no schema version": func() (kafka.Message, error) {
			e := good
			e.SchemaVersion = 0
			return eventbus.EncodeForRetry("c", schemaID, e)
		},
		"unknown event type": func() (kafka.Message, error) {
			e := good
			e.EventType = "course.never_heard_of_it"
			return eventbus.EncodeForRetry("c", schemaID, e)
		},
		"payload does not match": func() (kafka.Message, error) {
			e := good
			e.Payload = []byte(`{"not_a_field": 1}`)
			return eventbus.EncodeForRetry("c", schemaID, e)
		},
		"undecodable stub": func() (kafka.Message, error) {
			e := relay.DecodedEvent{EventID: uuid.New(), Payload: []byte(`{"undecodable_record":{}}`)}
			return eventbus.EncodeForRetry("c", schemaID, e)
		},
	}
	for name, fn := range bad {
		if _, err := fn(); !errors.Is(err, eventbus.ErrUnencodable) {
			t.Errorf("%s: want ErrUnencodable, got %v", name, err)
		}
	}
}
