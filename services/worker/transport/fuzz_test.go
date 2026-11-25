package transport_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport"
)

// FuzzDecode: whatever bytes and headers a record carries (the broker is a
// trust boundary: anything can produce to it), Decode must return an error or a
// well-formed event, never panic, and a decoded payload must be valid JSON.
func FuzzDecode(f *testing.F) {
	codec := transport.NewCodec(testSchemasFuzz(f))
	agg := uuid.New()
	for _, et := range events.Types() {
		raw, _ := json.Marshal(map[string]any{})
		msg, err := codec.Encode(relay.DecodedEvent{EventID: uuid.New(), AggregateID: agg, EventType: et.EventType, Payload: raw})
		if err == nil {
			f.Add(msg.Value, "1")
		}
	}
	f.Add([]byte{0, 0, 0, 0, 1, 0}, "x")
	f.Add([]byte{}, "")
	f.Fuzz(func(t *testing.T, value []byte, outbox string) {
		rec := kafka.Record{Topic: "enrollment.events", Value: value, Headers: []kafka.Header{{Key: transport.HeaderOutboxID, Value: []byte(outbox)}}}
		evt, err := transport.Decode(rec)
		if err != nil {
			return
		}
		if !json.Valid(evt.Payload) {
			t.Fatalf("decoded payload is not JSON: %q", evt.Payload)
		}
	})
}

// testSchemasFuzz is a hand-built Schemas (the registry stand-in needs a
// *testing.T, which a fuzz target does not have): every topic and event type
// gets a fixed id and version 1.
func testSchemasFuzz(testing.TB) transport.Schemas {
	s := transport.Schemas{EnvelopeIDs: map[string]uint32{}, PayloadVersions: map[string]int32{}}
	for _, et := range events.Types() {
		s.EnvelopeIDs[et.Topic] = 7
		s.PayloadVersions[et.EventType] = 1
	}
	return s
}

// FuzzEncode: an outbox payload of any shape must encode or fail with an error
// (never panic), and what encodes must decode.
func FuzzEncode(f *testing.F) {
	codec := transport.NewCodec(testSchemasFuzz(f))
	f.Add("enrollment.student_enrolled", []byte(`{"enrollment_id":"x","enrolled_at":"2026-01-01T00:00:00Z"}`))
	f.Add("course.course_archived", []byte(`{"course_id":null}`))
	f.Add("progress.lesson_completed", []byte(`{"completed_at":"0000-00-00T00:00:00Z"}`))
	f.Add("identity.user_registered", []byte(`[]`))
	f.Fuzz(func(t *testing.T, eventType string, payload []byte) {
		row := relay.DecodedEvent{EventID: uuid.New(), AggregateID: uuid.New(), EventType: eventType, Payload: payload}
		msg, err := codec.Encode(row)
		if err != nil {
			return
		}
		if _, err := transport.Decode(kafka.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value, Headers: msg.Headers}); err != nil {
			t.Fatalf("encoded %q but cannot decode it: %v", eventType, err)
		}
	})
}
