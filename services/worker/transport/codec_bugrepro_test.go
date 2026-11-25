//go:build bugrepro

// Reproducers for behaviour defects found by the second adversarial pass
// (docs/plans/week-3-findings-behaviour.md). They fail today by design.
//
//	cd services/worker && go test -tags=bugrepro -run Bugrepro ./transport/
package transport_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/transport"
)

// B-04: the codec parses the outbox JSON leniently for MISSING fields: a payload
// of {} (or one with null ids) encodes, is produced, and every consumer receives
// an event whose ids are absent, which handlers decode as the nil UUID. The
// poison then surfaces as a garbage effect (B-01 shows what a handler does
// with a nil id) instead of as an unencodable row that is parked at the
// producer. Expected: a payload with none of the event's fields is
// ErrUnencodable for every event type (all of them have at least one id).
func TestBugrepro_Codec_EmptyPayloadIsEncodedForEveryEventType(t *testing.T) {
	codec := transport.NewCodec(testSchemas(t))
	agg := uuid.New()
	for _, et := range events.Types() {
		row := rowFor(t, et.EventType, map[string]any{}, agg)
		msg, err := codec.Encode(row)
		if err == nil {
			got, derr := transport.Decode(kafka.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value, Headers: msg.Headers})
			t.Errorf("%s: an empty payload was encoded and produced (decodes to %s, %v)", et.EventType, got.Payload, derr)
		}
	}
}
