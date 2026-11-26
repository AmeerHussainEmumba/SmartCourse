// Regression test for B-04 (docs/plans/week-3-findings-behaviour.md), fixed: it
// began as a bugrepro reproducer that failed by design. No Docker needed.
//
//	cd libs/eventbus && go test -run Bugrepro .
package eventbus_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/eventbus"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/kafka"
)

// B-04 (was): the codec parsed the outbox JSON leniently for MISSING fields: a payload
// of {} (or one with null ids) encodes, is produced, and every consumer receives
// an event whose ids are absent, which handlers decode as the nil UUID. The
// poison then surfaces as a garbage effect (B-01 shows what a handler does
// with a nil id) instead of as an unencodable row that is parked at the
// producer. Expected: a payload with none of the event's fields is
// ErrUnencodable for every event type (all of them have at least one id).
func TestBugrepro_Codec_EmptyPayloadIsEncodedForEveryEventType(t *testing.T) {
	codec := eventbus.NewCodec(testSchemas(t))
	agg := uuid.New()
	for _, et := range events.Types() {
		row := rowFor(t, et.EventType, map[string]any{}, agg)
		msg, err := codec.Encode(row)
		if err == nil {
			got, derr := eventbus.Decode(kafka.Record{Topic: msg.Topic, Key: msg.Key, Value: msg.Value, Headers: msg.Headers})
			t.Errorf("%s: an empty payload was encoded and produced (decodes to %s, %v)", et.EventType, got.Payload, derr)
		}
	}
}
