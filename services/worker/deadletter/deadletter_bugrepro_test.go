//go:build integration && bugrepro

// Reproducers for behaviour defects found by the second adversarial pass
// (docs/plans/week-3-findings-behaviour.md). They fail today by design.
//
//	cd services/worker && go test -tags='integration bugrepro' -run Bugrepro ./deadletter/
package deadletter_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// B-01: the transport parks an undecodable record as a stand-in event whose
// payload is {"undecodable_record": {...}} and whose event type comes from the
// record's headers. Replaying it runs the consumer's real handler on that
// stand-in: the handler's lenient JSON decode succeeds with zero values, the
// effect is applied (a mirror row for the nil user id), the marker is written
// and the record is marked replayed. A record that could never be decoded must
// not be reported as successfully replayed.
func TestBugrepro_ReplayOfUndecodableRecord_MustNotApplyZeroValueGarbage(t *testing.T) {
	f := newFixture(t)
	stub := relay.DecodedEvent{
		EventID: uuid.New(), EventType: "identity.user_profile_updated", AggregateID: uuid.New(),
		Payload: json.RawMessage(`{"undecodable_record":{"topic":"identity.events","partition":0,"offset":7,"value_base64":"AAAA"}}`),
	}
	f.park(stub, mirrorConsumer, "decode: bad frame")
	res, err := f.replayer(course.Handlers()...).Replay(context.Background(), stub.EventID.String())
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	f.pg.DB.Raw(`SELECT count(*) FROM instructor_mirror WHERE user_id = ?`, uuid.Nil).Scan(&n)
	if res.Replayed() || n != 0 {
		t.Fatalf("replay of an undecodable stand-in reported %+v and wrote %d garbage mirror rows for the nil user; want a failed replay and no effect", res.Outcomes, n)
	}
}
