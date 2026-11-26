// Package eventstest is the producer payload-contract check. A producing
// service writes its event payloads as JSON through libs/outbox and
// deliberately does not import this module (so its API binary does not link
// Protobuf); the JSON field names are the proto field names, and the relay's
// codec rejects a payload that does not fit. This package lets a producing
// service's TEST prove the two stay in step, so a renamed or added field
// fails in that service's test run and not at the first relay publish.
// Production code must not import it (it imports "testing").
package eventstest

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
)

// Check reports why payload (the JSON a producer wrote to the outbox) does not
// conform to the contract of eventType, or nil when it does. It requires:
//
//   - eventType is in events.Types();
//   - the payload is a JSON object whose keys are exactly the proto field
//     names (snake_case, as declared), no more and no fewer; protojson alone
//     would also accept camelCase and a missing field, so it is not enough;
//   - protojson accepts it strictly: no unknown field and every value of the
//     right JSON type (UUIDs and timestamps as strings, integers as numbers).
func Check(eventType string, payload []byte) error {
	typ, ok := events.Lookup(eventType)
	if !ok {
		return fmt.Errorf("event type %q is not in the libs/events catalogue", eventType)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(payload, &keys); err != nil {
		return fmt.Errorf("%s: payload is not a JSON object: %w", eventType, err)
	}
	fields := typ.Message().Fields()
	want := make(map[string]bool, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		want[string(fields.Get(i).Name())] = true
	}
	var missing, extra []string
	for name := range want {
		if _, ok := keys[name]; !ok {
			missing = append(missing, name)
		}
	}
	for name := range keys {
		if !want[name] {
			extra = append(extra, name)
		}
	}
	if len(missing)+len(extra) > 0 {
		sort.Strings(missing)
		sort.Strings(extra)
		return fmt.Errorf("%s (%s): payload fields differ from the proto contract: missing %v, not in the contract %v",
			eventType, typ.Message().FullName(), missing, extra)
	}
	msg := typ.New()
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payload, msg); err != nil {
		return fmt.Errorf("%s: protojson rejects the payload: %w", eventType, err)
	}
	return nil
}

// RequireConforms fails t if payload does not conform to eventType's contract.
func RequireConforms(t testing.TB, eventType string, payload []byte) {
	t.Helper()
	if err := Check(eventType, payload); err != nil {
		t.Fatalf("payload contract: %v\npayload: %s", err, strings.TrimSpace(string(payload)))
	}
}
