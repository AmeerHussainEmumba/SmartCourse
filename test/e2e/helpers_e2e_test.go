//go:build e2e

package e2e_test

import (
	"encoding/json"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// decodeAccessToken reads access_token from a login or refresh response.
func decodeAccessToken(t *testing.T, raw []byte) string {
	t.Helper()
	var r struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &r); err != nil || r.AccessToken == "" {
		t.Fatalf("no access_token in %s (err %v)", raw, err)
	}
	return r.AccessToken
}

// countEvent is how many of the records are the event with this id (a retry
// topic or a source topic can hold copies of one event).
func countEvent(recs []harness.KRecord, eventID string) int {
	n := 0
	for _, r := range recs {
		if r.EventID() == eventID {
			n++
		}
	}
	return n
}
