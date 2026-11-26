package harness

import (
	"encoding/json"
	"net/http"
	"testing"
)

// The failed-events surface is audit-api (admin token, fail-closed token
// version check). A consuming service's worker parks an event that exhausts
// its retry budget in MongoDB; audit-api lists it and replays it by
// republishing to the consumer's retry topic.

// FailedEventRecord is one item of GET /api/v1/admin/failed-events.
type FailedEventRecord struct {
	EventID   string `json:"event_id"`
	Consumer  string `json:"consumer"`
	EventType string `json:"event_type"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	LastError string `json:"last_error"`
}

// ListFailedEvents lists parked or replayed records as an admin ("" for both).
func (s *Stack) ListFailedEvents(t testing.TB, status string) []FailedEventRecord {
	t.Helper()
	url := s.AuditURL + "/api/v1/admin/failed-events?limit=100"
	if status != "" {
		url += "&status=" + status
	}
	raw := call(t, http.StatusOK, "GET", url, s.AdminToken(t), nil)
	var r struct {
		Items []FailedEventRecord `json:"items"`
	}
	decode(t, raw, &r)
	return r.Items
}

// ReplayOutcome is what a replay did for one parked record.
type ReplayOutcome struct {
	Consumer   string `json:"consumer"`
	Status     string `json:"status"` // replayed, already_replayed, refused, produce_failed, produced_not_marked
	RetryTopic string `json:"retry_topic"`
	Error      string `json:"error"`
}

// ReplayResult is the 200 body of a replay.
type ReplayResult struct {
	EventID  string          `json:"event_id"`
	Replayed bool            `json:"replayed"`
	Outcomes []ReplayOutcome `json:"outcomes"`
}

// ReplayFailedEvent replays every parked record of the event as an admin:
// audit-api produces it to retry.<consumer> and marks the record replayed. The
// consumer's own group applies it afterwards, asynchronously, so a caller
// waits for the effect. It returns the status, the decoded 200 body (nil for
// any other status) and the raw body.
func (s *Stack) ReplayFailedEvent(t testing.TB, eventID string) (int, *ReplayResult, []byte) {
	t.Helper()
	code, raw, err := Do("POST", s.AuditURL+"/api/v1/admin/failed-events/"+eventID+"/replay", s.AdminToken(t), nil)
	if err != nil {
		t.Fatalf("replay %s: %v", eventID, err)
	}
	if code != http.StatusOK {
		return code, nil, raw
	}
	var r ReplayResult
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("decode replay response %s: %v", raw, err)
	}
	return code, &r, raw
}

// CourseMetrics scrapes course-api's internal /metrics (its outbox gauges).
func (s *Stack) CourseMetrics() (Metrics, error) { return s.Metrics(CourseAPI) }

// CheckReady is GET /health/ready on a public API port.
func CheckReady(baseURL string) error {
	code, raw, err := Do("GET", baseURL+"/health/ready", "", nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return Errf("GET %s/health/ready: HTTP %d %s", baseURL, code, raw)
	}
	return nil
}
