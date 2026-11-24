package harness

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

// FailedEventRecord is one failed-events record as the worker admin API lists
// it (GET /api/v1/admin/failed-events).
type FailedEventRecord struct {
	EventID   string `json:"event_id"`
	Consumer  string `json:"consumer"`
	EventType string `json:"event_type"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	LastError string `json:"last_error"`
}

// ListFailedEvents lists parked and replayed failed events through the worker
// admin API with an admin token, optionally filtered by status ("parked",
// "replayed" or "").
func (s *Stack) ListFailedEvents(t testing.TB, status string) []FailedEventRecord {
	t.Helper()
	url := s.WorkerAdminURL + "/api/v1/admin/failed-events?limit=100"
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

// ReplayOutcome is one consumer's result in a replay response.
type ReplayOutcome struct {
	Consumer string `json:"consumer"`
	Status   string `json:"status"`
	Applied  bool   `json:"applied"`
	Error    string `json:"error"`
}

// ReplayResult is the body of POST .../failed-events/{event_id}/replay.
type ReplayResult struct {
	EventID  string          `json:"event_id"`
	Replayed bool            `json:"replayed"`
	Outcomes []ReplayOutcome `json:"outcomes"`
}

// ReplayFailedEvent replays an event by id through the worker admin API and
// returns the HTTP status with the parsed body (the body of a 422 is the error
// document, not a ReplayResult: result is nil then).
func (s *Stack) ReplayFailedEvent(t testing.TB, eventID string) (int, *ReplayResult, []byte) {
	t.Helper()
	code, raw, err := Do("POST", s.WorkerAdminURL+"/api/v1/admin/failed-events/"+eventID+"/replay", s.AdminToken(t), nil)
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

// WorkerHealth reads the worker admin /health/ready.
func (s *Stack) WorkerHealth() (int, string, error) {
	code, raw, err := Do("GET", s.WorkerAdminURL+"/health/ready", "", nil)
	if err != nil {
		return 0, "", err
	}
	return code, string(raw), nil
}

// WorkerMetrics scrapes the worker's /metrics listener.
func (s *Stack) WorkerMetrics() (Metrics, error) {
	return ScrapeMetrics(s.WorkerMetricsURL + "/metrics")
}

// CourseMetrics scrapes course-api's /metrics.
func (s *Stack) CourseMetrics() (Metrics, error) { return ScrapeMetrics(s.CourseURL + "/metrics") }

// CheckReady is nil when the URL's /health/ready answers 200.
func CheckReady(baseURL string) error {
	code, raw, err := Do("GET", baseURL+"/health/ready", "", nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("GET %s/health/ready: HTTP %d %s", baseURL, code, raw)
	}
	return nil
}
