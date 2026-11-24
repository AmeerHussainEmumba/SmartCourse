//go:build integration

// GET /analytics/issues (FR-5, "Failed Events / Workflow Issues"): the route
// shape, admin-only access and the 503-when-unwired behaviour, against fake
// FailedEventsSource/WorkflowIssuesSource. The real dead-letter store is not
// reachable from this module (issues.go), so there is no test here that
// moves the metric from real source data; that belongs with whoever supplies
// the real sources.
package analytics_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

func tokenFor(t *testing.T, id uuid.UUID, role string) string {
	t.Helper()
	tok, err := auth.IssueAccessToken(testJWTCfg(), id, role, 1)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

type fakeFailedEvents struct {
	sum *analytics.FailedEventsSummary
	err error
}

func (f *fakeFailedEvents) SummarizeFailedEvents(context.Context) (*analytics.FailedEventsSummary, error) {
	return f.sum, f.err
}

type fakeWorkflowIssues struct {
	sum *analytics.WorkflowIssuesSummary
	err error
}

func (f *fakeWorkflowIssues) SummarizeWorkflowIssues(context.Context) (*analytics.WorkflowIssuesSummary, error) {
	return f.sum, f.err
}

func issuesEnv(t *testing.T, fe analytics.FailedEventsSource, wf analytics.WorkflowIssuesSource) *secEnv {
	env := testenv.Setup(t)
	svc := analytics.NewService(analytics.NewRepository(env.DB))
	if fe != nil || wf != nil {
		svc = svc.WithIssueSources(fe, wf)
	}
	return &secEnv{t: t, env: env, r: newTestEngine(t, svc)}
}

const issuesPath = "/api/v1/analytics/issues"

func TestIssues_AdminOnly_AndReflectsSources(t *testing.T) {
	fe := &fakeFailedEvents{sum: &analytics.FailedEventsSummary{Total: 5, Unresolved: 3, ByEventType: map[string]int64{"enrollment.student_enrolled": 5}}}
	wf := &fakeWorkflowIssues{sum: &analytics.WorkflowIssuesSummary{Failed: 2, ByWorkflow: map[string]int64{"publish_course": 2}}}
	s := issuesEnv(t, fe, wf)

	if rec := s.get(issuesPath, ""); rec.Code != 401 {
		t.Errorf("anonymous: want 401, got %d", rec.Code)
	}
	for _, role := range []string{"student", "instructor"} {
		if rec := s.get(issuesPath, s.token(role)); rec.Code != 403 {
			t.Errorf("%s: want 403, got %d", role, rec.Code)
		}
	}
	rec := s.get(issuesPath, s.token("admin"))
	if rec.Code != 200 {
		t.Fatalf("admin: want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		FailedEvents struct {
			Total       int64            `json:"total"`
			Unresolved  int64            `json:"unresolved"`
			ByEventType map[string]int64 `json:"by_event_type"`
		} `json:"failed_events"`
		WorkflowIssues struct {
			Failed     int64            `json:"failed"`
			ByWorkflow map[string]int64 `json:"by_workflow"`
		} `json:"workflow_issues"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.FailedEvents.Total != 5 || body.FailedEvents.Unresolved != 3 || body.FailedEvents.ByEventType["enrollment.student_enrolled"] != 5 ||
		body.WorkflowIssues.Failed != 2 || body.WorkflowIssues.ByWorkflow["publish_course"] != 2 {
		t.Fatalf("response does not reflect the sources: %s", rec.Body.String())
	}

	// The metric moves when the source data does.
	fe.sum = &analytics.FailedEventsSummary{Total: 6, Unresolved: 4}
	rec = s.get(issuesPath, s.token("admin"))
	if rec.Code != 200 || !json.Valid(rec.Body.Bytes()) {
		t.Fatalf("after source change: %d %s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.FailedEvents.Total != 6 || body.FailedEvents.Unresolved != 4 {
		t.Fatalf("metric did not move with the source: %v %s", err, rec.Body.String())
	}
}

func TestIssues_NilMaps_SerialiseAsEmptyObjects(t *testing.T) {
	s := issuesEnv(t, &fakeFailedEvents{sum: &analytics.FailedEventsSummary{}}, &fakeWorkflowIssues{sum: &analytics.WorkflowIssuesSummary{}})
	rec := s.get(issuesPath, s.token("admin"))
	var raw map[string]map[string]json.RawMessage
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &raw) != nil {
		t.Fatalf("want 200 JSON, got %d %s", rec.Code, rec.Body.String())
	}
	if string(raw["failed_events"]["by_event_type"]) != "{}" || string(raw["workflow_issues"]["by_workflow"]) != "{}" {
		t.Fatalf("absent maps must be {}, got %s", rec.Body.String())
	}
}

func TestIssues_SourcesNotWired_Is503NotAZero(t *testing.T) {
	s := issuesEnv(t, nil, nil)
	if rec := s.get(issuesPath, s.token("admin")); rec.Code != 503 {
		t.Errorf("unwired sources: want 503, got %d: %s", rec.Code, rec.Body.String())
	}
	// Non-admins are still refused before anything about the wiring is revealed.
	if rec := s.get(issuesPath, s.token("student")); rec.Code != 403 {
		t.Errorf("student with unwired sources: want 403, got %d", rec.Code)
	}
}

func TestIssues_SourceFailure_Is500WithoutLeakingTheCause(t *testing.T) {
	s := issuesEnv(t, &fakeFailedEvents{err: errors.New("mongo: dial tcp 10.0.0.5:27017 refused")}, &fakeWorkflowIssues{sum: &analytics.WorkflowIssuesSummary{}})
	rec := s.get(issuesPath, s.token("admin"))
	if rec.Code != 500 {
		t.Fatalf("want 500, got %d", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(body, "10.0.0.5") || strings.Contains(body, "mongo") {
		t.Errorf("error body leaks the source error: %s", body)
	}
}
