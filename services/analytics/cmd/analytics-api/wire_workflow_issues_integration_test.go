//go:build integration

// analytics-api as deployed, Workflow Issues half of GET /analytics/issues
// (DR-026): workflow_issues is an object with counts, fed by real
// course.publish_failed events through the real relay handler, moves when
// another failure arrives, counts a redelivered event once, and is healed by
// the reconciler.
package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

func deliverPublishFailed(t *testing.T, db *gorm.DB, eventID uuid.UUID, outboxID int64) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"course_id": uuid.New(), "version_id": uuid.New(), "version_number": 1, "title": "T",
		"instructor_id": uuid.New(), "failed_activity": "VerifyAssets", "reason": "boom", "failed_at": time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range analytics.Handlers() {
		if !h.EventTypes["course.publish_failed"] {
			continue
		}
		if err := db.Transaction(func(tx *gorm.DB) error {
			return h.Handle(context.Background(), tx, relay.DecodedEvent{
				OutboxID: outboxID, EventID: eventID, EventType: "course.publish_failed", Payload: payload, OccurredAt: time.Now(),
			})
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAnalyticsAPI_Issues_WorkflowIssuesFromPublishFailedEvents(t *testing.T) {
	pg, mongoEnv := testenv.Setup(t), testenv.SetupMongo(t)
	h := newHarness(t, pg, mongoEnv.URI)
	type wfBody struct {
		Failed     int64            `json:"failed"`
		ByWorkflow map[string]int64 `json:"by_workflow"`
	}
	read := func() wfBody {
		t.Helper()
		rec := h.get(issuesPath, "admin")
		if rec.Code != 200 {
			t.Fatalf("issues = %d %s", rec.Code, rec.Body.String())
		}
		var raw struct {
			WorkflowIssues *wfBody `json:"workflow_issues"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
			t.Fatal(err)
		}
		if raw.WorkflowIssues == nil {
			t.Fatalf("workflow_issues is null: %s", rec.Body.String())
		}
		return *raw.WorkflowIssues
	}

	if b := read(); b.Failed != 0 || len(b.ByWorkflow) != 0 {
		t.Fatalf("before any failure: %+v", b)
	}
	e1 := uuid.New()
	deliverPublishFailed(t, pg.DB, e1, 1)
	deliverPublishFailed(t, pg.DB, uuid.New(), 2)
	deliverPublishFailed(t, pg.DB, e1, 1) // redelivery of the first event
	if b := read(); b.Failed != 2 || b.ByWorkflow[analytics.PublishWorkflow] != 2 {
		t.Fatalf("after two failures and a redelivery: %+v, want failed=2", b)
	}

	// The reconciler keeps the count consistent with the mirror.
	if err := pg.DB.Exec(`UPDATE workflow_issue_stats SET failed = 99`).Error; err != nil {
		t.Fatal(err)
	}
	if err := analytics.NewReconciler(pg.DB, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b := read(); b.Failed != 2 {
		t.Fatalf("after reconcile: %+v, want the corrupted 99 healed to 2", b)
	}
}
