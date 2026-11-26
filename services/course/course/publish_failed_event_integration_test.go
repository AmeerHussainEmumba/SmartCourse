//go:build integration

// What this file checks, against real Postgres: course.publish_failed is
// written in the transaction that moves a version to publish_failed, once per
// failure. A retried failure step, a reconciler pass that loses to a
// completion, and a successful publish write none; a retry-then-fail-again is
// a second failure and writes a second event. The reason is bounded.
package course_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

func publishFailedEvents(t *testing.T, env *testenv.Env, courseID uuid.UUID) []course.PublishFailedPayload {
	t.Helper()
	rows, err := env.DB.Raw(`SELECT payload FROM outbox WHERE event_type = ? AND aggregate_id = ? ORDER BY id`,
		course.EventPublishFailed, courseID).Rows()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []course.PublishFailedPayload
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var p course.PublishFailedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func TestMarkPublishFailed_WritesPublishFailedWithTheTransition(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, draft := newCourse(t, svc, instr, "pf-basic", "Failing Course")

	if err := svc.MarkPublishFailed(ctx, draft, "asset not found", "VerifyAssets"); err != nil {
		t.Fatal(err)
	}
	evs := publishFailedEvents(t, env, courseID)
	if len(evs) != 1 {
		t.Fatalf("publish_failed events: %d, want 1", len(evs))
	}
	p := evs[0]
	if p.CourseID != courseID || p.VersionID != draft || p.VersionNumber != 1 || p.Title != "Failing Course" ||
		p.InstructorID != instr || p.FailedActivity != "VerifyAssets" || p.Reason != "asset not found" || p.FailedAt.IsZero() {
		t.Errorf("payload = %+v", p)
	}
	var aggregateType string
	_ = env.DB.Raw(`SELECT aggregate_type FROM outbox WHERE event_type = ?`, course.EventPublishFailed).Row().Scan(&aggregateType)
	if aggregateType != "course" {
		t.Errorf("aggregate_type = %q, want course", aggregateType)
	}

	// Temporal retries the failure step (retry5x): a repeat after the first
	// attempt committed must not emit again, however many times it runs.
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := svc.MarkPublishFailed(ctx, draft, "asset not found", "VerifyAssets"); err != nil {
				t.Errorf("repeat: %v", err)
			}
		}()
	}
	wg.Wait()
	if n := len(publishFailedEvents(t, env, courseID)); n != 1 {
		t.Fatalf("publish_failed events after repeated and concurrent failure steps: %d, want 1", n)
	}
}

func TestMarkPublishFailed_ASecondFailureAfterARetryIsASecondEvent(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, draft := newCourse(t, svc, instr, "pf-twice", "Twice")

	if err := svc.MarkPublishFailed(ctx, draft, "first", "ValidateMetadata"); err != nil {
		t.Fatal(err)
	}
	// The instructor publishes again (publish_failed -> publishing) and it fails again.
	if err := env.DB.Exec(`UPDATE course_versions SET state = 'publishing' WHERE id = ?`, draft).Error; err != nil {
		t.Fatal(err)
	}
	if err := svc.MarkPublishFailed(ctx, draft, "second", "BuildOutline"); err != nil {
		t.Fatal(err)
	}
	evs := publishFailedEvents(t, env, courseID)
	if len(evs) != 2 || evs[0].Reason != "first" || evs[1].Reason != "second" || evs[1].FailedActivity != "BuildOutline" {
		t.Fatalf("events = %+v, want one per failure", evs)
	}
}

func TestMarkPublishFailed_ReasonAndActivityAreBounded(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	courseID, draft := newCourse(t, svc, instr, "pf-long", "Long")

	long := strings.Repeat("é", 5000) + string([]byte{0xff}) + "tail"
	if err := svc.MarkPublishFailed(context.Background(), draft, long, strings.Repeat("A", 1000)); err != nil {
		t.Fatal(err)
	}
	evs := publishFailedEvents(t, env, courseID)
	if len(evs) != 1 {
		t.Fatalf("events: %d", len(evs))
	}
	if n := len([]rune(evs[0].Reason)); n != 500 {
		t.Errorf("reason has %d characters, want it cut to 500", n)
	}
	if n := len(evs[0].FailedActivity); n != 100 {
		t.Errorf("failed_activity has %d characters, want 100", n)
	}
}

func TestReconcile_ReleaseWritesPublishFailedOnce(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	fake := &reconcileFake{describe: func(string) (enumspb.WorkflowExecutionStatus, error) {
		return enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	courseID, vid := publishingVersion(t, env, svc, instr, "pf-recon")

	// Two replicas reconcile at once, then a third pass: one failure, one event.
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.ReconcileStalePublishes(context.Background()); err != nil {
				t.Errorf("reconcile: %v", err)
			}
		}()
	}
	wg.Wait()
	if _, err := svc.ReconcileStalePublishes(context.Background()); err != nil {
		t.Fatal(err)
	}
	evs := publishFailedEvents(t, env, courseID)
	if len(evs) != 1 {
		t.Fatalf("publish_failed events after reconciliation: %d, want 1", len(evs))
	}
	if evs[0].VersionID != vid || evs[0].FailedActivity != course.FailedActivityReconciler || !strings.Contains(strings.ToLower(evs[0].Reason), "terminated") {
		t.Errorf("payload = %+v", evs[0])
	}
}

func TestReconcile_LosingToACompletionWritesNoPublishFailed(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	var svc *course.Service
	var courseID, vid uuid.UUID
	fake := &reconcileFake{}
	fake.describe = func(string) (enumspb.WorkflowExecutionStatus, error) {
		if err := svc.PromoteAndPublish(context.Background(), courseID, vid, nil, "Race"); err != nil {
			t.Errorf("promote: %v", err)
		}
		return enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil
	}
	svc = course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	courseID, vid = publishingVersion(t, env, svc, instr, "pf-race")
	if _, err := svc.ReconcileStalePublishes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(publishFailedEvents(t, env, courseID)); n != 0 {
		t.Fatalf("publish_failed events for a publish that completed: %d, want 0", n)
	}
}

func TestPublishSuccess_WritesNoPublishFailed(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	courseID, _ := newCourse(t, svc, instr, "pf-ok", "Fine")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)
	if n := len(publishFailedEvents(t, env, courseID)); n != 0 {
		t.Fatalf("publish_failed events for a successful publish: %d", n)
	}
}
