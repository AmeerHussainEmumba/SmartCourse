//go:build integration

package course_test

import (
	"context"
	"errors"
	"testing"

	temporalclient "go.temporal.io/sdk/client"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// Regression tests for the publish claim: the version is moved to `publishing`
// BEFORE the workflow starts, a second publish gets 409, and the workflow's own
// state writes can never be overwritten by a late "publishing" write.

func versionState(t *testing.T, env *testenv.Env, draftID any) (state string, workflowID, publishErr *string) {
	t.Helper()
	if err := env.DB.Raw(`SELECT state::text, workflow_id, publish_error FROM course_versions WHERE id = ?`, draftID).
		Row().Scan(&state, &workflowID, &publishErr); err != nil {
		t.Fatalf("read version: %v", err)
	}
	return
}

func isPublishInProgress(err error) bool {
	ae, ok := apperrors.As(err)
	return ok && ae.HTTPStatus == 409 && ae.Code == "publish_in_progress"
}

func TestPublishClaim_SecondPublishWhilePublishing_Returns409(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	ctx := context.Background()
	starts := 0
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		starts++
		return &fakeWorkflowRun{id: o.ID}, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	courseID, _ := newCourse(t, svc, instr, "claim-dup", "Claim")
	authorDraft(t, svc, courseID, instr, 1, 1)

	if _, err := svc.Publish(ctx, courseID, instr); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	_, err := svc.Publish(ctx, courseID, instr)
	if !isPublishInProgress(err) {
		t.Fatalf("second publish while publishing: want 409 publish_in_progress, got %v", err)
	}
	if starts != 1 {
		t.Fatalf("workflow started %d times, want exactly 1", starts)
	}
}

// The state must already be `publishing` when the workflow is started, so
// nothing the workflow does can be overwritten afterwards.
func TestPublishClaim_StateIsPublishingBeforeWorkflowStarts(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	var courseID, draftID any
	var stateAtStart string
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		stateAtStart, _, _ = versionState(t, env, draftID)
		return &fakeWorkflowRun{id: o.ID}, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	cid, did := newCourse(t, svc, instr, "claim-order", "Order")
	courseID, draftID = cid, did
	authorDraft(t, svc, cid, instr, 1, 1)

	if _, err := svc.Publish(context.Background(), cid, instr); err != nil {
		t.Fatal(err)
	}
	_ = courseID
	if stateAtStart != "publishing" {
		t.Fatalf("version state when the workflow was started = %q, want publishing", stateAtStart)
	}
}

// A workflow that fails fast (for example ValidateMetadata) writes
// publish_failed before Publish returns. That must survive: the old code wrote
// "publishing" AFTER starting the workflow and so overwrote it, leaving the
// version stuck "publishing" with a finished workflow.
func TestPublishClaim_FastFailingWorkflow_FailureNotOverwritten(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	var draftID any
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		// what MarkPublishFailed does, finishing before ExecuteWorkflow returns
		if err := env.DB.Exec(`UPDATE course_versions SET state = 'publish_failed', publish_error = 'metadata invalid' WHERE id = ?`, draftID).Error; err != nil {
			t.Errorf("simulate failure: %v", err)
		}
		return &fakeWorkflowRun{id: o.ID}, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	cid, did := newCourse(t, svc, instr, "claim-fast-fail", "FastFail")
	draftID = did
	authorDraft(t, svc, cid, instr, 1, 1)

	if _, err := svc.Publish(context.Background(), cid, instr); err != nil {
		t.Fatal(err)
	}
	state, _, perr := versionState(t, env, did)
	if state != "publish_failed" || perr == nil || *perr != "metadata invalid" {
		t.Fatalf("a fast-failing workflow's result was overwritten: state=%q error=%v", state, perr)
	}
}

// If the workflow cannot be started, the claim is released so the course is
// not wedged in `publishing`, and a retry works.
func TestPublishClaim_WorkflowStartFails_ReleasesClaimAndRetrySucceeds(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	fail := true
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		if fail {
			return nil, errors.New("temporal unreachable")
		}
		return &fakeWorkflowRun{id: o.ID}, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	cid, did := newCourse(t, svc, instr, "claim-release", "Release")
	authorDraft(t, svc, cid, instr, 1, 1)

	_, err := svc.Publish(context.Background(), cid, instr)
	if ae, ok := apperrors.As(err); !ok || ae.HTTPStatus != 503 {
		t.Fatalf("want 503 when Temporal is unreachable, got %v", err)
	}
	if state, _, _ := versionState(t, env, did); state != "draft" {
		t.Fatalf("claim not released: state=%q, want draft", state)
	}
	fail = false
	if _, err := svc.Publish(context.Background(), cid, instr); err != nil {
		t.Fatalf("retry after a failed start should work: %v", err)
	}
}

// A failed start on a retry of a previously failed publish restores that
// failure (state and message), not a blank draft.
func TestPublishClaim_StartFailsOnRetry_RestoresPreviousFailure(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, _ temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		return nil, errors.New("temporal unreachable")
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	cid, did := newCourse(t, svc, instr, "claim-restore", "Restore")
	authorDraft(t, svc, cid, instr, 1, 1)
	env.DB.Exec(`UPDATE course_versions SET state = 'publish_failed', publish_error = 'earlier failure', workflow_id = 'old-wf' WHERE id = ?`, did)

	if _, err := svc.Publish(context.Background(), cid, instr); err == nil {
		t.Fatal("expected an error")
	}
	state, wf, perr := versionState(t, env, did)
	if state != "publish_failed" || perr == nil || *perr != "earlier failure" || wf == nil || *wf != "old-wf" {
		t.Fatalf("previous failure not restored: state=%q workflow=%v error=%v", state, wf, perr)
	}
}

// A `publishing` claim whose process died (no workflow ever finished it) must
// not block the course forever: after the lease it can be reclaimed. A fresh
// claim must still be refused.
func TestPublishClaim_StaleClaimIsReclaimable_FreshOneIsNot(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	starts := 0
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		starts++
		return &fakeWorkflowRun{id: o.ID}, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	cid, did := newCourse(t, svc, instr, "claim-stale", "Stale")
	authorDraft(t, svc, cid, instr, 1, 1)
	ctx := context.Background()

	env.DB.Exec(`UPDATE course_versions SET state = 'publishing', workflow_id = 'dead-wf', publish_started_at = now() WHERE id = ?`, did)
	if _, err := svc.Publish(ctx, cid, instr); !isPublishInProgress(err) {
		t.Fatalf("a fresh claim must be refused with 409, got %v", err)
	}
	env.DB.Exec(`UPDATE course_versions SET publish_started_at = now() - interval '2 hours' WHERE id = ?`, did)
	if _, err := svc.Publish(ctx, cid, instr); err != nil {
		t.Fatalf("a stale claim should be reclaimable: %v", err)
	}
	// rows from before the migration have no timestamp at all: also reclaimable
	env.DB.Exec(`UPDATE course_versions SET state = 'publishing', publish_started_at = NULL WHERE id = ?`, did)
	if _, err := svc.Publish(ctx, cid, instr); err != nil {
		t.Fatalf("a claim with no timestamp should be reclaimable: %v", err)
	}
	if starts != 2 {
		t.Fatalf("workflow starts = %d, want 2 (the two reclaims)", starts)
	}
}
