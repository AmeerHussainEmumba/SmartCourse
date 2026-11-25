//go:build integration

package course_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// Tests for ReconcileStalePublishes (ADR-0028's lease, reconciled against
// Temporal): a version stuck `publishing` whose workflow is gone is released
// to publish_failed; a running one is left alone; and the release can never
// overwrite a legitimate result.

// reconcileFake answers ExecuteWorkflow (always starts) and
// DescribeWorkflowExecution from a per-workflow-ID script.
type reconcileFake struct {
	temporalclient.Client
	mu        sync.Mutex
	describes []string
	describe  func(workflowID string) (enumspb.WorkflowExecutionStatus, error)
}

func (f *reconcileFake) ExecuteWorkflow(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
	return &fakeWorkflowRun{id: o.ID}, nil
}

func (f *reconcileFake) DescribeWorkflowExecution(_ context.Context, workflowID, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.mu.Lock()
	f.describes = append(f.describes, workflowID)
	f.mu.Unlock()
	st, err := f.describe(workflowID)
	if err != nil {
		return nil, err
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{Status: st},
	}, nil
}

// publishingVersion publishes a fresh course through the fake (so the version
// is genuinely claimed) and ages the claim past the grace period.
func publishingVersion(t *testing.T, env *testenv.Env, svc *course.Service, instr uuid.UUID, slug string) (courseID, versionID uuid.UUID) {
	t.Helper()
	courseID, versionID = newCourse(t, svc, instr, slug, slug)
	authorDraft(t, svc, courseID, instr, 1, 1)
	if _, err := svc.Publish(context.Background(), courseID, instr); err != nil {
		t.Fatalf("publish: %v", err)
	}
	ageClaim(t, env, versionID)
	return courseID, versionID
}

func ageClaim(t *testing.T, env *testenv.Env, versionID uuid.UUID) {
	t.Helper()
	if err := env.DB.Exec(`UPDATE course_versions SET publish_started_at = now() - interval '10 minutes' WHERE id = ?`, versionID).Error; err != nil {
		t.Fatalf("age claim: %v", err)
	}
}

func TestReconcile_StateMatrix(t *testing.T) {
	notFound := serviceerror.NewNotFound("workflow not found")
	cases := []struct {
		name         string
		status       enumspb.WorkflowExecutionStatus
		err          error
		wantState    string
		wantReleased int
		wantRunning  int
		wantErrs     int
		wantErrText  string
	}{
		{"not_found", 0, notFound, "publish_failed", 1, 0, 0, "interrupted before its workflow was started"},
		{"running_left_alone", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, nil, "publishing", 0, 1, 0, ""},
		{"continued_as_new_left_alone", enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, nil, "publishing", 0, 1, 0, ""},
		{"completed_without_result", enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, nil, "publish_failed", 1, 0, 0, "completed"},
		{"failed_without_result", enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil, "publish_failed", 1, 0, 0, "failed"},
		{"terminated", enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil, "publish_failed", 1, 0, 0, "terminated"},
		{"canceled", enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, nil, "publish_failed", 1, 0, 0, "canceled"},
		{"timed_out", enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, nil, "publish_failed", 1, 0, 0, "timedout"},
		{"unknown_status_left_alone", enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, nil, "publishing", 0, 0, 1, ""},
		{"temporal_unavailable_left_alone", 0, serviceerror.NewUnavailable("down"), "publishing", 0, 0, 1, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := testenv.Setup(t)
			env.DB = env.ServiceDB(t, "course")
			instr := seedInstructor(t, env)
			fake := &reconcileFake{describe: func(string) (enumspb.WorkflowExecutionStatus, error) { return tc.status, tc.err }}
			svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
			_, vid := publishingVersion(t, env, svc, instr, "rec-"+tc.name[:3])

			res, err := svc.ReconcileStalePublishes(context.Background())
			if (err != nil) != (tc.wantErrs > 0) {
				t.Fatalf("err = %v, wantErrs %d", err, tc.wantErrs)
			}
			if res.Checked != 1 || res.Released != tc.wantReleased || res.Running != tc.wantRunning || res.Errors != tc.wantErrs {
				t.Fatalf("result = %+v", res)
			}
			state, _, pubErr := versionState(t, env, vid)
			if state != tc.wantState {
				t.Fatalf("state = %q, want %q", state, tc.wantState)
			}
			if tc.wantState == "publish_failed" {
				if pubErr == nil || *pubErr == "" || !strings.Contains(strings.ToLower(*pubErr), tc.wantErrText) {
					t.Fatalf("publish_error = %v, want it to mention %q", pubErr, tc.wantErrText)
				}
			} else if pubErr != nil {
				t.Fatalf("publish_error = %q, want none while still publishing", *pubErr)
			}
		})
	}
}

// A claim younger than the grace period is never even described: the workflow
// may be about to start.
func TestReconcile_FreshClaimNotTouched(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	fake := &reconcileFake{describe: func(string) (enumspb.WorkflowExecutionStatus, error) {
		return 0, serviceerror.NewNotFound("nope")
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	courseID, vid := newCourse(t, svc, instr, "rec-fresh", "Fresh")
	authorDraft(t, svc, courseID, instr, 1, 1)
	if _, err := svc.Publish(context.Background(), courseID, instr); err != nil {
		t.Fatal(err)
	}

	res, err := svc.ReconcileStalePublishes(context.Background())
	if err != nil || res.Checked != 0 {
		t.Fatalf("res=%+v err=%v, want nothing checked", res, err)
	}
	if len(fake.describes) != 0 {
		t.Fatalf("fresh claim was described: %v", fake.describes)
	}
	if st, _, _ := versionState(t, env, vid); st != "publishing" {
		t.Fatalf("state = %q", st)
	}
}

// Versions that are not `publishing` are never touched.
func TestReconcile_OnlyPublishingVersions(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	fake := &reconcileFake{describe: func(string) (enumspb.WorkflowExecutionStatus, error) {
		return 0, serviceerror.NewNotFound("nope")
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	_, draftID := newCourse(t, svc, instr, "rec-draft", "Draft")
	_, readyID := newCourse(t, svc, instr, "rec-ready", "Ready")
	if err := env.DB.Exec(`UPDATE course_versions SET state = 'ready' WHERE id = ?`, readyID).Error; err != nil {
		t.Fatal(err)
	}
	res, err := svc.ReconcileStalePublishes(context.Background())
	if err != nil || res.Checked != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if st, _, _ := versionState(t, env, draftID); st != "draft" {
		t.Fatalf("draft state = %q", st)
	}
	if st, _, _ := versionState(t, env, readyID); st != "ready" {
		t.Fatalf("ready state = %q", st)
	}
}

// The point of the change: after a crash between claim and start, republish
// works right after reconciliation instead of waiting out the 15-minute lease,
// and a second reconciliation pass is a no-op.
func TestReconcile_ReleasesAndRepublishSucceedsWithoutLeaseWait(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	fake := &reconcileFake{describe: func(string) (enumspb.WorkflowExecutionStatus, error) {
		return 0, serviceerror.NewNotFound("nope")
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	courseID, vid := publishingVersion(t, env, svc, instr, "rec-republish")
	// Make the claim only 2 minutes old: well inside the 15-minute lease.
	if err := env.DB.Exec(`UPDATE course_versions SET publish_started_at = now() - interval '2 minutes' WHERE id = ?`, vid).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := svc.Publish(context.Background(), courseID, instr); !isPublishInProgress(err) {
		t.Fatalf("before reconcile: want 409 publish_in_progress, got %v", err)
	}
	res, err := svc.ReconcileStalePublishes(context.Background())
	if err != nil || res.Released != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if again, err := svc.ReconcileStalePublishes(context.Background()); err != nil || again.Checked != 0 || again.Released != 0 {
		t.Fatalf("second pass not a no-op: %+v err=%v", again, err)
	}
	if _, err := svc.Publish(context.Background(), courseID, instr); err != nil {
		t.Fatalf("republish after reconcile: %v", err)
	}
	if st, _, pubErr := versionState(t, env, vid); st != "publishing" || pubErr != nil {
		t.Fatalf("after republish state=%q err=%v", st, pubErr)
	}
}

// Rows from before migration 000006 have no claim timestamp and may have no
// workflow ID; they are still reconciled.
func TestReconcile_LegacyRowWithoutTimestampOrWorkflowID(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	fake := &reconcileFake{describe: func(string) (enumspb.WorkflowExecutionStatus, error) {
		t.Fatal("must not describe an empty workflow ID")
		return 0, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	_, vid := newCourse(t, svc, instr, "rec-legacy", "Legacy")
	if err := env.DB.Exec(`UPDATE course_versions SET state='publishing', workflow_id=NULL, publish_started_at=NULL WHERE id = ?`, vid).Error; err != nil {
		t.Fatal(err)
	}
	res, err := svc.ReconcileStalePublishes(context.Background())
	if err != nil || res.Released != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if st, _, _ := versionState(t, env, vid); st != "publish_failed" {
		t.Fatalf("state = %q", st)
	}
}

// Deterministic race: the workflow's PromoteAndPublish lands between the
// reconciler reading the stale claim and releasing it (here, inside the
// Temporal describe call, which reports the workflow gone). The guarded UPDATE
// must lose: the version stays `ready` and is never flipped to publish_failed.
func TestReconcile_CompletionBetweenReadAndRelease_NotOverwritten(t *testing.T) {
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
	courseID, vid = publishingVersion(t, env, svc, instr, "rec-race1")

	res, err := svc.ReconcileStalePublishes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Released != 0 || res.Lost != 1 {
		t.Fatalf("result = %+v, want the release to lose the race", res)
	}
	st, _, pubErr := versionState(t, env, vid)
	if st != "ready" || pubErr != nil {
		t.Fatalf("state=%q err=%v, want ready with no error", st, pubErr)
	}
}

// Concurrent race: reconciliation (workflow reported gone) against a real
// PromoteAndPublish, repeated. Whatever the interleaving, the version ends
// `ready` and the course points at it: never publish_failed with a promoted
// version, and no errors.
func TestReconcile_ConcurrentWithCompletion(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	instr := seedInstructor(t, env)
	fake := &reconcileFake{describe: func(string) (enumspb.WorkflowExecutionStatus, error) {
		return enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	for i := 0; i < 15; i++ {
		courseID, vid := publishingVersion(t, env, svc, instr, fmt.Sprintf("rec-conc-%d", i))
		var wg sync.WaitGroup
		wg.Add(2)
		var promoteErr, reconErr error
		start := make(chan struct{})
		go func() {
			defer wg.Done()
			<-start
			promoteErr = svc.PromoteAndPublish(context.Background(), courseID, vid, nil, "T")
		}()
		go func() {
			defer wg.Done()
			<-start
			_, reconErr = svc.ReconcileStalePublishes(context.Background())
		}()
		close(start)
		wg.Wait()
		if promoteErr != nil || reconErr != nil {
			t.Fatalf("iteration %d: promote=%v reconcile=%v", i, promoteErr, reconErr)
		}
		st, _, _ := versionState(t, env, vid)
		var published *uuid.UUID
		_ = env.DB.Raw(`SELECT published_version_id FROM courses WHERE id = ?`, courseID).Row().Scan(&published)
		if published == nil || *published != vid {
			t.Fatalf("iteration %d: course not pointing at promoted version", i)
		}
		if st != "ready" {
			t.Fatalf("iteration %d: promoted version is %q, want ready", i, st)
		}
	}
}
