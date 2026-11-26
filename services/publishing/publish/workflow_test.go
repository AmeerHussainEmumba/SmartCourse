// What this file checks: PublishCourseWorkflow's own orchestration (RFC §7.4):
// the activities run in the documented order, VerifyAssets and ComputeMetadata
// run concurrently, a failure at any step reaches MarkPublishFailed and stops
// the graph before PromoteAndPublish, and the failure reason is never raw error
// text (S-08). The activities are stubs registered BY NAME, as the course worker
// registers the real ones: this module has no access to their implementation.
// No Docker, no server: Temporal's in-process test environment.
package publish

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
)

// recorder notes which activities ran, in order, and the task queue each was
// scheduled on.
type recorder struct {
	mu     sync.Mutex
	ran    []string
	queues map[string]string
}

func (r *recorder) note(ctx context.Context, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ran = append(r.ran, name)
	r.queues[name] = activity.GetInfo(ctx).TaskQueue
}

func (r *recorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ran...)
}

// registerStubs registers a recording stub per contract activity name, with the
// parameter and result types the real activities have. outline is what
// BuildOutline returns.
func registerStubs(env *testsuite.TestWorkflowEnvironment, outline publishing.PublishOutline) *recorder {
	rec := &recorder{queues: map[string]string{}}
	reg := func(name string, fn any) { env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name}) }
	reg(publishing.ActivityValidateMetadata, func(ctx context.Context, _ uuid.UUID) error {
		rec.note(ctx, publishing.ActivityValidateMetadata)
		return nil
	})
	reg(publishing.ActivityVerifyAssets, func(ctx context.Context, _ uuid.UUID) error {
		rec.note(ctx, publishing.ActivityVerifyAssets)
		return nil
	})
	reg(publishing.ActivityComputeMetadata, func(ctx context.Context, _ uuid.UUID) error {
		rec.note(ctx, publishing.ActivityComputeMetadata)
		return nil
	})
	reg(publishing.ActivityBuildOutline, func(ctx context.Context, _, _ uuid.UUID) (*publishing.PublishOutline, error) {
		rec.note(ctx, publishing.ActivityBuildOutline)
		return &outline, nil
	})
	reg(publishing.ActivityIndexSearch, func(ctx context.Context, _ publishing.PublishOutline) error {
		rec.note(ctx, publishing.ActivityIndexSearch)
		return nil
	})
	reg(publishing.ActivityWarmCache, func(ctx context.Context, _ publishing.PublishOutline) error {
		rec.note(ctx, publishing.ActivityWarmCache)
		return nil
	})
	reg(publishing.ActivityPromoteAndPublish, func(ctx context.Context, _, _ uuid.UUID, _ *uuid.UUID, _ string) error {
		rec.note(ctx, publishing.ActivityPromoteAndPublish)
		return nil
	})
	reg(publishing.ActivityMarkPublishFailed, func(ctx context.Context, _ uuid.UUID, _, _ string) error {
		rec.note(ctx, publishing.ActivityMarkPublishFailed)
		return nil
	})
	return rec
}

func newInput() publishing.PublishWorkflowInput {
	return publishing.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "Intro to Go"}
}

// publishFailure is what the real activities return: an ApplicationError of
// publishing.FailureType carrying a fixed category.
func publishFailure(msg string) *temporal.ApplicationError {
	return temporal.NewApplicationError(msg, publishing.FailureType).(*temporal.ApplicationError)
}

func TestPublishCourseWorkflow_HappyPath_RunsFullGraphInOrder(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := newInput()
	outline := publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID, VersionNumber: 1}
	registerStubs(env, outline)

	env.OnActivity(publishing.ActivityValidateMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(publishing.ActivityVerifyAssets, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(publishing.ActivityComputeMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(publishing.ActivityBuildOutline, mock.Anything, in.CourseID, in.VersionID).Return(&outline, nil).Once()
	env.OnActivity(publishing.ActivityIndexSearch, mock.Anything, outline).Return(nil).Once()
	env.OnActivity(publishing.ActivityWarmCache, mock.Anything, outline).Return(nil).Once()
	env.OnActivity(publishing.ActivityPromoteAndPublish, mock.Anything, in.CourseID, in.VersionID, in.OldVersionID, in.Title).Return(nil).Once()
	env.OnActivity(publishing.ActivityMarkPublishFailed, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("expected no workflow error, got: %v", err)
	}
	env.AssertNotCalled(t, publishing.ActivityMarkPublishFailed, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	env.AssertExpectations(t)
}

// The order of the graph, observed from what actually ran: Validate first,
// BuildOutline after both of the parallel pair, Promote last.
func TestPublishCourseWorkflow_HappyPath_OrderOfExecution(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := newInput()
	rec := registerStubs(env, publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID})
	env.ExecuteWorkflow(PublishCourseWorkflow, in)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	ran := rec.names()
	pos := map[string]int{}
	for i, n := range ran {
		pos[n] = i
	}
	if len(ran) != 7 {
		t.Fatalf("ran %v, want the 7 steps of the success path", ran)
	}
	before := func(a, b string) {
		if pos[a] >= pos[b] {
			t.Errorf("%s must run before %s (ran %v)", a, b, ran)
		}
	}
	before(publishing.ActivityValidateMetadata, publishing.ActivityVerifyAssets)
	before(publishing.ActivityValidateMetadata, publishing.ActivityComputeMetadata)
	before(publishing.ActivityVerifyAssets, publishing.ActivityBuildOutline)
	before(publishing.ActivityComputeMetadata, publishing.ActivityBuildOutline)
	before(publishing.ActivityBuildOutline, publishing.ActivityIndexSearch)
	before(publishing.ActivityBuildOutline, publishing.ActivityWarmCache)
	before(publishing.ActivityIndexSearch, publishing.ActivityPromoteAndPublish)
	before(publishing.ActivityWarmCache, publishing.ActivityPromoteAndPublish)
}

// A failure at the very first activity still calls MarkPublishFailed and never
// reaches PromoteAndPublish, the one activity whose effect a student can see.
func TestPublishCourseWorkflow_ValidateMetadataFails_StopsBeforePromote(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := newInput()
	registerStubs(env, publishing.PublishOutline{})
	cause := publishFailure("validation_failed: title is required")

	env.OnActivity(publishing.ActivityValidateMetadata, mock.Anything, in.VersionID).Return(cause).Once()
	env.OnActivity(publishing.ActivityMarkPublishFailed, mock.Anything, in.VersionID, cause.Message(), publishing.ActivityValidateMetadata).Return(nil).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete (with an error result, not hang)")
	}
	if env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to surface ValidateMetadata's error")
	}
	env.AssertNotCalled(t, publishing.ActivityVerifyAssets, mock.Anything, mock.Anything)
	env.AssertNotCalled(t, publishing.ActivityPromoteAndPublish, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	env.AssertExpectations(t)
}

// VerifyAssets and ComputeMetadata run concurrently (RFC §7.4): both start
// before either's result is awaited, so ComputeMetadata is still called when
// VerifyAssets is the one that fails.
func TestPublishCourseWorkflow_VerifyAssetsFails_StillWaitsOnComputeMetadata(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := newInput()
	registerStubs(env, publishing.PublishOutline{})
	cause := publishFailure("content_too_large: lesson content too large")

	env.OnActivity(publishing.ActivityValidateMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	// VerifyAssets runs under retry3x: a failing mock satisfies all 3 attempts.
	env.OnActivity(publishing.ActivityVerifyAssets, mock.Anything, in.VersionID).Return(cause).Times(3)
	env.OnActivity(publishing.ActivityComputeMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(publishing.ActivityMarkPublishFailed, mock.Anything, in.VersionID, cause.Message(), publishing.ActivityVerifyAssets).Return(nil).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to surface VerifyAssets's error")
	}
	env.AssertNotCalled(t, publishing.ActivityBuildOutline, mock.Anything, mock.Anything, mock.Anything)
	env.AssertExpectations(t) // fails if ComputeMetadata was never called
}

// Failure late in the graph: PromoteAndPublish fails after its retries, and
// MarkPublishFailed names it. Nothing is compensated.
func TestPublishCourseWorkflow_PromoteFails_MarksFailedWithActivityName(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := newInput()
	outline := publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID, VersionNumber: 2}
	registerStubs(env, outline)
	cause := publishFailure("conflict: version is no longer a draft")

	env.OnActivity(publishing.ActivityPromoteAndPublish, mock.Anything, in.CourseID, in.VersionID, in.OldVersionID, in.Title).Return(cause).Times(5) // retry5x
	env.OnActivity(publishing.ActivityMarkPublishFailed, mock.Anything, in.VersionID, cause.Message(), publishing.ActivityPromoteAndPublish).Return(nil).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail")
	}
	env.AssertExpectations(t)
}

// S-08: an activity error that is not a publish failure must never reach
// publish_error, the event or the email as text.
func TestPublishCourseWorkflow_UnmappedActivityErrorTextNeverBecomesTheReason(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := newInput()
	registerStubs(env, publishing.PublishOutline{})
	raw := errors.New(`stat asset: Head "http://minio.internal.svc:9000/b/k": dial tcp 10.42.7.19:9000: connect: connection refused`)

	env.OnActivity(publishing.ActivityValidateMetadata, mock.Anything, in.VersionID).Return(raw).Once()
	env.OnActivity(publishing.ActivityMarkPublishFailed, mock.Anything, in.VersionID, "internal_error", publishing.ActivityValidateMetadata).Return(nil).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail")
	}
	env.AssertExpectations(t) // MarkPublishFailed got exactly "internal_error"
}

func TestSafeReason(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		want  string
	}{
		{"categorised", temporal.NewApplicationError("asset_invalid: asset not found", publishing.FailureType), "asset_invalid: asset not found"},
		{"oversized trusted message", temporal.NewApplicationError(strings.Repeat("x", 500), publishing.FailureType), "internal_error"},
		{"other application error", temporal.NewApplicationError("dial tcp 10.0.0.1:9000", "errorString"), "internal_error"},
		{"plain error", errors.New("Head http://minio:9000"), "internal_error"},
		{"timeout", temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil), "timeout: the step took too long"},
	} {
		if got := safeReason(tc.cause); got != tc.want {
			t.Errorf("%s: safeReason = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// The contract and the workflow must agree on the set of activities: every name
// in publishing.ActivityNames() is scheduled by some path of the workflow and
// the workflow schedules nothing else. Adding an activity to one side fails the
// build of the other. The success path plus the failure path cover every name.
func TestPublishCourseWorkflow_ScheduledActivitiesEqualTheContract(t *testing.T) {
	scheduled := map[string]bool{}
	var mu sync.Mutex
	run := func(failFirst bool) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		in := newInput()
		registerStubs(env, publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID})
		env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
			mu.Lock()
			scheduled[info.ActivityType.Name] = true
			mu.Unlock()
		})
		if failFirst {
			env.OnActivity(publishing.ActivityValidateMetadata, mock.Anything, in.VersionID).Return(publishFailure("validation_failed: x"))
		}
		env.ExecuteWorkflow(PublishCourseWorkflow, in)
		if !env.IsWorkflowCompleted() {
			t.Fatal("workflow did not complete")
		}
		if failFirst != (env.GetWorkflowError() != nil) {
			t.Fatalf("failFirst=%v but workflow error = %v", failFirst, env.GetWorkflowError())
		}
	}
	run(false)
	run(true)

	var got []string
	for n := range scheduled {
		got = append(got, n)
	}
	sort.Strings(got)
	want := append([]string(nil), publishing.ActivityNames()...)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("workflow schedules %v, the contract lists %v", got, want)
	}
}

// Every activity is scheduled on the activity queue (the contract default or
// the starter's override), never on the workflow's own queue, on the failure
// path too.
func TestPublishCourseWorkflow_UsesTheActivityQueue(t *testing.T) {
	for _, tc := range []struct{ name, queue, want string }{
		{"default queue", "", publishing.ActivityQueue},
		{"starter override", "custom-activities", "custom-activities"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			in := newInput()
			in.ActivityQueue = tc.queue
			rec := registerStubs(env, publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID})
			env.ExecuteWorkflow(PublishCourseWorkflow, in)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			if len(rec.queues) != 7 {
				t.Fatalf("recorded %d activities, want 7", len(rec.queues))
			}
			for name, q := range rec.queues {
				if q != tc.want {
					t.Errorf("activity %s ran on queue %q, want %q", name, q, tc.want)
				}
			}
		})
	}
}

func TestPublishCourseWorkflow_FailurePathUsesTheActivityQueueToo(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := newInput()
	in.ActivityQueue = "custom-activities"
	rec := registerStubs(env, publishing.PublishOutline{})
	env.OnActivity(publishing.ActivityValidateMetadata, mock.Anything, in.VersionID).Return(publishFailure("asset_invalid: nope")).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)
	if env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if got := rec.queues[publishing.ActivityMarkPublishFailed]; got != "custom-activities" {
		t.Errorf("MarkPublishFailed queue = %q (ran: %v)", got, rec.ran)
	}
}
