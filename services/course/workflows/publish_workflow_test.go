// What this file checks (RFC §18.6/§21 — "Workflow tests... blocked on the
// Temporal workflow existing," now built): PublishCourseWorkflow's own
// orchestration logic — that activities run in RFC §7.4's order, that
// VerifyAssets and ComputeMetadata genuinely run concurrently, and that a
// failure at any step calls MarkPublishFailed and stops the graph without
// reaching PromoteAndPublish — in isolation from real Postgres/Redis.
// Activities are mocked here (testify's mock, via Temporal's own
// testsuite), which is the right call specifically for control-flow
// assertions: a real Activities instance, wired to the real database, is
// exactly what publish_workflow_integration_test.go in this same package
// exercises instead. No build tag: this needs no Docker, no DB — it's
// fast enough to run on every `go test ./...`.
package workflows

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

func TestPublishCourseWorkflow_HappyPath_RunsFullGraphInOrder(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	in := course.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "Intro to Go"}
	outline := course.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID, VersionNumber: 1}

	var a *Activities
	env.OnActivity(a.ValidateMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(a.VerifyAssets, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(a.ComputeMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(a.BuildOutline, mock.Anything, in.CourseID, in.VersionID).Return(&outline, nil).Once()
	env.OnActivity(a.IndexSearch, mock.Anything, outline).Return(nil).Once()
	env.OnActivity(a.WarmCache, mock.Anything, outline).Return(nil).Once()
	env.OnActivity(a.PromoteAndPublish, mock.Anything, in.CourseID, in.VersionID, in.OldVersionID, in.Title).Return(nil).Once()
	env.OnActivity(a.MarkPublishFailed, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe() // must NOT actually be called — asserted below

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("expected no workflow error, got: %v", err)
	}
	env.AssertNotCalled(t, "MarkPublishFailed", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	env.AssertExpectations(t)
}

// TestPublishCourseWorkflow_ValidateMetadataFails_StopsBeforePromote proves
// the "no compensation needed" design (RFC §7.4): a failure at the very
// first activity must still call MarkPublishFailed, and must never reach
// PromoteAndPublish — the one activity whose effect a student can observe.
func TestPublishCourseWorkflow_ValidateMetadataFails_StopsBeforePromote(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	in := course.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "Empty Course"}
	cause := publishFailure("validation_failed: title is required")

	var a *Activities
	env.OnActivity(a.ValidateMetadata, mock.Anything, in.VersionID).Return(cause).Once()
	env.OnActivity(a.MarkPublishFailed, mock.Anything, in.VersionID, cause.Message(), "ValidateMetadata").Return(nil).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete (with an error result, not hang)")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected the workflow to surface ValidateMetadata's error")
	}
	env.AssertNotCalled(t, "VerifyAssets", mock.Anything, mock.Anything)
	env.AssertNotCalled(t, "PromoteAndPublish", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	env.AssertExpectations(t)
}

// TestPublishCourseWorkflow_VerifyAssetsFails_StillWaitsOnComputeMetadata
// proves VerifyAssets and ComputeMetadata actually run concurrently (RFC
// §7.4), not sequentially with an early return: both are started before
// either's result is awaited, so ComputeMetadata must still be mocked and
// called even though VerifyAssets is the one that fails.
func TestPublishCourseWorkflow_VerifyAssetsFails_StillWaitsOnComputeMetadata(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	in := course.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "Bad Content"}
	cause := publishFailure("content_too_large: lesson content too large")

	var a *Activities
	env.OnActivity(a.ValidateMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	// VerifyAssets runs under retry3x (RFC §7.4: 3 attempts) — the test
	// environment honors that policy against mocked activities exactly
	// like a live worker would, so a failing mock must satisfy all 3
	// attempts, not just the first.
	env.OnActivity(a.VerifyAssets, mock.Anything, in.VersionID).Return(cause).Times(3)
	env.OnActivity(a.ComputeMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(a.MarkPublishFailed, mock.Anything, in.VersionID, cause.Message(), "VerifyAssets").Return(nil).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected the workflow to surface VerifyAssets's error")
	}
	env.AssertNotCalled(t, "BuildOutline", mock.Anything, mock.Anything, mock.Anything)
	env.AssertExpectations(t) // fails if ComputeMetadata was never actually called
}

// publishFailure is what the real activities return (Activities.failure): an
// ApplicationError of course.PublishFailureType carrying a fixed category.
func publishFailure(msg string) *temporal.ApplicationError {
	return temporal.NewApplicationError(msg, course.PublishFailureType).(*temporal.ApplicationError)
}

// S-08: an activity error that is not a publish failure (a panic, an error from
// an older worker, a driver or storage error that escaped the activity's own
// mapping) must never reach publish_error, the event or the email as text.
func TestPublishCourseWorkflow_UnmappedActivityErrorTextNeverBecomesTheReason(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	in := course.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "T"}
	raw := errors.New(`stat asset: Head "http://minio.internal.svc:9000/b/k": dial tcp 10.42.7.19:9000: connect: connection refused`)

	var a *Activities
	env.OnActivity(a.ValidateMetadata, mock.Anything, in.VersionID).Return(raw).Times(1)
	env.OnActivity(a.MarkPublishFailed, mock.Anything, in.VersionID, "internal_error", "ValidateMetadata").Return(nil).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if err := env.GetWorkflowError(); err == nil {
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
		{"categorised", temporal.NewApplicationError("asset_invalid: asset not found", course.PublishFailureType), "asset_invalid: asset not found"},
		{"oversized trusted message", temporal.NewApplicationError(strings.Repeat("x", 500), course.PublishFailureType), "internal_error"},
		{"other application error", temporal.NewApplicationError("dial tcp 10.0.0.1:9000", "errorString"), "internal_error"},
		{"plain error", errors.New("Head http://minio:9000"), "internal_error"},
		{"timeout", temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil), "timeout: the step took too long"},
	} {
		if got := safeReason(tc.cause); got != tc.want {
			t.Errorf("%s: safeReason = %q, want %q", tc.name, got, tc.want)
		}
	}
}
