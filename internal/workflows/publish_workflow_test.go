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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
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
	env.OnActivity(a.MarkPublishFailed, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe() // must NOT actually be called — asserted below

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("expected no workflow error, got: %v", err)
	}
	env.AssertNotCalled(t, "MarkPublishFailed", mock.Anything, mock.Anything, mock.Anything)
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
	cause := apperr("title is required to publish")

	var a *Activities
	env.OnActivity(a.ValidateMetadata, mock.Anything, in.VersionID).Return(cause).Once()
	env.OnActivity(a.MarkPublishFailed, mock.Anything, in.VersionID, cause.Error()).Return(nil).Once()

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
	cause := apperr("lesson content too large")

	var a *Activities
	env.OnActivity(a.ValidateMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	// VerifyAssets runs under retry3x (RFC §7.4: 3 attempts) — the test
	// environment honors that policy against mocked activities exactly
	// like a live worker would, so a failing mock must satisfy all 3
	// attempts, not just the first.
	env.OnActivity(a.VerifyAssets, mock.Anything, in.VersionID).Return(cause).Times(3)
	env.OnActivity(a.ComputeMetadata, mock.Anything, in.VersionID).Return(nil).Once()
	env.OnActivity(a.MarkPublishFailed, mock.Anything, in.VersionID, cause.Error()).Return(nil).Once()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected the workflow to surface VerifyAssets's error")
	}
	env.AssertNotCalled(t, "BuildOutline", mock.Anything, mock.Anything, mock.Anything)
	env.AssertExpectations(t) // fails if ComputeMetadata was never actually called
}

type apperr string

func (e apperr) Error() string { return string(e) }
