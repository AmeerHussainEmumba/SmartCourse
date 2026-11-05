// Package workflows holds SmartCourse's one Temporal workflow — course
// publishing (ADR-0009: "Temporal for publishing only", restated in
// ADR-0025 for the concrete M2 implementation). It imports internal/course
// because orchestrating course's own publish steps is the only reason this
// package exists; nothing else in the codebase imports workflows, and
// workflows must never be imported by internal/course itself (that would
// cycle — see course.PublishWorkflowName's doc comment for how
// Service.Publish starts this workflow by name instead).
package workflows

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
)

// Retry counts below mirror RFC §7.4's activity table. RFC §21's D1
// ("Retry counts and backoff curves... settled by observed failure
// patterns") flags these as provisional starting points, not tuned final
// values — expect them to change once there's real publish traffic to
// observe.
var (
	noRetry     = &temporal.RetryPolicy{MaximumAttempts: 1}
	retry3x     = &temporal.RetryPolicy{MaximumAttempts: 3}
	retry5x     = &temporal.RetryPolicy{MaximumAttempts: 5}
	defaultOpts = workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
)

func withRetry(policy *temporal.RetryPolicy) workflow.ActivityOptions {
	opts := defaultOpts
	opts.RetryPolicy = policy
	return opts
}

// PublishCourseWorkflow implements RFC §7.4's activity graph:
//
//	ValidateMetadata -> {VerifyAssets, ComputeMetadata} -> BuildOutline -> {IndexSearch, WarmCache} -> PromoteAndPublish
//
// PromoteAndPublish is deliberately last and un-parallelized (RFC §7.4:
// "the only one whose effect students can observe" — everything before it
// operates on the draft or on derived stores students never see directly).
// On any activity's final failure, MarkPublishFailed records the error and
// the workflow returns that error without attempting compensation: nothing
// before PromoteAndPublish touched the live version, so there's nothing to
// undo (RFC §7.4's own stated property of this design, not an oversight).
//
// Registered under course.PublishWorkflowName (see bootstrap.
// BuildPublishWorker), which is also the string course.Service.Publish
// passes to client.ExecuteWorkflow — the two packages agree on a shared
// constant instead of each hardcoding "PublishCourseWorkflow" separately.
func PublishCourseWorkflow(ctx workflow.Context, in course.PublishWorkflowInput) error {
	var a *Activities // never dereferenced locally — Temporal resolves the activity name from the method expression and runs it on a worker, never in-process here (this is the SDK's documented pattern, not a nil-pointer bug)

	validateCtx := workflow.WithActivityOptions(ctx, withRetry(noRetry))
	if err := workflow.ExecuteActivity(validateCtx, a.ValidateMetadata, in.VersionID).Get(ctx, nil); err != nil {
		return failPublish(ctx, in.VersionID, err)
	}

	verifyCtx := workflow.WithActivityOptions(ctx, withRetry(retry3x))
	verifyFuture := workflow.ExecuteActivity(verifyCtx, a.VerifyAssets, in.VersionID)
	computeFuture := workflow.ExecuteActivity(verifyCtx, a.ComputeMetadata, in.VersionID)
	if err := verifyFuture.Get(ctx, nil); err != nil {
		return failPublish(ctx, in.VersionID, err)
	}
	if err := computeFuture.Get(ctx, nil); err != nil {
		return failPublish(ctx, in.VersionID, err)
	}

	outlineCtx := workflow.WithActivityOptions(ctx, withRetry(retry3x))
	var outline course.PublishOutline
	if err := workflow.ExecuteActivity(outlineCtx, a.BuildOutline, in.CourseID, in.VersionID).Get(ctx, &outline); err != nil {
		return failPublish(ctx, in.VersionID, err)
	}

	indexCtx := workflow.WithActivityOptions(ctx, withRetry(retry5x))
	indexFuture := workflow.ExecuteActivity(indexCtx, a.IndexSearch, outline)
	warmCtx := workflow.WithActivityOptions(ctx, withRetry(retry3x))
	warmFuture := workflow.ExecuteActivity(warmCtx, a.WarmCache, outline)
	if err := indexFuture.Get(ctx, nil); err != nil {
		return failPublish(ctx, in.VersionID, err)
	}
	if err := warmFuture.Get(ctx, nil); err != nil {
		return failPublish(ctx, in.VersionID, err)
	}

	promoteCtx := workflow.WithActivityOptions(ctx, withRetry(retry5x))
	if err := workflow.ExecuteActivity(promoteCtx, a.PromoteAndPublish, in.CourseID, in.VersionID, in.OldVersionID, in.Title).Get(ctx, nil); err != nil {
		return failPublish(ctx, in.VersionID, err)
	}
	return nil
}

// failPublish records the failure (best-effort — a failure to even write
// the failure doesn't change what caused the publish to fail) and returns
// the original error as the workflow's result. The reason stored is the
// activity's own message, not cause.Error()'s full text: Temporal wraps
// every activity failure in an ActivityError/ApplicationError pair whose
// Error() string embeds diagnostic noise (scheduledEventID, retryable
// flag, Go type name) — exactly the kind of thing that shouldn't end up
// in course_versions.publish_error for an instructor to read.
func failPublish(ctx workflow.Context, versionID uuid.UUID, cause error) error {
	var a *Activities
	reason := cause.Error()
	var appErr *temporal.ApplicationError
	if errors.As(cause, &appErr) {
		reason = appErr.Message()
	}
	failCtx := workflow.WithActivityOptions(ctx, withRetry(retry5x))
	_ = workflow.ExecuteActivity(failCtx, a.MarkPublishFailed, versionID, reason).Get(ctx, nil)
	return cause
}
