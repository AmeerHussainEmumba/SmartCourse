// Package publish is SmartCourse's one Temporal workflow: course publishing
// (ADR-0009 "Temporal for publishing only", ADR-0025). It holds only the
// deterministic orchestration: the activity graph, the retry policies and the
// failure semantics. It calls every activity BY NAME (libs/contracts/publishing)
// on the activity queue and imports no service code and no database: the
// activities run in the course worker, which owns the data.
//
// Determinism: the workflow function reads no clock, random source, environment
// or network, starts no goroutine and iterates no map; everything it does goes
// through the workflow package. Keep it so, and never rename a workflow or
// activity name or a DTO's JSON field without a versioning plan: running
// workflows replay against this code (see README, "Wire format").
package publish

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
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

// withRetry is the activity options for policy. Every activity runs on the
// activity queue (publishing.ActivityQueue, or the starter's override), a
// different queue from the workflow's own, so the workflow can be served by a
// process that has none of the activities' dependencies.
func withRetry(policy *temporal.RetryPolicy, queue string) workflow.ActivityOptions {
	opts := defaultOpts
	opts.RetryPolicy = policy
	opts.TaskQueue = queue
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
// Registered under publishing.WorkflowName (see Register), which is also the
// string course.Service.Publish passes to client.ExecuteWorkflow: the two sides
// agree on the contract's constant instead of each hardcoding the name.
func PublishCourseWorkflow(ctx workflow.Context, in publishing.PublishWorkflowInput) error {
	queue := in.ActivityQueueOrDefault()

	validateCtx := workflow.WithActivityOptions(ctx, withRetry(noRetry, queue))
	if err := workflow.ExecuteActivity(validateCtx, publishing.ActivityValidateMetadata, in.VersionID).Get(ctx, nil); err != nil {
		return failPublish(ctx, queue, in.VersionID, err)
	}

	verifyCtx := workflow.WithActivityOptions(ctx, withRetry(retry3x, queue))
	verifyFuture := workflow.ExecuteActivity(verifyCtx, publishing.ActivityVerifyAssets, in.VersionID)
	computeFuture := workflow.ExecuteActivity(verifyCtx, publishing.ActivityComputeMetadata, in.VersionID)
	if err := verifyFuture.Get(ctx, nil); err != nil {
		return failPublish(ctx, queue, in.VersionID, err)
	}
	if err := computeFuture.Get(ctx, nil); err != nil {
		return failPublish(ctx, queue, in.VersionID, err)
	}

	outlineCtx := workflow.WithActivityOptions(ctx, withRetry(retry3x, queue))
	var outline publishing.PublishOutline
	if err := workflow.ExecuteActivity(outlineCtx, publishing.ActivityBuildOutline, in.CourseID, in.VersionID).Get(ctx, &outline); err != nil {
		return failPublish(ctx, queue, in.VersionID, err)
	}

	indexCtx := workflow.WithActivityOptions(ctx, withRetry(retry5x, queue))
	indexFuture := workflow.ExecuteActivity(indexCtx, publishing.ActivityIndexSearch, outline)
	warmCtx := workflow.WithActivityOptions(ctx, withRetry(retry3x, queue))
	warmFuture := workflow.ExecuteActivity(warmCtx, publishing.ActivityWarmCache, outline)
	if err := indexFuture.Get(ctx, nil); err != nil {
		return failPublish(ctx, queue, in.VersionID, err)
	}
	if err := warmFuture.Get(ctx, nil); err != nil {
		return failPublish(ctx, queue, in.VersionID, err)
	}

	promoteCtx := workflow.WithActivityOptions(ctx, withRetry(retry5x, queue))
	if err := workflow.ExecuteActivity(promoteCtx, publishing.ActivityPromoteAndPublish, in.CourseID, in.VersionID, in.OldVersionID, in.Title).Get(ctx, nil); err != nil {
		return failPublish(ctx, queue, in.VersionID, err)
	}
	return nil
}

// failPublish records the failure (best-effort — a failure to even write
// the failure doesn't change what caused the publish to fail) and returns
// the original error as the workflow's result.
//
// The reason stored and emitted is a bounded, fixed-vocabulary string and
// never raw error text (S-08): it ends up in course_versions.publish_error,
// in course.publish_failed (kept by analytics) and in the instructor's email.
// The activities (in the course worker) report their failures as an
// ApplicationError of type publishing.FailureType whose message is a fixed
// vocabulary reason; that message is trusted, and only that one. Any
// other cause (a timeout, a panic, an error from an older worker version,
// whose text may embed a driver or storage error, Temporal's scheduledEventID
// noise or a Go type name) maps to a fixed string by kind.
func failPublish(ctx workflow.Context, queue string, versionID uuid.UUID, cause error) error {
	reason := safeReason(cause)
	// The name of the activity whose final failure ended the publish, for
	// course.publish_failed. Empty when the cause is not an activity failure.
	var failedActivity string
	var actErr *temporal.ActivityError
	if errors.As(cause, &actErr) {
		failedActivity = actErr.ActivityType().GetName()
	}
	failCtx := workflow.WithActivityOptions(ctx, withRetry(retry5x, queue))
	_ = workflow.ExecuteActivity(failCtx, publishing.ActivityMarkPublishFailed, versionID, reason, failedActivity).Get(ctx, nil)
	return cause
}

// maxReasonLength bounds a trusted activity message; the vocabulary is far
// shorter, so this only guards against a future category that is not.
const maxReasonLength = 120

// safeReason is the publish-failure reason for cause, free of raw error text.
func safeReason(cause error) string {
	var appErr *temporal.ApplicationError
	if errors.As(cause, &appErr) && appErr.Type() == publishing.FailureType {
		if m := appErr.Message(); m != "" && len(m) <= maxReasonLength {
			return m
		}
		return "internal_error"
	}
	var timeout *temporal.TimeoutError
	if errors.As(cause, &timeout) {
		return "timeout: the step took too long"
	}
	if temporal.IsCanceledError(cause) {
		return "canceled"
	}
	return "internal_error"
}
