// What this file checks: PublishCourseWorkflow under fault injection, in
// Temporal's in-process test environment with the activities stubbed BY NAME
// to fail or flap (no Docker, no server). It restores the three scenarios that
// lived next to the old in-service workflow and were not carried over when
// services/worker was split:
//
//   - flapping: an activity that fails on all but its last permitted attempt
//     still lets the publish finish, with no MarkPublishFailed;
//   - permanent failure: an activity that always fails is attempted exactly as
//     often as its retry policy allows, then MarkPublishFailed runs once with
//     that activity's name and nothing later in the graph runs;
//   - lost ack: an activity whose effect happened but whose reply was lost is
//     retried with the same arguments and the publish still finishes. That the
//     retried real activity is a no-op is proven where the data lives, in
//     services/course/activities (TestActivities_LostAck_*).
//
// The retry counts below mirror workflow.go (RFC §7.4); a change there changes
// this table on purpose.
package publish

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
)

// stage is the activity's position in the graph; an activity of a later stage
// must not run when an earlier one finally failed. The two activities of one
// stage run in parallel, so a sibling may or may not run.
type faultCase struct {
	name     string
	attempts int // MaximumAttempts of the activity's retry policy
	stage    int
	extra    int // arguments after the context
}

var faultCases = []faultCase{
	{publishing.ActivityValidateMetadata, 1, 0, 1},
	{publishing.ActivityVerifyAssets, 3, 1, 1},
	{publishing.ActivityComputeMetadata, 3, 1, 1},
	{publishing.ActivityBuildOutline, 3, 2, 2},
	{publishing.ActivityIndexSearch, 5, 3, 1},
	{publishing.ActivityWarmCache, 3, 3, 1},
	{publishing.ActivityPromoteAndPublish, 5, 4, 4},
}

func anyArgs(extra int) []any {
	args := []any{mock.Anything} // the activity context
	for i := 0; i < extra; i++ {
		args = append(args, mock.Anything)
	}
	return args
}

// faultEnv is a workflow test environment where every activity except the
// scripted ones (and MarkPublishFailed, which each test scripts) is a mock that
// succeeds, so AssertNumberOfCalls sees every call.
func faultEnv(t *testing.T, scripted ...string) (*testsuite.TestWorkflowEnvironment, publishing.PublishWorkflowInput) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := newInput()
	outline := publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID, VersionNumber: 1}
	registerStubs(env, outline)
	skip := map[string]bool{}
	for _, n := range scripted {
		skip[n] = true
	}
	for _, c := range faultCases {
		if skip[c.name] {
			continue
		}
		if c.name == publishing.ActivityBuildOutline {
			env.OnActivity(c.name, anyArgs(c.extra)...).Return(&outline, nil)
		} else {
			env.OnActivity(c.name, anyArgs(c.extra)...).Return(nil)
		}
	}
	return env, in
}

// failTimes scripts name to return err on its first n calls and succeed after
// (BuildOutline returns an outline, as the real one does).
func failTimes(env *testsuite.TestWorkflowEnvironment, c faultCase, n int, err error, outline *publishing.PublishOutline) {
	call := env.OnActivity(c.name, anyArgs(c.extra)...)
	if c.name == publishing.ActivityBuildOutline {
		call.Return(nil, err).Times(n)
		env.OnActivity(c.name, anyArgs(c.extra)...).Return(outline, nil)
		return
	}
	call.Return(err).Times(n)
	env.OnActivity(c.name, anyArgs(c.extra)...).Return(nil)
}

// Flapping: fails attempts-1 times, succeeds on the last permitted attempt.
func TestPublishCourseWorkflow_FlappingActivity_RecoversWithinItsRetryBudget(t *testing.T) {
	for _, c := range faultCases {
		if c.attempts < 2 {
			continue // ValidateMetadata is not retried by design (noRetry)
		}
		t.Run(c.name, func(t *testing.T) {
			env, in := faultEnv(t, c.name)
			outline := publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID, VersionNumber: 1}
			failTimes(env, c, c.attempts-1, errors.New("connection reset by peer"), &outline)
			env.OnActivity(publishing.ActivityMarkPublishFailed, anyArgs(3)...).Return(nil).Maybe()

			env.ExecuteWorkflow(PublishCourseWorkflow, in)

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("a flapping %s within its %d attempts must not fail the publish: %v", c.name, c.attempts, err)
			}
			env.AssertNumberOfCalls(t, c.name, c.attempts)
			wantPromote := 1
			if c.name == publishing.ActivityPromoteAndPublish {
				wantPromote = c.attempts
			}
			env.AssertNumberOfCalls(t, publishing.ActivityPromoteAndPublish, wantPromote)
			env.AssertNumberOfCalls(t, publishing.ActivityMarkPublishFailed, 0)
		})
	}
}

// Permanent failure: attempted exactly as often as the policy allows, then
// MarkPublishFailed once, naming the activity, with its fixed-vocabulary
// reason; nothing in a later stage runs.
func TestPublishCourseWorkflow_PermanentFailure_StopsAfterTheRetryBudget(t *testing.T) {
	for _, c := range faultCases {
		t.Run(c.name, func(t *testing.T) {
			env, in := faultEnv(t, c.name)
			cause := publishFailure("asset_invalid: asset not found")
			// Failing more often than the budget allows: every attempt fails.
			failTimes(env, c, c.attempts+10, cause, nil)
			env.OnActivity(publishing.ActivityMarkPublishFailed, mock.Anything, in.VersionID, cause.Message(), c.name).Return(nil).Once()

			env.ExecuteWorkflow(PublishCourseWorkflow, in)

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if env.GetWorkflowError() == nil {
				t.Fatalf("%s failing permanently must fail the publish", c.name)
			}
			env.AssertNumberOfCalls(t, c.name, c.attempts)
			env.AssertNumberOfCalls(t, publishing.ActivityMarkPublishFailed, 1)
			for _, later := range faultCases {
				if later.stage > c.stage {
					env.AssertNumberOfCalls(t, later.name, 0)
				}
			}
			// MarkPublishFailed got this activity's name and the fixed reason.
			env.AssertCalled(t, publishing.ActivityMarkPublishFailed, mock.Anything, in.VersionID, cause.Message(), c.name)
		})
	}
}

// Lost ack: PromoteAndPublish's effect happened but its reply never reached the
// workflow (modelled as a transient error after the stub has recorded the
// effect). Temporal retries it with the SAME arguments, the publish finishes,
// and the failure path is never taken.
func TestPublishCourseWorkflow_LostAckOnPromote_IsRetriedWithTheSameArguments(t *testing.T) {
	env, in := faultEnv(t, publishing.ActivityPromoteAndPublish)
	type call struct {
		course, version uuid.UUID
		old             *uuid.UUID
		title           string
	}
	var seen []call
	env.OnActivity(publishing.ActivityPromoteAndPublish, anyArgs(4)...).
		Run(func(args mock.Arguments) {
			seen = append(seen, call{args.Get(1).(uuid.UUID), args.Get(2).(uuid.UUID), args.Get(3).(*uuid.UUID), args.Get(4).(string)})
		}).
		Return(errors.New("rpc error: ack lost")).Times(2)
	env.OnActivity(publishing.ActivityPromoteAndPublish, anyArgs(4)...).
		Run(func(args mock.Arguments) {
			seen = append(seen, call{args.Get(1).(uuid.UUID), args.Get(2).(uuid.UUID), args.Get(3).(*uuid.UUID), args.Get(4).(string)})
		}).
		Return(nil)
	env.OnActivity(publishing.ActivityMarkPublishFailed, anyArgs(3)...).Return(nil).Maybe()

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("publish must finish after a lost ack: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("PromoteAndPublish ran %d times, want 3 (2 lost acks and the success)", len(seen))
	}
	for i, s := range seen {
		if s.course != in.CourseID || s.version != in.VersionID || s.title != in.Title {
			t.Errorf("attempt %d got different arguments: %+v", i, s)
		}
	}
	env.AssertNumberOfCalls(t, publishing.ActivityMarkPublishFailed, 0)
}

// A transient failure of the failure path itself must not change the outcome:
// MarkPublishFailed is retried (retry5x) and the workflow still returns the
// original error, not MarkPublishFailed's.
func TestPublishCourseWorkflow_MarkPublishFailedFlapping_StillReturnsTheOriginalError(t *testing.T) {
	env, in := faultEnv(t, publishing.ActivityValidateMetadata)
	cause := publishFailure("validation_failed: title is required")
	env.OnActivity(publishing.ActivityValidateMetadata, anyArgs(1)...).Return(cause)
	env.OnActivity(publishing.ActivityMarkPublishFailed, anyArgs(3)...).Return(errors.New("connection refused")).Times(2)
	env.OnActivity(publishing.ActivityMarkPublishFailed, anyArgs(3)...).Return(nil)

	env.ExecuteWorkflow(PublishCourseWorkflow, in)

	if env.GetWorkflowError() == nil {
		t.Fatal("the publish must fail with ValidateMetadata's error")
	}
	env.AssertNumberOfCalls(t, publishing.ActivityMarkPublishFailed, 3)
	env.AssertNumberOfCalls(t, publishing.ActivityPromoteAndPublish, 0)
}
