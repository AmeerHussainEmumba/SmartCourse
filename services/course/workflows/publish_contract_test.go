// The publishing contract (libs/contracts/publishing, docs/plans/microservices-split.md
// §5) as the workflow honours it: it calls every activity by NAME, and schedules
// each on the activity queue, not on its own. The activities here are plain
// functions registered under the contract's names, with no dependency on the
// Activities type, which is what the orchestrating service will have.
package workflows

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
)

// byName registers one recording activity per contract name and returns what
// ran, in order, with the task queue each was scheduled on.
type byName struct {
	mu     sync.Mutex
	ran    []string
	queues map[string]string
}

func registerByName(env *testsuite.TestWorkflowEnvironment, outline publishing.PublishOutline) *byName {
	rec := &byName{queues: map[string]string{}}
	note := func(ctx context.Context, name string) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.ran = append(rec.ran, name)
		rec.queues[name] = activity.GetInfo(ctx).TaskQueue
	}
	reg := func(name string, fn any) { env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name}) }
	reg(publishing.ActivityValidateMetadata, func(ctx context.Context, _ uuid.UUID) error {
		note(ctx, publishing.ActivityValidateMetadata)
		return nil
	})
	reg(publishing.ActivityVerifyAssets, func(ctx context.Context, _ uuid.UUID) error {
		note(ctx, publishing.ActivityVerifyAssets)
		return nil
	})
	reg(publishing.ActivityComputeMetadata, func(ctx context.Context, _ uuid.UUID) error {
		note(ctx, publishing.ActivityComputeMetadata)
		return nil
	})
	reg(publishing.ActivityBuildOutline, func(ctx context.Context, _, _ uuid.UUID) (*publishing.PublishOutline, error) {
		note(ctx, publishing.ActivityBuildOutline)
		return &outline, nil
	})
	reg(publishing.ActivityIndexSearch, func(ctx context.Context, o publishing.PublishOutline) error {
		note(ctx, publishing.ActivityIndexSearch)
		return nil
	})
	reg(publishing.ActivityWarmCache, func(ctx context.Context, o publishing.PublishOutline) error {
		note(ctx, publishing.ActivityWarmCache)
		return nil
	})
	reg(publishing.ActivityPromoteAndPublish, func(ctx context.Context, _, _ uuid.UUID, _ *uuid.UUID, _ string) error {
		note(ctx, publishing.ActivityPromoteAndPublish)
		return nil
	})
	reg(publishing.ActivityMarkPublishFailed, func(ctx context.Context, _ uuid.UUID, _, _ string) error {
		note(ctx, publishing.ActivityMarkPublishFailed)
		return nil
	})
	return rec
}

func TestPublishCourseWorkflow_CallsActivitiesByNameOnTheActivityQueue(t *testing.T) {
	for _, tc := range []struct {
		name, queue, want string
	}{
		{"default queue", "", publishing.ActivityQueue},
		{"starter override", "custom-activities", "custom-activities"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			in := publishing.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "T", ActivityQueue: tc.queue}
			rec := registerByName(env, publishing.PublishOutline{CourseID: in.CourseID, VersionID: in.VersionID, VersionNumber: 1})

			env.ExecuteWorkflow(PublishCourseWorkflow, in)
			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}

			ran := append([]string(nil), rec.ran...)
			sort.Strings(ran)
			want := []string{
				publishing.ActivityBuildOutline, publishing.ActivityComputeMetadata, publishing.ActivityIndexSearch,
				publishing.ActivityPromoteAndPublish, publishing.ActivityValidateMetadata, publishing.ActivityVerifyAssets,
				publishing.ActivityWarmCache,
			}
			if len(ran) != len(want) {
				t.Fatalf("activities run = %v, want %v", ran, want)
			}
			for i := range want {
				if ran[i] != want[i] {
					t.Fatalf("activities run = %v, want %v", ran, want)
				}
			}
			for name, q := range rec.queues {
				if q != tc.want {
					t.Errorf("activity %s ran on queue %q, want %q", name, q, tc.want)
				}
			}
		})
	}
}

// A failing activity still reaches MarkPublishFailed, by name and on the same
// queue.
func TestPublishCourseWorkflow_FailurePathUsesTheActivityQueueToo(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	in := publishing.PublishWorkflowInput{CourseID: uuid.New(), VersionID: uuid.New(), Title: "T"}
	rec := registerByName(env, publishing.PublishOutline{})
	env.RegisterActivityWithOptions(func(ctx context.Context, _ uuid.UUID) error {
		note := activity.GetInfo(ctx).TaskQueue
		rec.mu.Lock()
		rec.queues["validate-failed"] = note
		rec.mu.Unlock()
		return publishFailure("asset_invalid: nope")
	}, activity.RegisterOptions{Name: publishing.ActivityValidateMetadata, DisableAlreadyRegisteredCheck: true})

	env.ExecuteWorkflow(PublishCourseWorkflow, in)
	if env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.queues[publishing.ActivityMarkPublishFailed] != publishing.ActivityQueue {
		t.Errorf("MarkPublishFailed queue = %q (ran: %v)", rec.queues[publishing.ActivityMarkPublishFailed], rec.ran)
	}
	if rec.queues["validate-failed"] != publishing.ActivityQueue {
		t.Errorf("failing activity queue = %q", rec.queues["validate-failed"])
	}
}
