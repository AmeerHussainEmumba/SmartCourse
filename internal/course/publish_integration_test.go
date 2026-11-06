//go:build integration

// What this file checks, against real Postgres (RFC §18.2): Service.Publish
// itself — the method that calls client.Client.ExecuteWorkflow — which
// every other test in this package deliberately bypasses (see
// runPublishPipeline's doc comment in service_integration_test.go). A fake
// client.Client stands in for Temporal here (not a real server — that's
// internal/workflows/publish_workflow_integration_test.go's job, and
// cmd/seed's make-seed smoke test for the full live wire-up): this file is
// about Publish's own logic — the workflow ID it constructs, that it calls
// ExecuteWorkflow with the right options, that it writes workflow_id/state
// to Postgres afterward (SetVersionPublishing's real effect, not just the
// in-memory response), and that an ExecuteWorkflow failure becomes a clean
// apperrors.ErrUnavailable without ever touching the database.
package course_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

// fakeTemporalClient implements client.Client by embedding the real
// interface (nil) and overriding only ExecuteWorkflow — the one method
// Service.Publish actually calls. Any other method call would panic on the
// nil embedded interface, which is fine: if Publish ever starts calling
// something else, this test should fail loudly, not silently no-op.
type fakeTemporalClient struct {
	temporalclient.Client
	executeWorkflow func(ctx context.Context, options temporalclient.StartWorkflowOptions, workflow any, args ...any) (temporalclient.WorkflowRun, error)
}

func (f *fakeTemporalClient) ExecuteWorkflow(ctx context.Context, options temporalclient.StartWorkflowOptions, workflow any, args ...any) (temporalclient.WorkflowRun, error) {
	return f.executeWorkflow(ctx, options, workflow, args...)
}

type fakeWorkflowRun struct{ id string }

func (f *fakeWorkflowRun) GetID() string                  { return f.id }
func (f *fakeWorkflowRun) GetRunID() string               { return "" }
func (f *fakeWorkflowRun) GetFirstExecutionRunID() string { return "" }
func (f *fakeWorkflowRun) Get(context.Context, any) error { return nil }
func (f *fakeWorkflowRun) GetWithOptions(context.Context, any, temporalclient.WorkflowRunGetOptions) error {
	return nil
}

func TestPublish_CallsExecuteWorkflowAndPersistsState(t *testing.T) {
	env := testenv.Setup(t)
	instructorID := seedInstructor(t, env)
	ctx := context.Background()

	var gotOptions temporalclient.StartWorkflowOptions
	var gotWorkflow any
	called := false
	fake := &fakeTemporalClient{
		executeWorkflow: func(_ context.Context, options temporalclient.StartWorkflowOptions, workflow any, _ ...any) (temporalclient.WorkflowRun, error) {
			called = true
			gotOptions = options
			gotWorkflow = workflow
			return &fakeWorkflowRun{id: options.ID}, nil
		},
	}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "course-publish")

	courseResp, _, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: "Publish Me", Slug: "publish-me"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)
	mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "M1", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	if _, err := svc.AddLesson(ctx, courseID, instructorID, uuid.MustParse(mod.ID), course.CreateLessonRequest{
		Title: "L1", Position: 1, ContentType: "text", Content: map[string]any{"body": "x"}, DurationS: 30,
	}); err != nil {
		t.Fatalf("AddLesson: %v", err)
	}

	accepted, err := svc.Publish(ctx, courseID, instructorID)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !called {
		t.Fatal("expected ExecuteWorkflow to be called")
	}

	wantID := fmt.Sprintf("publish-%s-%d", courseID, 1) // first draft is always version 1
	if gotOptions.ID != wantID {
		t.Fatalf("expected workflow ID %q, got %q", wantID, gotOptions.ID)
	}
	if gotOptions.TaskQueue != "course-publish" {
		t.Fatalf("expected task queue 'course-publish', got %q", gotOptions.TaskQueue)
	}
	if gotWorkflow != course.PublishWorkflowName {
		t.Fatalf("expected workflow name %q, got %v", course.PublishWorkflowName, gotWorkflow)
	}
	if accepted.WorkflowID != wantID || accepted.State != "publishing" {
		t.Fatalf("unexpected accepted response: %+v", accepted)
	}

	// The real effect SetVersionPublishing is supposed to have — not just
	// the in-memory response.
	var state, workflowID string
	if err := env.DB.Raw(`SELECT state, workflow_id FROM course_versions WHERE id = ?`, uuid.MustParse(*courseResp.DraftVersionID)).
		Row().Scan(&state, &workflowID); err != nil {
		t.Fatalf("read course_versions: %v", err)
	}
	if state != "publishing" || workflowID != wantID {
		t.Fatalf("expected persisted state 'publishing' and workflow_id %q, got state=%q workflow_id=%q", wantID, state, workflowID)
	}
}

func TestPublish_NoDraft_ReturnsErrorWithoutCallingTemporal(t *testing.T) {
	env := testenv.Setup(t)
	instructorID := seedInstructor(t, env)
	ctx := context.Background()

	fake := &fakeTemporalClient{
		executeWorkflow: func(context.Context, temporalclient.StartWorkflowOptions, any, ...any) (temporalclient.WorkflowRun, error) {
			t.Fatal("ExecuteWorkflow must not be called when there is no draft to publish")
			return nil, nil
		},
	}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "course-publish")

	// Build a course with a draft, publish it via the real pipeline (a
	// second, nil-Temporal service sharing the same DB — this file isn't
	// testing the pipeline itself), leaving the course with no draft.
	pipelineSvc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	courseResp, _, err := pipelineSvc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: "Already Published", Slug: "already-published"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)
	mod, err := pipelineSvc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "M1", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	if _, err := pipelineSvc.AddLesson(ctx, courseID, instructorID, uuid.MustParse(mod.ID), course.CreateLessonRequest{
		Title: "L1", Position: 1, ContentType: "text", Content: map[string]any{"body": "x"}, DurationS: 30,
	}); err != nil {
		t.Fatalf("AddLesson: %v", err)
	}
	runPublishPipeline(t, pipelineSvc, courseID, instructorID)

	if _, err := svc.Publish(ctx, courseID, instructorID); err == nil {
		t.Fatal("expected Publish to reject a course with no draft")
	} else if appErr, ok := apperrors.As(err); !ok || appErr.Code != "no_draft" {
		t.Fatalf("expected no_draft error, got: %v", err)
	}
}

func TestPublish_ExecuteWorkflowFails_ReturnsUnavailableWithoutPersisting(t *testing.T) {
	env := testenv.Setup(t)
	instructorID := seedInstructor(t, env)
	ctx := context.Background()

	fake := &fakeTemporalClient{
		executeWorkflow: func(context.Context, temporalclient.StartWorkflowOptions, any, ...any) (temporalclient.WorkflowRun, error) {
			return nil, fmt.Errorf("temporal unreachable")
		},
	}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "course-publish")

	courseResp, _, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: "Unreachable", Slug: "unreachable"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)
	mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "M1", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	if _, err := svc.AddLesson(ctx, courseID, instructorID, uuid.MustParse(mod.ID), course.CreateLessonRequest{
		Title: "L1", Position: 1, ContentType: "text", Content: map[string]any{"body": "x"}, DurationS: 30,
	}); err != nil {
		t.Fatalf("AddLesson: %v", err)
	}

	if _, err := svc.Publish(ctx, courseID, instructorID); err == nil {
		t.Fatal("expected Publish to fail when ExecuteWorkflow fails")
	} else if appErr, ok := apperrors.As(err); !ok || appErr.Code != "temporarily_unavailable" {
		t.Fatalf("expected temporarily_unavailable error, got: %v", err)
	}

	var state string
	var workflowID *string
	if err := env.DB.Raw(`SELECT state, workflow_id FROM course_versions WHERE id = ?`, uuid.MustParse(*courseResp.DraftVersionID)).
		Row().Scan(&state, &workflowID); err != nil {
		t.Fatalf("read course_versions: %v", err)
	}
	if state != "draft" || workflowID != nil {
		t.Fatalf("expected the draft to be untouched after a failed ExecuteWorkflow call, got state=%q workflow_id=%v", state, workflowID)
	}
}
