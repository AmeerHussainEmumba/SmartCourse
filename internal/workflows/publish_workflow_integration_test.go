//go:build integration

// What this file checks, against real Postgres and Redis (RFC §18.2):
// that PublishCourseWorkflow's activities, wired to a REAL course.Service
// (not mocked — see publish_workflow_test.go for where mocking is the
// right call instead), actually produce the effects RFC §7.4 promises: the
// version ends up `ready`, course_search_index has a real row, and the
// version-scoped Redis cache key exists. Runs via Temporal's own
// TestWorkflowEnvironment, not a live Temporal server (ADR-0025 explains
// why: the SDK's own in-process test harness runs real activity code
// without needing a container, and a full client/worker/server round trip
// is exercised manually instead — `make dev-workflow && make seed`, which
// fails loudly if that wiring is broken).
package workflows_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"go.temporal.io/sdk/testsuite"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/workflows"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func TestPublishCourseWorkflow_RealActivities_PromotesIndexesAndCachesTheCourse(t *testing.T) {
	env := testenv.Setup(t)
	ctx := context.Background()

	instructorID := uuid.Must(uuid.NewV7())
	if err := env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Workflow Instructor', 'instructor', 'active')`,
		instructorID, instructorID.String()+"@example.com").Error; err != nil {
		t.Fatalf("seed instructor: %v", err)
	}
	if err := env.DB.Exec(`INSERT INTO instructor_mirror (user_id, full_name) VALUES (?, 'Workflow Instructor')`, instructorID).Error; err != nil {
		t.Fatalf("seed instructor_mirror: %v", err)
	}

	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	courseResp, versionResp, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{
		Title: "Workflow Course", Slug: "workflow-course", Description: "Published via the real workflow",
	})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)
	versionID := uuid.MustParse(versionResp.ID)

	mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "Basics", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	if _, err := svc.AddLesson(ctx, courseID, instructorID, uuid.MustParse(mod.ID), course.CreateLessonRequest{
		Title: "Hello", Position: 1, ContentType: "text", Content: map[string]any{"body": "x"}, DurationS: 60,
	}); err != nil {
		t.Fatalf("AddLesson: %v", err)
	}

	var suite testsuite.WorkflowTestSuite
	testEnv := suite.NewTestWorkflowEnvironment()
	testEnv.RegisterActivity(workflows.NewActivities(svc))

	testEnv.ExecuteWorkflow(workflows.PublishCourseWorkflow, course.PublishWorkflowInput{
		CourseID: courseID, VersionID: versionID, Title: "Workflow Course",
	})

	if !testEnv.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := testEnv.GetWorkflowError(); err != nil {
		t.Fatalf("expected no workflow error, got: %v", err)
	}

	status, err := svc.GetPublishStatus(ctx, courseID, instructorID)
	if err != nil {
		t.Fatalf("GetPublishStatus: %v", err)
	}
	if status.State != "ready" {
		t.Fatalf("expected state 'ready' after the workflow completes, got %q (error: %v)", status.State, status.Error)
	}

	var searchRows int64
	env.DB.Raw(`SELECT count(*) FROM course_search_index WHERE course_id = ?`, courseID).Row().Scan(&searchRows)
	if searchRows != 1 {
		t.Fatalf("expected IndexSearch to write exactly 1 course_search_index row, got %d", searchRows)
	}

	cacheKey := "course:detail:" + courseID.String() + ":v1"
	cached, err := env.RDB.Get(ctx, cacheKey).Result()
	if err != nil {
		t.Fatalf("expected WarmCache to write %q: %v", cacheKey, err)
	}

	// Regression test for a bug caught live (`make dev-workflow` + a real
	// publish): BuildOutline runs BEFORE PromoteAndPublish, so the Course
	// and Version it reads are still in their pre-promotion shape
	// (draft_version_id still set, version state "publishing"). Caching
	// that verbatim meant the very first GetPublishedDetail cache hit
	// after a publish served a course that still looked like it hadn't
	// been published — the version's state visibly wrong, and a
	// draft_version_id present on an already-live course. The cached
	// payload must instead reflect what the row becomes the instant
	// PromoteAndPublish's transaction commits.
	var detail course.CourseDetailResponse
	if err := json.Unmarshal([]byte(cached), &detail); err != nil {
		t.Fatalf("unmarshal cached detail: %v", err)
	}
	if detail.Version.State != "ready" {
		t.Fatalf("expected cached detail's version state to be 'ready' (post-promotion), got %q", detail.Version.State)
	}
	if detail.Course.DraftVersionID != nil {
		t.Fatalf("expected cached detail to have no draft_version_id (post-promotion), got %q", *detail.Course.DraftVersionID)
	}
	if detail.Course.PublishedVersionID == nil || *detail.Course.PublishedVersionID != versionID.String() {
		t.Fatalf("expected cached detail's published_version_id to be the newly-promoted version, got %+v", detail.Course.PublishedVersionID)
	}
}
