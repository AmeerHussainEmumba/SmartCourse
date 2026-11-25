//go:build integration

package activities_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/activities"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// The real Activities against real Postgres and Redis, called directly in the
// order of the publish graph. The workflow that sequences them lives in
// services/publishing; the combined path is proven by the container E2E tests.

func draftWithOneLesson(t *testing.T, env *testenv.Env, svc *course.Service, slug string) (courseID, versionID, instr uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	instr = seedInstructor(t, env)
	cResp, vResp, err := svc.CreateCourse(ctx, instr, course.CreateCourseRequest{Title: "Go Concurrency", Slug: slug, Description: "d"})
	if err != nil {
		t.Fatal(err)
	}
	courseID, versionID = uuid.MustParse(cResp.ID), uuid.MustParse(vResp.ID)
	mod, err := svc.AddModule(ctx, courseID, instr, course.CreateModuleRequest{Title: "M", Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddLesson(ctx, courseID, instr, uuid.MustParse(mod.ID), course.CreateLessonRequest{Title: "L", Position: 1, ContentType: "text", Content: map[string]any{"b": "x"}, DurationS: 5}); err != nil {
		t.Fatal(err)
	}
	return courseID, versionID, instr
}

func TestActivities_FullGraph_PromotesIndexesAndCachesTheCourse(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	acts := activities.NewActivities(svc)
	courseID, versionID, _ := draftWithOneLesson(t, env, svc, "acts-happy")

	in := publishing.PublishWorkflowInput{CourseID: courseID, VersionID: versionID, Title: "Go Concurrency"}
	if err := runPublish(t, acts, in); err != nil {
		t.Fatalf("publish: %v", err)
	}
	var state string
	env.DB.Raw(`SELECT state::text FROM course_versions WHERE id = ?`, versionID).Row().Scan(&state)
	if state != "ready" {
		t.Fatalf("version state %q, want ready", state)
	}
	var indexed int
	env.DB.Raw(`SELECT count(*) FROM course_search_index WHERE course_id = ?`, courseID).Row().Scan(&indexed)
	if indexed != 1 {
		t.Fatalf("%d search rows, want 1", indexed)
	}
	if d, err := svc.GetPublishedDetail(context.Background(), "acts-happy"); err != nil || d.Version.Title != "Go Concurrency" {
		t.Fatalf("published detail: %v %+v", err, d)
	}
	var events int
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, courseID).Row().Scan(&events)
	if events != 1 {
		t.Fatalf("%d course_published rows in course.outbox, want 1", events)
	}
}

func TestActivities_PromoteAndPublish_RetryIsANoOp(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	acts := activities.NewActivities(svc)
	courseID, versionID, _ := draftWithOneLesson(t, env, svc, "acts-retry")

	in := publishing.PublishWorkflowInput{CourseID: courseID, VersionID: versionID, Title: "Go Concurrency"}
	if err := runPublish(t, acts, in); err != nil {
		t.Fatal(err)
	}
	// Temporal retries an activity whose ack was lost: the second call must not
	// emit a second event.
	if err := acts.PromoteAndPublish(context.Background(), courseID, versionID, nil, "Go Concurrency"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	var events int
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, courseID).Row().Scan(&events)
	if events != 1 {
		t.Fatalf("%d course_published rows after a retry, want 1", events)
	}
}

func TestActivities_ValidateFailure_IsABoundedPublishFailureAndRecorded(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	acts := activities.NewActivities(svc)
	instr := seedInstructor(t, env)
	cResp, vResp, err := svc.CreateCourse(context.Background(), instr, course.CreateCourseRequest{Title: "Empty", Slug: "acts-empty", Description: "d"})
	if err != nil {
		t.Fatal(err)
	}
	courseID, versionID := uuid.MustParse(cResp.ID), uuid.MustParse(vResp.ID)

	// runPublish asserts the error is an ApplicationError of the contract's
	// FailureType and records it with MarkPublishFailed.
	if err := runPublish(t, acts, publishing.PublishWorkflowInput{CourseID: courseID, VersionID: versionID, Title: "Empty"}); err == nil {
		t.Fatal("publishing a draft with no modules succeeded")
	}
	var state string
	var perr *string
	env.DB.Raw(`SELECT state::text, publish_error FROM course_versions WHERE id = ?`, versionID).Row().Scan(&state, &perr)
	if state != "publish_failed" || perr == nil || *perr == "" {
		t.Fatalf("state=%q publish_error=%v, want publish_failed with a reason", state, perr)
	}
	var events int
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = 'course.publish_failed' AND aggregate_id = ?`, courseID).Row().Scan(&events)
	if events != 1 {
		t.Fatalf("%d publish_failed rows in course.outbox, want 1", events)
	}
}
