//go:build integration

package workflows_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.temporal.io/sdk/testsuite"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/assets"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/internal/miniotest"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/workflows"
)

// The real VerifyAssets, against a real MinIO, inside the publish workflow:
// a draft that references an asset which was never uploaded fails the publish
// cleanly (publish_failed with a readable error, live version untouched, no
// course_published event), and a draft whose asset was uploaded publishes.

func runPublish(t *testing.T, svc *course.Service, in course.PublishWorkflowInput) error {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	te := suite.NewTestWorkflowEnvironment()
	te.RegisterActivity(workflows.NewActivities(svc))
	te.ExecuteWorkflow(workflows.PublishCourseWorkflow, in)
	if !te.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	return te.GetWorkflowError()
}

func TestPublishWorkflow_MissingAsset_FailsCleanly_LiveVersionUntouched(t *testing.T) {
	env := testenv.Setup(t)
	m := miniotest.Setup(t)
	store, err := assets.New(m.Config)
	if err != nil {
		t.Fatal(err)
	}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "").WithAssetStore(store)
	ctx := context.Background()
	instr := seedWorkflowInstructor(t, env)

	cResp, vResp, err := svc.CreateCourse(ctx, instr, course.CreateCourseRequest{Title: "Live Title", Slug: "asset-live", Description: "d"})
	if err != nil {
		t.Fatal(err)
	}
	courseID, v1 := uuid.MustParse(cResp.ID), uuid.MustParse(vResp.ID)
	mod, err := svc.AddModule(ctx, courseID, instr, course.CreateModuleRequest{Title: "M", Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddLesson(ctx, courseID, instr, uuid.MustParse(mod.ID), course.CreateLessonRequest{Title: "L", Position: 1, ContentType: "text", Content: map[string]any{"b": "x"}, DurationS: 5}); err != nil {
		t.Fatal(err)
	}
	if err := runPublish(t, svc, course.PublishWorkflowInput{CourseID: courseID, VersionID: v1, Title: "Live Title"}); err != nil {
		t.Fatalf("first publish: %v", err)
	}

	// A second draft whose lesson references an asset nobody uploaded.
	v2 := uuid.Must(uuid.NewV7())
	if err := env.DB.Exec(`INSERT INTO course_versions (id, course_id, version_number, state, title, description, tags) VALUES (?, ?, 2, 'draft', 'Draft Title', 'd', '{}')`, v2, courseID).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.DB.Exec(`UPDATE courses SET draft_version_id = ? WHERE id = ?`, v2, courseID).Error; err != nil {
		t.Fatal(err)
	}
	mod2, err := svc.AddModule(ctx, courseID, instr, course.CreateModuleRequest{Title: "M2", Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	missing := "courses/" + courseID.String() + "/assets/" + uuid.NewString() + ".mp4"
	if _, err := svc.AddLesson(ctx, courseID, instr, uuid.MustParse(mod2.ID), course.CreateLessonRequest{
		Title: "Video", Position: 1, ContentType: "video", Content: map[string]any{}, AssetKey: &missing, DurationS: 60,
	}); err != nil {
		t.Fatal(err)
	}

	err = runPublish(t, svc, course.PublishWorkflowInput{CourseID: courseID, VersionID: v2, OldVersionID: &v1, Title: "Draft Title"})
	if err == nil {
		t.Fatal("publish succeeded although a lesson references a missing asset")
	}

	var state string
	var perr *string
	env.DB.Raw(`SELECT state::text, publish_error FROM course_versions WHERE id = ?`, v2).Row().Scan(&state, &perr)
	if state != "publish_failed" || perr == nil || !strings.Contains(*perr, "never uploaded") {
		t.Fatalf("v2 state=%q publish_error=%v, want publish_failed mentioning the missing upload", state, perr)
	}
	var published *uuid.UUID
	env.DB.Raw(`SELECT published_version_id FROM courses WHERE id = ?`, courseID).Row().Scan(&published)
	if published == nil || *published != v1 {
		t.Fatalf("live version changed: %v, want %s", published, v1)
	}
	var v1State string
	env.DB.Raw(`SELECT state::text FROM course_versions WHERE id = ?`, v1).Row().Scan(&v1State)
	if v1State != "ready" {
		t.Fatalf("live version state %q, want ready", v1State)
	}
	detail, err := svc.GetPublishedDetail(ctx, "asset-live")
	if err != nil || detail.Version.Title != "Live Title" {
		t.Fatalf("students no longer see the live version: %v %+v", err, detail)
	}
	var events int
	env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, courseID).Row().Scan(&events)
	if events != 1 {
		t.Fatalf("%d course_published events, want only the first publish's", events)
	}

	// Upload the asset for real, point the lesson at it, and the same version publishes.
	resp, err := svc.CreateUploadURL(ctx, instr, course.UploadURLRequest{CourseID: courseID.String(), ContentType: "video/mp4", SizeBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	if got := miniotest.Upload(t, resp.UploadURL, resp.Headers, []byte("0123456789abcdef")); got != 200 {
		t.Fatalf("upload status %d", got)
	}
	if err := env.DB.Exec(`UPDATE lessons SET asset_key = ? WHERE asset_key = ?`, resp.AssetKey, missing).Error; err != nil {
		t.Fatal(err)
	}
	if err := runPublish(t, svc, course.PublishWorkflowInput{CourseID: courseID, VersionID: v2, OldVersionID: &v1, Title: "Draft Title"}); err != nil {
		t.Fatalf("publish after uploading the asset: %v", err)
	}
	env.DB.Raw(`SELECT published_version_id FROM courses WHERE id = ?`, courseID).Row().Scan(&published)
	if published == nil || *published != v2 {
		t.Fatalf("published = %v, want %s", published, v2)
	}
}
