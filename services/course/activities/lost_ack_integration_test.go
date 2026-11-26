//go:build integration

// What this file checks, against real Postgres and Redis: the lost-ack case of
// the publish workflow's fault injection. Temporal re-runs an activity whose
// effect committed but whose reply never reached the workflow (retry policy in
// services/publishing/publish/workflow.go); the workflow-level half of that
// (the retry happens, with the same arguments, and the publish finishes) is
// services/publishing/publish/faults_test.go. This is the data-side half: the
// real activities run a second time over their own committed effect and
// change nothing. It restores a scenario of the retired in-service workflow's
// tests, on the split topology: no Temporal here, the activities are called
// directly the way a retry calls them.
package activities_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/activities"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

func count(t *testing.T, db *gorm.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.Raw(query, args...).Row().Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Every activity of the graph is repeated after it succeeded (its ack "lost"):
// the repeat succeeds and the observable state is exactly what one run left:
// one search row, one course_published event, the version ready.
func TestActivities_LostAck_RepeatingEveryActivityLeavesOneEffect(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	acts := activities.NewActivities(svc)
	courseID, versionID, _ := draftWithOneLesson(t, env, svc, "acts-lost-ack")
	ctx := context.Background()
	in := publishing.PublishWorkflowInput{CourseID: courseID, VersionID: versionID, Title: "Go Concurrency"}

	// Run each activity twice, in graph order, as a retry after a lost ack does.
	twice := func(name string, f func() error) {
		t.Helper()
		for attempt := 1; attempt <= 2; attempt++ {
			if err := f(); err != nil {
				t.Fatalf("%s, attempt %d: %v", name, attempt, err)
			}
		}
	}
	twice(publishing.ActivityValidateMetadata, func() error { return acts.ValidateMetadata(ctx, versionID) })
	twice(publishing.ActivityVerifyAssets, func() error { return acts.VerifyAssets(ctx, versionID) })
	twice(publishing.ActivityComputeMetadata, func() error { return acts.ComputeMetadata(ctx, versionID) })
	var outline *publishing.PublishOutline
	twice(publishing.ActivityBuildOutline, func() (err error) { outline, err = acts.BuildOutline(ctx, courseID, versionID); return })
	twice(publishing.ActivityIndexSearch, func() error { return acts.IndexSearch(ctx, *outline) })
	twice(publishing.ActivityWarmCache, func() error { return acts.WarmCache(ctx, *outline) })
	twice(publishing.ActivityPromoteAndPublish, func() error {
		return acts.PromoteAndPublish(ctx, in.CourseID, in.VersionID, in.OldVersionID, in.Title)
	})

	if n := count(t, env.DB, `SELECT count(*) FROM course_search_index WHERE course_id = ?`, courseID); n != 1 {
		t.Errorf("%d search rows after repeated runs, want 1", n)
	}
	if n := count(t, env.DB, `SELECT count(*) FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, courseID); n != 1 {
		t.Errorf("%d course_published events after repeated runs, want 1", n)
	}
	if n := count(t, env.DB, `SELECT count(*) FROM course_versions WHERE id = ? AND state = 'ready'`, versionID); n != 1 {
		t.Errorf("version not ready after repeated runs")
	}
	if n := count(t, env.DB, `SELECT count(*) FROM courses WHERE id = ? AND published_version_id = ?`, courseID, versionID); n != 1 {
		t.Errorf("published_version_id not the promoted version")
	}
}

// MarkPublishFailed is the activity the workflow runs on the failure path; its
// ack can be lost too. A repeat leaves one publish_failed event and the same
// state and reason, not a second event and not an error that would turn into a
// stuck workflow.
func TestActivities_LostAck_MarkPublishFailedRepeatedWritesOneEvent(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	acts := activities.NewActivities(svc)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	cResp, vResp, err := svc.CreateCourse(ctx, instr, course.CreateCourseRequest{Title: "Lost Ack Fail", Slug: "acts-lost-ack-fail", Description: "d"})
	if err != nil {
		t.Fatal(err)
	}
	courseID, versionID := uuid.MustParse(cResp.ID), uuid.MustParse(vResp.ID)

	for attempt := 1; attempt <= 3; attempt++ {
		if err := acts.MarkPublishFailed(ctx, versionID, "asset_invalid: asset not found", publishing.ActivityVerifyAssets); err != nil {
			t.Fatalf("MarkPublishFailed attempt %d: %v", attempt, err)
		}
	}
	if n := count(t, env.DB, `SELECT count(*) FROM outbox WHERE event_type = 'course.publish_failed' AND aggregate_id = ?`, courseID); n != 1 {
		t.Errorf("%d publish_failed events after 3 identical calls, want 1", n)
	}
	var state, reason string
	if err := env.DB.Raw(`SELECT state::text, publish_error FROM course_versions WHERE id = ?`, versionID).Row().Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "publish_failed" || reason != "asset_invalid: asset not found" {
		t.Errorf("state=%q publish_error=%q", state, reason)
	}
}
