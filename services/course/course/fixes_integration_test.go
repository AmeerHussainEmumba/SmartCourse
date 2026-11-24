//go:build integration

// Reproducers for real defects in the course service found by adversarial
// testing. Regression tests for fixed defects.
package course_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// repository.go SearchCourses filters only on archived_at; it never checks
// courses.published_version_id. IndexSearch runs BEFORE PromoteAndPublish in
// the workflow (and concurrently with WarmCache), so:
//   - a course whose publish fails after IndexSearch (WarmCache or Promote
//     exhausting retries) stays in GET /courses/search forever, while its
//     detail/catalogue 404 (never published);
//   - during a republish, search serves the NEW version's title/description
//     while catalogue/detail still serve the old one (a half-published read
//     that ADR-0010's blue-green is meant to rule out).
func TestBugrepro_SearchIndexesUnpublishedCourse_BeforePromote(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, draft := newCourse(t, svc, instr, "zebra-quantum", "Zebra Quantum Mechanics")
	authorDraft(t, svc, courseID, instr, 1, 1)

	outline, err := svc.BuildOutline(ctx, courseID, draft)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.IndexSearch(ctx, outline); err != nil {
		t.Fatal(err)
	}
	// the workflow now fails (e.g. WarmCache exhausts its retries) -> MarkPublishFailed
	if err := svc.MarkPublishFailed(ctx, draft, "warm cache failed", "WarmCache"); err != nil {
		t.Fatal(err)
	}

	if _, err := svc.GetPublishedDetail(ctx, "zebra-quantum"); err == nil {
		t.Fatal("precondition: course must not be readable (never published)")
	}
	res, err := svc.SearchCourses(ctx, "Zebra", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Errorf("search returns %d result(s) for a course that was never published (slug %q); detail returns 404 for it", len(res), res[0].Slug)
	}
}

func TestBugrepro_SearchServesNewVersionMetadata_BeforePromote(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, v1 := newCourse(t, svc, instr, "walrus", "Walrus Basics")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)
	v2 := startVersion2(t, env, courseID, "Narwhal Advanced")
	authorDraft(t, svc, courseID, instr, 1, 1)
	outline, err := svc.BuildOutline(ctx, courseID, v2)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.IndexSearch(ctx, outline); err != nil { // not yet promoted
		t.Fatal(err)
	}
	d, _ := svc.GetPublishedDetail(ctx, "walrus")
	if d == nil || d.Version.ID != v1.String() {
		t.Fatal("precondition: v1 must still be the live version")
	}
	res, _ := svc.SearchCourses(ctx, "Narwhal", 10)
	if len(res) != 0 {
		t.Errorf("search already serves v2's title %q while detail/catalogue still serve v1 %q", res[0].Title, d.Version.Title)
	}
}

// GetPublishedDetail caches the whole detail payload for 24h, including
// course.enrollment_count and course.enrollment_limit, which change on every
// enrollment/withdrawal and are NOT version data. The catalogue shows the
// live count; the detail page keeps showing the count frozen at publish time.
func TestBugrepro_CachedDetailServesStaleEnrollmentCount(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, _ := newCourse(t, svc, instr, "stale-count", "Stale Count")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)
	if _, err := svc.GetPublishedDetail(ctx, "stale-count"); err != nil {
		t.Fatal(err)
	}
	// seven students enroll (enrollment.Enroll's effect on courses)
	if err := env.DB.Exec(`UPDATE courses SET enrollment_count = 7, enrollment_limit = 9 WHERE id = ?`, courseID).Error; err != nil {
		t.Fatal(err)
	}
	cat, err := svc.ListCatalogue(ctx, 10, "", "", "", "")
	if err != nil || len(cat.Items) != 1 || cat.Items[0].EnrollmentCount != 7 {
		t.Fatalf("precondition: catalogue shows the live count: %v %+v", err, cat)
	}
	d, err := svc.GetPublishedDetail(ctx, "stale-count")
	if err != nil {
		t.Fatal(err)
	}
	if d.Course.EnrollmentCount != 7 {
		t.Errorf("detail shows enrollment_count=%d while catalogue and database say 7 (served from the 24h cache)", d.Course.EnrollmentCount)
	}
	if d.Course.EnrollmentLimit == nil || *d.Course.EnrollmentLimit != 9 {
		t.Errorf("detail shows a stale enrollment_limit: %v", d.Course.EnrollmentLimit)
	}
}

// PromoteAndPublish's idempotency guard is check-then-act outside the
// transaction ("must be safe to run twice" per its comment). Two executions in
// flight at once (Temporal retries an attempt that timed out while the first
// is still running) both pass the guard and each writes a course_published
// event. Determinism: the test holds the course row lock so both are parked
// after their guard read, then releases.
func TestBugrepro_ConcurrentPromoteAndPublish_EmitsDuplicateCoursePublishedEvents(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, draft := newCourse(t, svc, instr, "double-promote", "Double Promote")
	authorDraft(t, svc, courseID, instr, 1, 1)

	hold := env.DB.Begin()
	if err := hold.Exec(`SELECT 1 FROM courses WHERE id = ? FOR UPDATE`, courseID).Error; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.PromoteAndPublish(ctx, courseID, draft, nil, "Double Promote")
		}()
	}
	deadline := time.Now().Add(15 * time.Second)
	for count(t, env, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND (query ILIKE 'UPDATE%courses%' OR query ILIKE '%FROM "courses"%FOR UPDATE%')`) < 2 {
		if time.Now().After(deadline) {
			hold.Rollback()
			t.Fatal("both promotions never parked on the row lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	hold.Commit()
	wg.Wait()

	if n := count(t, env, `SELECT count(*) FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, courseID); n != 1 {
		t.Errorf("%d course.course_published events for ONE promotion", n)
	}
}

// Service.Publish never looks at archived_at. An archived course can be
// published again; PromoteAndPublish then sets published_version_id and emits
// course.course_published for a course nobody can see or enroll in
// (analytics.course_mirror counts it as published).
func TestBugrepro_PublishAcceptedForArchivedCourse(t *testing.T) {
	env := testenv.Setup(t)
	instr := seedInstructor(t, env)
	ctx := context.Background()
	var started int
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		started++
		return &fakeWorkflowRun{id: o.ID}, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	courseID, _ := newCourse(t, svc, instr, "publish-archived", "Publish Archived")
	authorDraft(t, svc, courseID, instr, 1, 1)
	if err := svc.ArchiveCourse(ctx, courseID, instr); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Publish(ctx, courseID, instr); err == nil {
		t.Errorf("Publish accepted an archived course and started a workflow (%d started)", started)
	}
}

// ValidateMetadata requires >= 1 module but not >= 1 lesson. A published
// course with an empty module has total_lessons=0, and progress.isFullyComplete
// requires totalLessons > 0, so no student can ever complete it.
func TestBugrepro_CourseWithEmptyModuleCanBePublished(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	_, draft := newCourse(t, svc, instr, "empty-module", "Empty Module")
	courseID := func() uuid.UUID {
		var id uuid.UUID
		env.DB.Raw(`SELECT course_id FROM course_versions WHERE id = ?`, draft).Row().Scan(&id)
		return id
	}()
	if _, err := svc.AddModule(ctx, courseID, instr, course.CreateModuleRequest{Title: "Empty", Position: 1}); err != nil {
		t.Fatal(err)
	}
	if err := svc.ValidateMetadata(ctx, draft); err == nil {
		t.Errorf("a draft whose only module has zero lessons passes ValidateMetadata; publishing it yields a course that can never be completed")
	}
}
