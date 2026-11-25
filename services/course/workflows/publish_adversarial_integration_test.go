//go:build integration

// Adversarial workflow tests: the REAL activities (real Postgres/Redis) run
// inside Temporal's test environment, wrapped so individual activities can be
// made to fail, flap, or "commit then lose the ack". The assertions are about
// what the database and Redis look like afterwards: no half-promoted course,
// no duplicate events, a failed publish that can be retried.
package workflows_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"go.temporal.io/sdk/testsuite"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/workflows"
)

// chaosActivities embeds the real activities and overrides the ones a test
// wants to sabotage. Temporal registers the exported methods of the struct,
// promoted ones included, under the same names the workflow dispatches to.
type chaosActivities struct {
	*workflows.Activities
	svc *course.Service

	warmFailFirst      int32 // WarmCache fails this many times, then works
	indexAlwaysFails   bool
	promoteLoseAckOnce bool // PromoteAndPublish commits, then returns an error once
	markAlwaysFails    bool

	warmCalls, indexCalls, promoteCalls, markCalls atomic.Int32
}

var errChaos = errors.New("chaos: injected failure")

func (c *chaosActivities) WarmCache(ctx context.Context, o course.PublishOutline) error {
	if n := c.warmCalls.Add(1); n <= c.warmFailFirst {
		return errChaos
	}
	return c.Activities.WarmCache(ctx, o)
}

func (c *chaosActivities) IndexSearch(ctx context.Context, o course.PublishOutline) error {
	c.indexCalls.Add(1)
	if c.indexAlwaysFails {
		return errChaos
	}
	return c.Activities.IndexSearch(ctx, o)
}

func (c *chaosActivities) PromoteAndPublish(ctx context.Context, courseID, versionID uuid.UUID, old *uuid.UUID, title string) error {
	n := c.promoteCalls.Add(1)
	if err := c.Activities.PromoteAndPublish(ctx, courseID, versionID, old, title); err != nil {
		return err
	}
	if c.promoteLoseAckOnce && n == 1 {
		return errChaos // committed, but the worker "crashed" before acknowledging
	}
	return nil
}

func (c *chaosActivities) MarkPublishFailed(ctx context.Context, versionID uuid.UUID, reason, failedActivity string) error {
	c.markCalls.Add(1)
	if c.markAlwaysFails {
		return errChaos
	}
	return c.Activities.MarkPublishFailed(ctx, versionID, reason, failedActivity)
}

type fixture struct {
	env                *testenv.Env
	svc                *course.Service
	instructor         uuid.UUID
	courseID, draftID  uuid.UUID
	slug, title        string
	chaos              *chaosActivities
	publishedPtr       func() *uuid.UUID
	countPublishedEvts func() int64
}

func newFixture(t *testing.T, withModule bool) *fixture {
	t.Helper()
	env := testenv.Setup(t)
	ctx := context.Background()
	f := &fixture{env: env, instructor: seedWorkflowInstructor(t, env), slug: "chaos-course", title: "Chaos Course"}
	f.svc = course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	c, v, err := f.svc.CreateCourse(ctx, f.instructor, course.CreateCourseRequest{Title: f.title, Slug: f.slug, Description: "d"})
	if err != nil {
		t.Fatal(err)
	}
	f.courseID, f.draftID = uuid.MustParse(c.ID), uuid.MustParse(v.ID)
	if withModule {
		f.addContent(t)
	}
	f.chaos = &chaosActivities{Activities: workflows.NewActivities(f.svc), svc: f.svc}
	f.publishedPtr = func() *uuid.UUID {
		var p *uuid.UUID
		env.DB.Raw(`SELECT published_version_id FROM courses WHERE id = ?`, f.courseID).Row().Scan(&p)
		return p
	}
	f.countPublishedEvts = func() int64 {
		var n int64
		env.DB.Raw(`SELECT count(*) FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, f.courseID).Row().Scan(&n)
		return n
	}
	return f
}

func (f *fixture) addContent(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	mod, err := f.svc.AddModule(ctx, f.courseID, f.instructor, course.CreateModuleRequest{Title: "M", Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.AddLesson(ctx, f.courseID, f.instructor, uuid.MustParse(mod.ID), course.CreateLessonRequest{
		Title: "L", Position: 1, ContentType: "text", Content: map[string]any{"b": "x"}, DurationS: 5,
	}); err != nil {
		t.Fatal(err)
	}
}

// run executes the workflow once against f.chaos and returns its error.
func (f *fixture) run(t *testing.T) error {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	te := suite.NewTestWorkflowEnvironment()
	te.RegisterActivity(f.chaos)
	te.ExecuteWorkflow(workflows.PublishCourseWorkflow, course.PublishWorkflowInput{
		CourseID: f.courseID, VersionID: f.draftID, Title: f.title,
	})
	if !te.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	return te.GetWorkflowError()
}

func (f *fixture) assertPublishedCoherently(t *testing.T) {
	t.Helper()
	p := f.publishedPtr()
	if p == nil || *p != f.draftID {
		t.Fatalf("published_version_id = %v, want %s", p, f.draftID)
	}
	var draft *uuid.UUID
	f.env.DB.Raw(`SELECT draft_version_id FROM courses WHERE id = ?`, f.courseID).Row().Scan(&draft)
	if draft != nil {
		t.Fatalf("draft_version_id still set after promotion: %s", draft)
	}
	var ready int64
	f.env.DB.Raw(`SELECT count(*) FROM course_versions WHERE course_id = ? AND state = 'ready'`, f.courseID).Row().Scan(&ready)
	if ready != 1 {
		t.Fatalf("%d ready versions", ready)
	}
	d, err := f.svc.GetPublishedDetail(context.Background(), f.slug)
	if err != nil || d.Version.ID != f.draftID.String() || len(d.Modules) != 1 {
		t.Fatalf("detail after publish: %v %+v", err, d)
	}
}

func TestAdvWorkflow_FlappingWarmCache_RetriesThenPublishesExactlyOnce(t *testing.T) {
	f := newFixture(t, true)
	f.chaos.warmFailFirst = 2 // retry3x: attempts 1,2 fail, 3 succeeds
	if err := f.run(t); err != nil {
		t.Fatalf("workflow should survive 2 transient WarmCache failures: %v", err)
	}
	if f.chaos.warmCalls.Load() != 3 {
		t.Fatalf("WarmCache called %d times, want 3", f.chaos.warmCalls.Load())
	}
	if f.chaos.markCalls.Load() != 0 {
		t.Fatal("MarkPublishFailed called on a publish that succeeded")
	}
	f.assertPublishedCoherently(t)
	if n := f.countPublishedEvts(); n != 1 {
		t.Fatalf("%d course_published events, want 1", n)
	}
}

func TestAdvWorkflow_PromoteCommitsThenLosesAck_RetryIsNoOp(t *testing.T) {
	f := newFixture(t, true)
	f.chaos.promoteLoseAckOnce = true
	if err := f.run(t); err != nil {
		t.Fatalf("workflow must succeed after the retried promotion: %v", err)
	}
	if f.chaos.promoteCalls.Load() != 2 {
		t.Fatalf("PromoteAndPublish called %d times, want 2 (commit+lost ack, then idempotent retry)", f.chaos.promoteCalls.Load())
	}
	f.assertPublishedCoherently(t)
	if n := f.countPublishedEvts(); n != 1 {
		t.Fatalf("retry after a committed promotion produced %d course_published events, want 1", n)
	}
	var state string
	f.env.DB.Raw(`SELECT state FROM course_versions WHERE id = ?`, f.draftID).Row().Scan(&state)
	if state != "ready" {
		t.Fatalf("version state %q", state)
	}
}

func TestAdvWorkflow_IndexSearchFailsPermanently_LiveCourseUntouched_RetryableAfterFix(t *testing.T) {
	f := newFixture(t, true)
	f.chaos.indexAlwaysFails = true
	err := f.run(t)
	if err == nil {
		t.Fatal("workflow must fail when IndexSearch exhausts its retries")
	}
	if f.chaos.indexCalls.Load() != 5 {
		t.Fatalf("IndexSearch attempts = %d, want 5 (retry5x)", f.chaos.indexCalls.Load())
	}
	if f.chaos.promoteCalls.Load() != 0 {
		t.Fatal("PromoteAndPublish ran although IndexSearch failed")
	}
	if p := f.publishedPtr(); p != nil {
		t.Fatalf("course became published (%s) despite a failed workflow", p)
	}
	var draft *uuid.UUID
	var state string
	var perr *string
	f.env.DB.Raw(`SELECT draft_version_id FROM courses WHERE id = ?`, f.courseID).Row().Scan(&draft)
	f.env.DB.Raw(`SELECT state, publish_error FROM course_versions WHERE id = ?`, f.draftID).Row().Scan(&state, &perr)
	if draft == nil || *draft != f.draftID {
		t.Fatalf("draft pointer lost after a failed publish: %v", draft)
	}
	if state != "publish_failed" || perr == nil || *perr == "" {
		t.Fatalf("state=%q publish_error=%v", state, perr)
	}
	if _, derr := f.svc.GetPublishedDetail(context.Background(), f.slug); derr == nil {
		t.Fatal("unpublished course readable")
	} else if ae, ok := apperrors.As(derr); !ok || ae.Code != "not_found" {
		t.Fatalf("unexpected read error %v", derr)
	}
	if n := f.countPublishedEvts(); n != 0 {
		t.Fatalf("%d course_published events for a failed publish", n)
	}

	// Fix the fault and retry the same version: publish_failed must be retryable.
	f.chaos.indexAlwaysFails = false
	if err := f.run(t); err != nil {
		t.Fatalf("retry after fix: %v", err)
	}
	f.assertPublishedCoherently(t)
	if n := f.countPublishedEvts(); n != 1 {
		t.Fatalf("%d course_published events after retry", n)
	}
}

func TestAdvWorkflow_MarkPublishFailedItselfFails_WorkflowStillTerminatesWithOriginalError(t *testing.T) {
	f := newFixture(t, false) // no modules -> ValidateMetadata fails
	f.chaos.markAlwaysFails = true
	err := f.run(t)
	if err == nil {
		t.Fatal("expected failure")
	}
	if f.chaos.markCalls.Load() != 5 {
		t.Fatalf("MarkPublishFailed attempts = %d, want 5 (retry5x)", f.chaos.markCalls.Load())
	}
	if p := f.publishedPtr(); p != nil {
		t.Fatal("published despite failure")
	}
}

// Two workflows for the same version (an operator re-run, a Temporal reset)
// execute concurrently against the same database. Whatever the interleaving,
// the end state must be one coherent published version.
func TestAdvWorkflow_TwoConcurrentRunsForSameVersion_EndStateCoherent(t *testing.T) {
	f := newFixture(t, true)
	other := &chaosActivities{Activities: workflows.NewActivities(f.svc), svc: f.svc}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, acts := range []*chaosActivities{f.chaos, other} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var suite testsuite.WorkflowTestSuite
			te := suite.NewTestWorkflowEnvironment()
			te.RegisterActivity(acts)
			te.ExecuteWorkflow(workflows.PublishCourseWorkflow, course.PublishWorkflowInput{CourseID: f.courseID, VersionID: f.draftID, Title: f.title})
			errs[i] = te.GetWorkflowError()
		}()
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Fatalf("run %d failed: %v", i, e)
		}
	}
	f.assertPublishedCoherently(t)
	if n := f.countPublishedEvts(); n < 1 || n > 2 {
		t.Fatalf("%d course_published events", n)
	}
}
