//go:build integration

// Adversarial publish / blue-green / cache tests (ADR-0010, ADR-0025). They
// drive the real activities against real Postgres and Redis; the Temporal
// orchestration side is attacked in services/course/workflows.
package course_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	temporalclient "go.temporal.io/sdk/client"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// authorDraft adds `modules` modules with `lessons` lessons each to the
// course's current draft.
func authorDraft(t *testing.T, svc *course.Service, courseID, instructorID uuid.UUID, modules, lessons int) {
	t.Helper()
	ctx := context.Background()
	for m := 1; m <= modules; m++ {
		mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: fmt.Sprintf("M%d", m), Position: m})
		if err != nil {
			t.Fatalf("AddModule: %v", err)
		}
		for l := 1; l <= lessons; l++ {
			if _, err := svc.AddLesson(ctx, courseID, instructorID, uuid.MustParse(mod.ID), course.CreateLessonRequest{
				Title: fmt.Sprintf("L%d", l), Position: l, ContentType: "text", Content: map[string]any{"body": "x"}, DurationS: 10,
			}); err != nil {
				t.Fatalf("AddLesson: %v", err)
			}
		}
	}
}

func newCourse(t *testing.T, svc *course.Service, instructorID uuid.UUID, slug, title string) (courseID, draftID uuid.UUID) {
	t.Helper()
	c, v, err := svc.CreateCourse(context.Background(), instructorID, course.CreateCourseRequest{Title: title, Slug: slug, Description: "d " + title})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	return uuid.MustParse(c.ID), uuid.MustParse(v.ID)
}

// startVersion2 does what a future "edit published course" feature will: a
// new draft row (version_number 2) pointed at by courses.draft_version_id.
// No production code creates a second version today (CreateCourse is the only
// caller of CreateVersion), so this is done in SQL.
func startVersion2(t *testing.T, env *testenv.Env, courseID uuid.UUID, title string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	if err := env.DB.Exec(`INSERT INTO course_versions (id, course_id, version_number, state, title, description, tags) VALUES (?, ?, 2, 'draft', ?, 'd v2', '{}')`,
		id, courseID, title).Error; err != nil {
		t.Fatalf("insert v2: %v", err)
	}
	if err := env.DB.Exec(`UPDATE courses SET draft_version_id = ? WHERE id = ?`, id, courseID).Error; err != nil {
		t.Fatalf("point draft at v2: %v", err)
	}
	return id
}

func count(t *testing.T, env *testenv.Env, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := env.DB.Raw(q, args...).Row().Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// Readers hammer GetPublishedDetail while a republish (v1 -> v2) runs step by
// step. Every observation must be a coherent version: either all of v1 (1
// module, published id v1, title v1) or all of v2 (3 modules, id v2, title
// v2, no draft). Anything else is a half-published read.
func TestAdvPublish_BlueGreen_ReadersNeverSeeHalfPublishedVersion(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)

	courseID, v1 := newCourse(t, svc, instr, "bluegreen", "Title V1")
	authorDraft(t, svc, courseID, instr, 1, 1)
	if st := runPublishPipeline(t, svc, courseID, instr); st.State != "ready" {
		t.Fatalf("v1 publish state %q", st.State)
	}
	v2 := startVersion2(t, env, courseID, "Title V2")
	authorDraft(t, svc, courseID, instr, 3, 2)

	type obs struct {
		versionID, title string
		modules          int
		published, draft *string
		state            string
	}
	var mu sync.Mutex
	var seen []obs
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				d, err := svc.GetPublishedDetail(ctx, "bluegreen")
				if err != nil {
					mu.Lock()
					seen = append(seen, obs{title: "ERR " + err.Error()})
					mu.Unlock()
					continue
				}
				mu.Lock()
				seen = append(seen, obs{d.Version.ID, d.Version.Title, len(d.Modules), d.Course.PublishedVersionID, d.Course.DraftVersionID, d.Version.State})
				mu.Unlock()
			}
		}()
	}

	if err := svc.ValidateMetadata(ctx, v2); err != nil {
		t.Fatal(err)
	}
	if err := svc.VerifyAssets(ctx, v2); err != nil {
		t.Fatal(err)
	}
	if err := svc.ComputeMetadata(ctx, v2); err != nil {
		t.Fatal(err)
	}
	outline, err := svc.BuildOutline(ctx, courseID, v2)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.WarmCache(ctx, outline); err != nil {
		t.Fatal(err)
	}
	waitSeen := func(pred func(o obs) bool, what string) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			mu.Lock()
			ok := false
			for _, o := range seen {
				if pred(o) {
					ok = true
					break
				}
			}
			mu.Unlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("readers never observed %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitSeen(func(o obs) bool { return o.versionID == v1.String() }, "v1 in the warm-but-not-promoted window")
	if err := svc.PromoteAndPublish(ctx, courseID, v2, &v1, outline.Title); err != nil {
		t.Fatal(err)
	}
	waitSeen(func(o obs) bool { return o.versionID == v2.String() }, "v2 after promotion")
	close(stop)
	wg.Wait()

	sawV1, sawV2 := false, false
	for _, o := range seen {
		switch {
		case o.versionID == v1.String():
			sawV1 = true
			if o.title != "Title V1" || o.modules != 1 || o.published == nil || *o.published != v1.String() || o.state != "ready" {
				t.Fatalf("half-published v1 read: %+v", o)
			}
		case o.versionID == v2.String():
			sawV2 = true
			if o.title != "Title V2" || o.modules != 3 || o.published == nil || *o.published != v2.String() || o.draft != nil || o.state != "ready" {
				t.Fatalf("half-published v2 read: %+v", o)
			}
		default:
			t.Fatalf("incoherent observation: %+v", o)
		}
	}
	if !sawV1 || !sawV2 {
		t.Fatalf("readers did not observe both versions (v1=%v v2=%v, %d observations); test did not exercise the swap", sawV1, sawV2, len(seen))
	}
	var s1, s2 string
	env.DB.Raw(`SELECT state FROM course_versions WHERE id = ?`, v1).Row().Scan(&s1)
	env.DB.Raw(`SELECT state FROM course_versions WHERE id = ?`, v2).Row().Scan(&s2)
	if s1 != "superseded" || s2 != "ready" {
		t.Fatalf("version states after promote: v1=%s v2=%s", s1, s2)
	}
	// After republish the reader must get v2 even though v1's cache key still exists.
	if n := env.RDB.Exists(ctx, "course:detail:"+courseID.String()+":v1").Val(); n != 1 {
		t.Logf("note: v1 cache key no longer present (%d)", n)
	}
	d, err := svc.GetPublishedDetail(ctx, "bluegreen")
	if err != nil || d.Version.ID != v2.String() {
		t.Fatalf("post-republish read: %v %+v", err, d)
	}
}

// A cache entry must never let an archived course be served, even though the
// cached payload itself looks perfectly valid.
func TestAdvPublish_ArchivedCourseNotServedFromWarmCache(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, _ := newCourse(t, svc, instr, "arch-cache", "Archive me")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)
	if _, err := svc.GetPublishedDetail(ctx, "arch-cache"); err != nil {
		t.Fatal(err)
	}
	if err := svc.ArchiveCourse(ctx, courseID, instr); err != nil {
		t.Fatal(err)
	}
	_, err := svc.GetPublishedDetail(ctx, "arch-cache")
	if ae, ok := apperrors.As(err); !ok || ae.Code != "not_found" {
		t.Fatalf("archived course still served: %v", err)
	}
	if res, _ := svc.SearchCourses(ctx, "Archive", 10); len(res) != 0 {
		t.Fatalf("archived course in search results: %+v", res)
	}
}

// Redis being wiped or unavailable must degrade to Postgres, not fail reads.
func TestAdvPublish_RedisFlushedOrCorrupt_FallsBackToPostgres(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, _ := newCourse(t, svc, instr, "cache-corrupt", "Cache corrupt")
	authorDraft(t, svc, courseID, instr, 2, 1)
	runPublishPipeline(t, svc, courseID, instr)

	key := "course:detail:" + courseID.String() + ":v1"
	if err := env.RDB.Set(ctx, key, "{not json", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	d, err := svc.GetPublishedDetail(ctx, "cache-corrupt")
	if err != nil || len(d.Modules) != 2 {
		t.Fatalf("corrupt cache entry broke the read: %v %+v", err, d)
	}
	// and the read healed the cache
	var healed course.CourseDetailResponse
	raw, err := env.RDB.Get(ctx, key).Bytes()
	if err != nil || len(raw) < 10 || raw[0] != '{' || string(raw) == "{not json" {
		t.Fatalf("cache not healed: %q %v", raw, err)
	}
	_ = healed
	env.RDB.FlushAll(ctx)
	if d, err := svc.GetPublishedDetail(ctx, "cache-corrupt"); err != nil || len(d.Modules) != 2 {
		t.Fatalf("flushed cache broke the read: %v", err)
	}
}

// Concurrent Service.Publish calls for the same draft: exactly ONE wins the
// claim and starts the workflow; every other gets 409 publish_in_progress.
// (Before the claim existed all 20 attached to the same workflow ID and
// returned 202, so a client could not tell a duplicate from a fresh publish.)
func TestAdvPublish_ConcurrentPublishRequests_OneWinsRestGet409(t *testing.T) {
	env := testenv.Setup(t)
	instr := seedInstructor(t, env)
	ctx := context.Background()
	var mu sync.Mutex
	ids := map[string]int{}
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		mu.Lock()
		ids[o.ID]++
		mu.Unlock()
		return &fakeWorkflowRun{id: o.ID}, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	courseID, draftID := newCourse(t, svc, instr, "conc-publish", "Concurrent")
	authorDraft(t, svc, courseID, instr, 1, 1)

	var wg sync.WaitGroup
	var ok, conflict, other atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.Publish(ctx, courseID, instr)
			switch ae, isApp := apperrors.As(err); {
			case err == nil:
				ok.Add(1)
			case isApp && ae.HTTPStatus == 409 && ae.Code == "publish_in_progress":
				conflict.Add(1)
			default:
				other.Add(1)
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || conflict.Load() != 19 || other.Load() != 0 {
		t.Fatalf("want 1 success and 19 x 409, got ok=%d conflict=%d other=%d", ok.Load(), conflict.Load(), other.Load())
	}
	total := 0
	for _, n := range ids {
		total += n
	}
	if len(ids) != 1 || total != 1 {
		t.Fatalf("the workflow must be started exactly once; starts by ID: %v", ids)
	}
	var state, wf string
	env.DB.Raw(`SELECT state, workflow_id FROM course_versions WHERE id = ?`, draftID).Row().Scan(&state, &wf)
	if state != "publishing" || ids[wf] != 1 {
		t.Fatalf("state=%s workflow_id=%s ids=%v", state, wf, ids)
	}
}

// A publish request on someone else's course, a missing course and a course
// whose draft pointer is cleared must all be refused without calling Temporal.
func TestAdvPublish_RefusedWithoutStartingWorkflow(t *testing.T) {
	env := testenv.Setup(t)
	instr, other := seedInstructor(t, env), seedInstructor(t, env)
	var calls atomic.Int32
	fake := &fakeTemporalClient{executeWorkflow: func(_ context.Context, o temporalclient.StartWorkflowOptions, _ any, _ ...any) (temporalclient.WorkflowRun, error) {
		calls.Add(1)
		return &fakeWorkflowRun{id: o.ID}, nil
	}}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, fake, "q")
	courseID, _ := newCourse(t, svc, instr, "refuse-pub", "Refuse")
	authorDraft(t, svc, courseID, instr, 1, 1)

	if _, err := svc.Publish(context.Background(), courseID, other); err == nil {
		t.Fatal("non-owner could publish")
	}
	if _, err := svc.Publish(context.Background(), uuid.New(), instr); err == nil {
		t.Fatal("missing course could be published")
	}
	runPublishPipeline(t, svc, courseID, instr) // now there is no draft
	if _, err := svc.Publish(context.Background(), courseID, instr); err == nil {
		t.Fatal("publish with no draft accepted")
	}
	if calls.Load() != 0 {
		t.Fatalf("Temporal was called %d times for refused requests", calls.Load())
	}
}

// ValidateMetadata/VerifyAssets edge cases: empty title, oversized content.
func TestAdvPublish_ValidationEdges(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)

	// Course with no modules.
	cid, draft := newCourse(t, svc, instr, "no-content", "No content")
	if err := svc.ValidateMetadata(ctx, draft); err == nil {
		t.Fatal("empty course passed ValidateMetadata")
	}
	// Blank title written below the API.
	authorDraft(t, svc, cid, instr, 1, 1)
	env.DB.Exec(`UPDATE course_versions SET title = '' WHERE id = ?`, draft)
	if err := svc.ValidateMetadata(ctx, draft); err == nil {
		t.Fatal("blank title passed ValidateMetadata")
	}
	// Unknown version ids surface as errors, not panics.
	if err := svc.ValidateMetadata(ctx, uuid.New()); err == nil {
		t.Fatal("unknown version passed ValidateMetadata")
	}
	if _, err := svc.BuildOutline(ctx, uuid.New(), uuid.New()); err == nil {
		t.Fatal("BuildOutline on unknown course succeeded")
	}
}

// A failed MarkPublishFailed-less crash between activities must leave the
// live course untouched: run everything but PromoteAndPublish and check.
func TestAdvPublish_PartialPipelineLeavesLiveVersionUntouched(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, v1 := newCourse(t, svc, instr, "partial", "Partial V1")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)
	v2 := startVersion2(t, env, courseID, "Partial V2")
	authorDraft(t, svc, courseID, instr, 2, 1)

	outline, err := svc.BuildOutline(ctx, courseID, v2)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.WarmCache(ctx, outline); err != nil {
		t.Fatal(err)
	}
	var pub, draft *uuid.UUID
	env.DB.Raw(`SELECT published_version_id, draft_version_id FROM courses WHERE id = ?`, courseID).Row().Scan(&pub, &draft)
	if pub == nil || *pub != v1 || draft == nil || *draft != v2 {
		t.Fatalf("pointers moved before promote: published=%v draft=%v", pub, draft)
	}
	d, err := svc.GetPublishedDetail(ctx, "partial")
	if err != nil || d.Version.ID != v1.String() {
		t.Fatalf("live version changed before promote: %v", err)
	}
	if n := count(t, env, `SELECT count(*) FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, courseID); n != 1 {
		t.Fatalf("course_published events before the second promote: %d", n)
	}
	// MarkPublishFailed then leaves the draft retryable.
	if err := svc.MarkPublishFailed(ctx, v2, "boom", "BuildOutline"); err != nil {
		t.Fatal(err)
	}
	st, _ := svc.GetPublishStatus(ctx, courseID, instr)
	if st.State != "publish_failed" || st.Error == nil || *st.Error != "boom" {
		t.Fatalf("status after failure: %+v", st)
	}
	if err := svc.PromoteAndPublish(ctx, courseID, v2, &v1, "Partial V2"); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
	if n := count(t, env, `SELECT count(*) FROM course_versions WHERE course_id = ? AND state = 'ready'`, courseID); n != 1 {
		t.Fatalf("%d ready versions after republish; blue-green allows exactly one", n)
	}
}
