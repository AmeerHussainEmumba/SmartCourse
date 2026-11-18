//go:build integration

// Tests for the behaviour added with the F-08/F-10/F-17/F-21/F-23/F-25/F-36
// fixes, beyond the original reproducers in fixes_integration_test.go.
package course_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

func errCode(err error) string {
	if e, ok := apperrors.As(err); ok {
		return e.Code
	}
	return ""
}

// During a republish the old version stays searchable (under its own
// metadata) until PromoteAndPublish; promotion then swaps the index entry in
// the same transaction.
func TestSearch_Republish_OldVersionServedUntilPromote_ThenNew(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, v1 := newCourse(t, svc, instr, "otter", "Otter Basics")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)

	v2 := startVersion2(t, env, courseID, "Lynx Advanced")
	authorDraft(t, svc, courseID, instr, 1, 1)
	outline, err := svc.BuildOutline(ctx, courseID, v2)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.IndexSearch(ctx, outline); err != nil {
		t.Fatal(err)
	}
	if res, _ := svc.SearchCourses(ctx, "Otter", 10); len(res) != 1 {
		t.Fatalf("old version must stay searchable before promotion, got %d results", len(res))
	}
	if res, _ := svc.SearchCourses(ctx, "Lynx", 10); len(res) != 0 {
		t.Fatalf("new version must not be searchable before promotion, got %d results", len(res))
	}

	if err := svc.PromoteAndPublish(ctx, courseID, v2, &v1, "Lynx Advanced"); err != nil {
		t.Fatal(err)
	}
	if res, _ := svc.SearchCourses(ctx, "Lynx", 10); len(res) != 1 {
		t.Fatalf("promoted version must be searchable, got %d results", len(res))
	}
	if res, _ := svc.SearchCourses(ctx, "Otter", 10); len(res) != 0 {
		t.Fatalf("superseded version's title must be gone, got %d results", len(res))
	}
}

func TestPromoteAndPublish_ArchivedCourse_Refused_NoEvent(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, draft := newCourse(t, svc, instr, "arch-promote", "Arch Promote")
	authorDraft(t, svc, courseID, instr, 1, 1)
	if err := svc.ArchiveCourse(ctx, courseID, instr); err != nil {
		t.Fatal(err)
	}
	if err := svc.PromoteAndPublish(ctx, courseID, draft, nil, "Arch Promote"); errCode(err) != "course_archived" {
		t.Fatalf("want course_archived, got %v", err)
	}
	if n := count(t, env, `SELECT count(*) FROM outbox WHERE event_type = 'course.course_published' AND aggregate_id = ?`, courseID); n != 0 {
		t.Fatalf("%d course_published events for an archived course", n)
	}
}

func TestValidateMetadata_RequiresALesson(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	instr := seedInstructor(t, env)
	courseID, draft := newCourse(t, svc, instr, "has-lesson", "Has Lesson")
	authorDraft(t, svc, courseID, instr, 1, 1)
	if err := svc.ValidateMetadata(context.Background(), draft); err != nil {
		t.Fatalf("a draft with a lesson must validate: %v", err)
	}
}

func TestDeleteModuleAndLesson_RejectNonDraftVersion(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, _ := newCourse(t, svc, instr, "immutable", "Immutable")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)

	var moduleID, lessonID uuid.UUID
	env.DB.Raw(`SELECT m.id, l.id FROM modules m JOIN lessons l ON l.module_id = m.id JOIN course_versions v ON v.id = m.course_version_id WHERE v.course_id = ?`, courseID).Row().Scan(&moduleID, &lessonID)

	if err := svc.DeleteLesson(ctx, courseID, instr, lessonID); errCode(err) != "no_draft" {
		t.Fatalf("DeleteLesson on a published lesson: want no_draft, got %v", err)
	}
	if err := svc.DeleteModule(ctx, courseID, instr, moduleID); errCode(err) != "no_draft" {
		t.Fatalf("DeleteModule on a published module: want no_draft, got %v", err)
	}
	if n := count(t, env, `SELECT count(*) FROM lessons WHERE id = ?`, lessonID); n != 1 {
		t.Fatal("published lesson was deleted")
	}
}

func TestInputValidation_NULAndDeepContent_Are400(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, _ := newCourse(t, svc, instr, "inputs", "Inputs")
	mod, err := svc.AddModule(ctx, courseID, instr, course.CreateModuleRequest{Title: "M", Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	moduleID := uuid.MustParse(mod.ID)

	if _, err := svc.AddModule(ctx, courseID, instr, course.CreateModuleRequest{Title: "a\x00b", Position: 2}); errCode(err) != "validation_failed" {
		t.Errorf("NUL in module title: %v", err)
	}
	deep := map[string]any{}
	cur := deep
	for i := 0; i < 100; i++ {
		n := map[string]any{}
		cur["k"] = n
		cur = n
	}
	for name, content := range map[string]map[string]any{
		"nul value": {"a": "x\x00y"},
		"nul key":   {"\x00": "v"},
		"too deep":  deep,
	} {
		_, err := svc.AddLesson(ctx, courseID, instr, moduleID, course.CreateLessonRequest{Title: "L", Position: 1, ContentType: "text", Content: content})
		if errCode(err) != "validation_failed" {
			t.Errorf("%s: want validation_failed, got %v", name, err)
		}
	}
	if _, err := svc.SearchCourses(ctx, "a\x00b", 5); errCode(err) != "validation_failed" {
		t.Errorf("NUL in search: %v", err)
	}
	if _, err := svc.GetPublishedDetail(ctx, "a\x00b"); errCode(err) != "not_found" {
		t.Errorf("NUL slug: %v", err)
	}
}

func TestListCatalogue_SearchEscapesLikeMetacharacters(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	for _, c := range []struct{ slug, title string }{{"pct", "100% Pure"}, {"plain", "Plain Course"}} {
		id, _ := newCourse(t, svc, instr, c.slug, c.title)
		authorDraft(t, svc, id, instr, 1, 1)
		runPublishPipeline(t, svc, id, instr)
	}
	for _, q := range []string{"%", "_", `\`, `abc\`} {
		resp, err := svc.ListCatalogue(ctx, 10, "", "", "", q)
		if err != nil {
			t.Fatalf("q=%q: %v", q, err)
		}
		if q == "%" {
			if len(resp.Items) != 1 || !strings.Contains(resp.Items[0].Title, "%") {
				t.Errorf("q=%%: want only the course whose title contains %%, got %+v", resp.Items)
			}
		} else if len(resp.Items) != 0 {
			t.Errorf("q=%q matched %d courses; metacharacters must be literal", q, len(resp.Items))
		}
	}
}

func TestDetail_ServesLiveEnrollmentCount_AfterCacheWarm(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instr := seedInstructor(t, env)
	courseID, _ := newCourse(t, svc, instr, "live-count", "Live Count")
	authorDraft(t, svc, courseID, instr, 1, 1)
	runPublishPipeline(t, svc, courseID, instr)
	for _, n := range []int{3, 4} {
		env.DB.Exec(`UPDATE courses SET enrollment_count = ? WHERE id = ?`, n, courseID)
		d, err := svc.GetPublishedDetail(ctx, "live-count")
		if err != nil || d.Course.EnrollmentCount != n {
			t.Fatalf("want live count %d, got %+v %v", n, d, err)
		}
	}
}
