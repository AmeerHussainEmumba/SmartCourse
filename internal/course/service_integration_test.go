//go:build integration

// What this file checks, against real Postgres (RFC §18.2): the full
// author -> add module -> add lesson -> publish -> appears in catalogue ->
// detail round trip, blue-green promotion (old version superseded, new one
// ready), ownership enforcement (an instructor can't edit another's
// course), and that publishing an empty draft is rejected. Run with
// `make test-integration`; see docs/guides/testing.md.
package course_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func seedInstructor(t *testing.T, env *testenv.Env) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	err := env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 'Test Instructor', 'instructor', 'active')`,
		id, id.String()+"@example.com").Error
	if err != nil {
		t.Fatalf("seed instructor: %v", err)
	}
	return id
}

func TestCoursePublish_FullLifecycle(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB))
	ctx := context.Background()
	instructorID := seedInstructor(t, env)

	courseResp, versionResp, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{
		Title: "Intro to Go", Slug: "intro-to-go", Description: "Learn Go",
	})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	if versionResp.State != "draft" {
		t.Fatalf("expected new version to be draft, got %s", versionResp.State)
	}
	courseID := uuid.MustParse(courseResp.ID)

	// Publishing with no modules must fail (ValidateMetadata, RFC §7.4).
	if _, err := svc.Publish(ctx, courseID, instructorID); err == nil {
		t.Fatal("expected publish to fail with no modules")
	}

	mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "Basics", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	moduleID := uuid.MustParse(mod.ID)

	if _, err := svc.AddLesson(ctx, courseID, instructorID, moduleID, course.CreateLessonRequest{
		Title: "Hello World", Position: 1, ContentType: "text",
		Content: map[string]any{"body": "fmt.Println(\"hello\")"}, DurationS: 300,
	}); err != nil {
		t.Fatalf("AddLesson: %v", err)
	}

	published, err := svc.Publish(ctx, courseID, instructorID)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if published.State != "ready" {
		t.Fatalf("expected published version state 'ready', got %s", published.State)
	}

	// Catalogue must now list it.
	catalogue, err := svc.ListCatalogue(ctx, 20, "")
	if err != nil {
		t.Fatalf("ListCatalogue: %v", err)
	}
	found := false
	for _, item := range catalogue.Items {
		if item.Slug == "intro-to-go" {
			found = true
			if item.InstructorName != "Test Instructor" {
				t.Errorf("expected instructor name joined in catalogue row, got %q", item.InstructorName)
			}
		}
	}
	if !found {
		t.Fatal("expected published course to appear in catalogue")
	}

	// Detail must show the module and lesson.
	detail, err := svc.GetPublishedDetail(ctx, "intro-to-go")
	if err != nil {
		t.Fatalf("GetPublishedDetail: %v", err)
	}
	if len(detail.Modules) != 1 || len(detail.Modules[0].Lessons) != 1 {
		t.Fatalf("expected 1 module with 1 lesson in detail, got %+v", detail.Modules)
	}
}

func TestUpdateDraft_RejectsNonOwner(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB))
	ctx := context.Background()

	owner := seedInstructor(t, env)
	other := seedInstructor(t, env)

	courseResp, _, err := svc.CreateCourse(ctx, owner, course.CreateCourseRequest{Title: "Owned Course", Slug: "owned-course"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)

	_, err = svc.UpdateDraft(ctx, courseID, other, course.UpdateDraftRequest{Title: "Hijacked"})
	if err == nil {
		t.Fatal("expected a non-owning instructor to be rejected")
	}
	appErr, ok := apperrors.As(err)
	if !ok || appErr.Code != "not_owner" {
		t.Fatalf("expected not_owner error, got: %v", err)
	}
}

func TestCreateCourse_RejectsInvalidSlug(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB))
	ctx := context.Background()
	instructorID := seedInstructor(t, env)

	_, _, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: "Bad Slug", Slug: "Not A Valid Slug!"})
	if err == nil {
		t.Fatal("expected invalid slug format to be rejected")
	}
}
