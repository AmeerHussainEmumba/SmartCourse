//go:build integration

// What this file checks, against real Postgres/Redis (RFC §18.2): the
// lower-priority Service methods service_integration_test.go's main
// lifecycle test doesn't exercise — DeleteModule, DeleteLesson,
// ResolveModuleCourseID/ResolveLessonCourseID, GetPublishStatus's
// non-owner and not-found branches, and GetPublishedDetail's
// cache-miss-falls-back-to-Postgres-and-refills path (every other test
// reaches GetPublishedDetail only after WarmCache already populated
// Redis, so that fallback branch was previously dead code as far as any
// test was concerned). A separate new file in this same package
// (service_integration_test.go) already defines seedInstructor,
// newTestService, and runPublishPipeline — reused here, not redefined.
package course_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func TestDeleteModule_RemovesModule_RejectsNonOwner(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instructorID := seedInstructor(t, env)

	courseResp, _, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: "Delete Module", Slug: "delete-module"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)

	mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "M1", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	moduleID := uuid.MustParse(mod.ID)

	other := seedInstructor(t, env)
	if err := svc.DeleteModule(ctx, courseID, other, moduleID); err == nil {
		t.Fatal("expected a non-owning instructor to be rejected")
	} else if appErr, ok := apperrors.As(err); !ok || appErr.Code != "not_owner" {
		t.Fatalf("expected not_owner error, got: %v", err)
	}

	if err := svc.DeleteModule(ctx, courseID, instructorID, moduleID); err != nil {
		t.Fatalf("DeleteModule: %v", err)
	}
	if _, err := svc.ResolveModuleCourseID(ctx, moduleID); err == nil {
		t.Fatal("expected the deleted module to no longer resolve")
	}
}

func TestDeleteLesson_RemovesLesson_RecomputesTotals(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instructorID := seedInstructor(t, env)

	courseResp, _, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: "Delete Lesson", Slug: "delete-lesson"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)

	mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "M1", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	moduleID := uuid.MustParse(mod.ID)

	lesson, err := svc.AddLesson(ctx, courseID, instructorID, moduleID, course.CreateLessonRequest{
		Title: "L1", Position: 1, ContentType: "text", Content: map[string]any{"body": "x"}, DurationS: 120,
	})
	if err != nil {
		t.Fatalf("AddLesson: %v", err)
	}
	lessonID := uuid.MustParse(lesson.ID)

	// ResolveLessonCourseID must resolve correctly before deletion — this
	// is the method's only coverage anywhere, so prove it actually
	// round-trips lesson -> module -> version -> course, not just that it
	// compiles.
	resolvedCourseID, err := svc.ResolveLessonCourseID(ctx, lessonID)
	if err != nil {
		t.Fatalf("ResolveLessonCourseID: %v", err)
	}
	if resolvedCourseID != courseID {
		t.Fatalf("expected ResolveLessonCourseID to resolve to %s, got %s", courseID, resolvedCourseID)
	}

	if err := svc.DeleteLesson(ctx, courseID, instructorID, lessonID); err != nil {
		t.Fatalf("DeleteLesson: %v", err)
	}

	// RecomputeVersionTotals must have run: total_duration_s should drop
	// back to 0 now that the only lesson is gone.
	var totalDuration int
	env.DB.Raw(`SELECT total_duration_s FROM course_versions WHERE id = ?`, uuid.MustParse(*courseResp.DraftVersionID)).
		Row().Scan(&totalDuration)
	if totalDuration != 0 {
		t.Fatalf("expected total_duration_s to be recomputed to 0 after deleting the only lesson, got %d", totalDuration)
	}

	if _, err := svc.ResolveLessonCourseID(ctx, lessonID); err == nil {
		t.Fatal("expected the deleted lesson to no longer resolve")
	}
}

func TestGetPublishStatus_RejectsNonOwner_And_NotFound(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instructorID := seedInstructor(t, env)

	courseResp, _, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: "Status Check", Slug: "status-check"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)

	other := seedInstructor(t, env)
	if _, err := svc.GetPublishStatus(ctx, courseID, other); err == nil {
		t.Fatal("expected a non-owning instructor to be rejected")
	} else if appErr, ok := apperrors.As(err); !ok || appErr.Code != "not_owner" {
		t.Fatalf("expected not_owner error, got: %v", err)
	}

	if _, err := svc.GetPublishStatus(ctx, uuid.Must(uuid.NewV7()), instructorID); err == nil {
		t.Fatal("expected a nonexistent course to be rejected")
	} else if appErr, ok := apperrors.As(err); !ok || appErr.Code != "not_found" {
		t.Fatalf("expected not_found error for a nonexistent course, got: %v", err)
	}
}

// TestGetPublishedDetail_CacheMissFallsBackToPostgresAndRefillsCache is the
// regression test for a previously-dead code path: every other test in
// this package reaches GetPublishedDetail only after runPublishPipeline's
// WarmCache step already populated Redis, so the cache-miss branch (read
// Postgres, then fill Redis) never actually ran under any test. Deleting
// the warmed key before the first read forces that branch; the second
// read's cache hit proves the fill-on-miss actually wrote back.
func TestGetPublishedDetail_CacheMissFallsBackToPostgresAndRefillsCache(t *testing.T) {
	env := testenv.Setup(t)
	svc := newTestService(env)
	ctx := context.Background()
	instructorID := seedInstructor(t, env)

	courseResp, _, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: "Cache Miss", Slug: "cache-miss"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID := uuid.MustParse(courseResp.ID)

	mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "M1", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	if _, err := svc.AddLesson(ctx, courseID, instructorID, uuid.MustParse(mod.ID), course.CreateLessonRequest{
		Title: "L1", Position: 1, ContentType: "text", Content: map[string]any{"body": "x"}, DurationS: 60,
	}); err != nil {
		t.Fatalf("AddLesson: %v", err)
	}
	runPublishPipeline(t, svc, courseID, instructorID)

	// WarmCache already wrote this key as part of the publish pipeline —
	// delete it to force GetPublishedDetail's cache-miss branch.
	cacheKey := fmt.Sprintf("course:detail:%s:v1", courseID)
	if err := env.RDB.Del(ctx, cacheKey).Err(); err != nil {
		t.Fatalf("delete warmed cache key: %v", err)
	}
	if exists, err := env.RDB.Exists(ctx, cacheKey).Result(); err != nil || exists != 0 {
		t.Fatalf("expected cache key to be gone before the miss case, exists=%d err=%v", exists, err)
	}

	detail, err := svc.GetPublishedDetail(ctx, "cache-miss")
	if err != nil {
		t.Fatalf("GetPublishedDetail (cache miss): %v", err)
	}
	if len(detail.Modules) != 1 || len(detail.Modules[0].Lessons) != 1 {
		t.Fatalf("expected 1 module with 1 lesson from the Postgres fallback, got %+v", detail.Modules)
	}

	// The miss must have refilled the cache — confirm the key exists again.
	if exists, err := env.RDB.Exists(ctx, cacheKey).Result(); err != nil || exists != 1 {
		t.Fatalf("expected GetPublishedDetail to refill the cache on a miss, exists=%d err=%v", exists, err)
	}

	// A second call should now be served from the (refilled) cache and
	// return an identical result.
	detailAgain, err := svc.GetPublishedDetail(ctx, "cache-miss")
	if err != nil {
		t.Fatalf("GetPublishedDetail (cache hit): %v", err)
	}
	if len(detailAgain.Modules) != 1 || len(detailAgain.Modules[0].Lessons) != 1 {
		t.Fatalf("expected the cache-hit read to match the fallback read, got %+v", detailAgain.Modules)
	}
}
