//go:build integration

// What this file checks, against real Postgres (RFC §13.2, RFC §18.2):
// ListCatalogue's keyset pagination actually works past page 1 — every
// other test in this package only ever calls it with an empty cursor
// (first page). This is the regression test for that gap: publish more
// courses than fit on one page, follow NextCursor, and confirm the second
// page returns the remaining courses, with no duplicates and consistent
// (created_at DESC, id DESC) ordering across both pages.
package course_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/course"
	"github.com/AmeerHussainEmumba/SmartCourse/test/integration/testenv"
)

func TestListCatalogue_SecondPage_ViaCursor(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	ctx := context.Background()
	instructorID := seedInstructor(t, env)

	slugs := []string{"page-course-1", "page-course-2", "page-course-3"}
	for _, slug := range slugs {
		cResp, _, err := svc.CreateCourse(ctx, instructorID, course.CreateCourseRequest{Title: slug, Slug: slug})
		if err != nil {
			t.Fatalf("CreateCourse %s: %v", slug, err)
		}
		courseID := uuid.MustParse(cResp.ID)
		mod, err := svc.AddModule(ctx, courseID, instructorID, course.CreateModuleRequest{Title: "M1", Position: 1})
		if err != nil {
			t.Fatalf("AddModule %s: %v", slug, err)
		}
		if _, err := svc.AddLesson(ctx, courseID, instructorID, uuid.MustParse(mod.ID), course.CreateLessonRequest{
			Title: "L1", Position: 1, ContentType: "text", Content: map[string]any{"body": "x"}, DurationS: 30,
		}); err != nil {
			t.Fatalf("AddLesson %s: %v", slug, err)
		}
		runPublishPipeline(t, svc, courseID, instructorID)
	}

	const pageSize = 2
	page1, err := svc.ListCatalogue(ctx, pageSize, "", "", "", "")
	if err != nil {
		t.Fatalf("ListCatalogue page 1: %v", err)
	}
	if len(page1.Items) != pageSize {
		t.Fatalf("expected %d items on page 1, got %d: %+v", pageSize, len(page1.Items), page1.Items)
	}
	if page1.NextCursor == nil {
		t.Fatal("expected page 1 to report a next_cursor given 3 courses and a page size of 2")
	}

	page2, err := svc.ListCatalogue(ctx, pageSize, *page1.NextCursor, "", "", "")
	if err != nil {
		t.Fatalf("ListCatalogue page 2: %v", err)
	}
	if len(page2.Items) != 1 {
		t.Fatalf("expected exactly 1 remaining item on page 2, got %d: %+v", len(page2.Items), page2.Items)
	}
	if page2.NextCursor != nil {
		t.Fatalf("expected no next_cursor once every course has been returned, got %q", *page2.NextCursor)
	}

	seen := map[string]bool{}
	for _, item := range page1.Items {
		seen[item.Slug] = true
	}
	for _, item := range page2.Items {
		if seen[item.Slug] {
			t.Fatalf("slug %q appeared on both page 1 and page 2 — pagination duplicated a row", item.Slug)
		}
		seen[item.Slug] = true
	}
	for _, slug := range slugs {
		if !seen[slug] {
			t.Fatalf("expected %q to appear across the two pages, but it never did; page1=%+v page2=%+v", slug, page1.Items, page2.Items)
		}
	}

	// Ordering must be consistent across the page boundary: page 1's last
	// item's created_at must be >= page 2's first item's created_at
	// (created_at DESC, id DESC — courses were created in slug order, so
	// newest-first means page 1 holds page-course-3 and page-course-2,
	// page 2 holds page-course-1).
	if page2.Items[0].Slug != "page-course-1" {
		t.Fatalf("expected the oldest course (page-course-1) to land on page 2, got %+v", page2.Items[0])
	}
}
