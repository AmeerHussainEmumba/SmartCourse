//go:build integration

package schemaisolation_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/progress"
	"gorm.io/gorm"
)

// The whole course service (course + enrollment + progress + its relay
// handlers) runs as the svc_course role, exactly what the course binaries
// connect with (search_path course,public). Every other schema is unreachable. If any code path secretly reads
// identity.users, analytics.* ... it fails here with "relation does not exist".
func TestSchemaIsolation_CourseServiceMainFlows(t *testing.T) {
	env := testenv.Setup(t)
	conn := env.ServiceDB(t, "course")
	env.DB = conn
	ctx := context.Background()

	// Negative controls: the restriction is real.
	for _, q := range []string{`SELECT 1 FROM users`, `SELECT 1 FROM user_role_mirror`, `SELECT 1 FROM enrollment_mirror`} {
		if err := conn.Exec(q).Error; err == nil {
			t.Fatalf("%q succeeded on a course-only search_path; isolation harness is broken", q)
		}
	}

	csvc := course.NewService(course.NewRepository(conn), env.RDB, nil, "")
	esvc := enrollment.NewService(enrollment.NewRepository(conn))
	psvc := progress.NewService(progress.NewRepository(conn))

	instructor, student := uuid.New(), uuid.New()

	// Mirror feed through the course relay handlers on the restricted connection.
	for _, h := range course.Handlers() {
		if !h.EventTypes["identity.user_registered"] {
			continue
		}
		raw, _ := json.Marshal(map[string]any{"user_id": instructor, "full_name": "Iso Instructor", "role": "instructor"})
		if err := conn.Transaction(func(tx *gorm.DB) error {
			return h.Handle(ctx, tx, relay.DecodedEvent{OutboxID: 1, EventID: uuid.New(), EventType: "identity.user_registered", Payload: raw})
		}); err != nil {
			t.Fatalf("course handler on restricted path: %v", err)
		}
	}

	// authoring
	c, v, err := csvc.CreateCourse(ctx, instructor, course.CreateCourseRequest{Title: "Isolated Course", Slug: "isolated-course", Description: "d"})
	if err != nil {
		t.Fatalf("CreateCourse: %v", err)
	}
	courseID, draftID := uuid.MustParse(c.ID), uuid.MustParse(v.ID)
	mod, err := csvc.AddModule(ctx, courseID, instructor, course.CreateModuleRequest{Title: "M", Position: 1})
	if err != nil {
		t.Fatalf("AddModule: %v", err)
	}
	lesson, err := csvc.AddLesson(ctx, courseID, instructor, uuid.MustParse(mod.ID), course.CreateLessonRequest{
		Title: "L", Position: 1, ContentType: "text", Content: map[string]any{"b": "x"}, DurationS: 5,
	})
	if err != nil {
		t.Fatalf("AddLesson: %v", err)
	}
	if _, err := csvc.UpdateDraft(ctx, courseID, instructor, course.UpdateDraftRequest{Title: "Isolated Course", Description: "d2"}); err != nil {
		t.Fatalf("UpdateDraft: %v", err)
	}

	// publish activities
	steps := []func() error{
		func() error { return csvc.ValidateMetadata(ctx, draftID) },
		func() error { return csvc.VerifyAssets(ctx, draftID) },
		func() error { return csvc.ComputeMetadata(ctx, draftID) },
	}
	for i, s := range steps {
		if err := s(); err != nil {
			t.Fatalf("publish step %d: %v", i, err)
		}
	}
	outline, err := csvc.BuildOutline(ctx, courseID, draftID)
	if err != nil {
		t.Fatalf("BuildOutline: %v", err)
	}
	if outline.InstructorName != "Iso Instructor" {
		t.Fatalf("instructor name must come from instructor_mirror, got %q", outline.InstructorName)
	}
	if err := csvc.IndexSearch(ctx, outline); err != nil {
		t.Fatalf("IndexSearch: %v", err)
	}
	if err := csvc.WarmCache(ctx, outline); err != nil {
		t.Fatalf("WarmCache: %v", err)
	}
	if err := csvc.PromoteAndPublish(ctx, courseID, draftID, nil, "Isolated Course"); err != nil {
		t.Fatalf("PromoteAndPublish: %v", err)
	}
	if _, err := csvc.GetPublishStatus(ctx, courseID, instructor); err != nil {
		t.Fatalf("GetPublishStatus: %v", err)
	}

	// reads
	if _, err := csvc.GetPublishedDetail(ctx, "isolated-course"); err != nil {
		t.Fatalf("GetPublishedDetail: %v", err)
	}
	env.RDB.FlushAll(ctx) // force the Postgres path
	if _, err := csvc.GetPublishedDetail(ctx, "isolated-course"); err != nil {
		t.Fatalf("GetPublishedDetail (cold): %v", err)
	}
	cat, err := csvc.ListCatalogue(ctx, 10, "", "", "", "Isolated")
	if err != nil || len(cat.Items) != 1 || cat.Items[0].InstructorName != "Iso Instructor" {
		t.Fatalf("ListCatalogue: %v %+v", err, cat)
	}
	if res, err := csvc.SearchCourses(ctx, "Isolated", 5); err != nil || len(res) != 1 {
		t.Fatalf("SearchCourses: %v %+v", err, res)
	}
	if _, err := csvc.ResolveModuleCourseID(ctx, uuid.MustParse(mod.ID)); err != nil {
		t.Fatalf("ResolveModuleCourseID: %v", err)
	}
	if _, err := csvc.ResolveLessonCourseID(ctx, uuid.MustParse(lesson.ID)); err != nil {
		t.Fatalf("ResolveLessonCourseID: %v", err)
	}

	// enrollment + progress
	e, err := esvc.Enroll(ctx, student, courseID)
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if _, err := esvc.GetByID(ctx, student, e.ID); err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if _, err := esvc.ListMine(ctx, student); err != nil {
		t.Fatalf("ListMine: %v", err)
	}
	if err := psvc.MarkComplete(ctx, student, e.ID, uuid.MustParse(lesson.LessonKey)); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if items, err := psvc.GetProgress(ctx, student, e.ID); err != nil || len(items) != 1 || items[0].CompletedAt == nil {
		t.Fatalf("GetProgress: %v %+v", err, items)
	}
	s2 := uuid.New()
	e2, err := esvc.Enroll(ctx, s2, courseID)
	if err != nil {
		t.Fatalf("Enroll #2: %v", err)
	}
	if err := esvc.Withdraw(ctx, s2, e2.ID); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	// archive + the enrollment archive handler on the restricted connection
	if err := csvc.ArchiveCourse(ctx, courseID, instructor); err != nil {
		t.Fatalf("ArchiveCourse: %v", err)
	}
	var archivedEvt []byte
	if err := conn.Raw(`SELECT payload FROM outbox WHERE event_type = 'course.course_archived'`).Row().Scan(&archivedEvt); err != nil {
		t.Fatalf("course.outbox row not visible to svc_course: %v", err)
	}
	for _, h := range enrollment.Handlers() {
		if err := conn.Transaction(func(tx *gorm.DB) error {
			return h.Handle(ctx, tx, relay.DecodedEvent{OutboxID: 99, EventID: uuid.New(), EventType: "course.course_archived", Payload: archivedEvt})
		}); err != nil {
			t.Fatalf("enrollment archive handler: %v", err)
		}
	}
}
