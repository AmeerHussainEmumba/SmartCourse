//go:build integration

package securitytest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func (w *world) scalar(q string, args ...any) string {
	w.t.Helper()
	var s string
	if err := w.env.DB.Raw(q, args...).Row().Scan(&s); err != nil {
		w.t.Fatalf("query %q: %v", q, err)
	}
	return s
}

// An instructor holding a perfectly valid instructor token must not be able to
// touch another instructor's course through ANY id they can reach: the course
// id, or the module / lesson ids (which resolve to the course internally).
func TestIDOR_InstructorB_CannotModifyInstructorA_Course(t *testing.T) {
	w := newWorld(t)
	fx := w.newDraft(instrA)
	pub := w.newPublished(instrA)
	c := fx.CourseID.String()

	attacks := []struct {
		method, path string
		body         any
	}{
		{"PATCH", "/api/v1/courses/" + c, map[string]any{"title": "PWNED"}},
		{"POST", "/api/v1/courses/" + c + "/modules", map[string]any{"title": "PWNED", "position": 9}},
		{"POST", "/api/v1/courses/" + c + "/publish", nil},
		{"GET", "/api/v1/courses/" + c + "/publish-status", nil},
		{"POST", "/api/v1/courses/" + pub.CourseID.String() + "/archive", nil},
		{"PATCH", "/api/v1/modules/" + fx.ModuleID.String(), map[string]any{"title": "PWNED"}},
		{"DELETE", "/api/v1/modules/" + fx.ModuleID.String(), nil},
		{"POST", "/api/v1/modules/" + fx.ModuleID.String() + "/lessons", map[string]any{"title": "PWNED", "position": 5, "content_type": "text"}},
		{"PATCH", "/api/v1/lessons/" + fx.LessonID.String(), map[string]any{"title": "PWNED", "content_type": "text"}},
		{"DELETE", "/api/v1/lessons/" + fx.LessonID.String(), nil},
		// lesson/module of a PUBLISHED course (history must be immutable even for the owner; for B doubly so)
		{"DELETE", "/api/v1/modules/" + pub.ModuleID.String(), nil},
		{"PATCH", "/api/v1/lessons/" + pub.LessonID.String(), map[string]any{"title": "PWNED", "content_type": "text"}},
	}
	for _, a := range attacks {
		rec := w.do(instrB, a.method, a.path, a.body)
		if rec.Code != 403 && rec.Code != 404 && rec.Code != 409 {
			t.Errorf("instructorB %s %s: want deny, got %d %s", a.method, a.path, rec.Code, rec.Body.String())
		}
		assertNoInternalLeak(t, a.method+" "+a.path, rec)
	}

	// Nothing changed.
	if got := w.scalar(`SELECT title FROM course_versions WHERE course_id = ? AND state = 'draft'`, fx.CourseID); got != fx.Title {
		t.Errorf("draft title changed to %q", got)
	}
	if got := w.scalar(`SELECT count(*)::text FROM modules WHERE id = ?`, fx.ModuleID); got != "1" {
		t.Error("module was deleted by another instructor")
	}
	if got := w.scalar(`SELECT count(*)::text FROM lessons WHERE id = ?`, fx.LessonID); got != "1" {
		t.Error("lesson was deleted by another instructor")
	}
	if got := w.scalar(`SELECT title FROM modules WHERE id = ?`, fx.ModuleID); got != "m1" {
		t.Errorf("module title changed to %q", got)
	}
	if got := w.scalar(`SELECT title FROM lessons WHERE id = ?`, fx.LessonID); got != "l1" {
		t.Errorf("lesson title changed to %q", got)
	}
	if got := w.scalar(`SELECT (archived_at IS NULL)::text FROM courses WHERE id = ?`, pub.CourseID); got != "true" {
		t.Error("course was archived by a non-owner")
	}
	if got := w.scalar(`SELECT state FROM course_versions WHERE course_id = ? AND state <> 'ready' ORDER BY created_at DESC LIMIT 1`, fx.CourseID); got != "draft" {
		t.Errorf("draft state changed to %q by non-owner publish", got)
	}
}

// Even the OWNER must not be able to rewrite a published (immutable) version
// by addressing its module/lesson ids directly (PATCH/POST variants; the DELETE
// variants are the bug reproduced in security_bugrepro_test.go).
func TestPublishedVersionIsImmutable_EvenForOwner(t *testing.T) {
	w := newWorld(t)
	pub := w.newPublished(instrA)
	for _, a := range []struct {
		method, path string
		body         any
	}{
		{"PATCH", "/api/v1/modules/" + pub.ModuleID.String(), map[string]any{"title": "rewritten"}},
		{"POST", "/api/v1/modules/" + pub.ModuleID.String() + "/lessons", map[string]any{"title": "injected", "position": 7, "content_type": "text"}},
		{"PATCH", "/api/v1/lessons/" + pub.LessonID.String(), map[string]any{"title": "rewritten", "content_type": "text"}},
	} {
		rec := w.do(instrA, a.method, a.path, a.body)
		if okStatus(rec.Code) {
			t.Errorf("owner mutated a published version via %s %s: %d", a.method, a.path, rec.Code)
		}
	}
	if w.scalar(`SELECT title FROM modules WHERE id = ?`, pub.ModuleID) != "m1" || w.scalar(`SELECT count(*)::text FROM lessons WHERE id = ?`, pub.LessonID) != "1" {
		t.Fatal("published content was modified")
	}
}

func TestIDOR_StudentB_CannotReadOrChangeStudentA_EnrollmentAndProgress(t *testing.T) {
	w := newWorld(t)
	fx := w.newPublished(instrA)
	eA := w.enroll(studentA, fx.CourseID)
	eAs := eA.String()

	for _, a := range []struct{ method, path string }{
		{"GET", "/api/v1/enrollments/" + eAs},
		{"GET", "/api/v1/enrollments/" + eAs + "/progress"},
		{"PUT", "/api/v1/enrollments/" + eAs + "/lessons/" + fx.LessonKey.String() + "/complete"},
		{"DELETE", "/api/v1/enrollments/" + eAs},
	} {
		for _, who := range []int{studentB, instrA, instrB, admin} {
			rec := w.do(who, a.method, a.path, nil)
			if rec.Code != 403 && rec.Code != 404 {
				t.Errorf("%s %s %s: want 403/404, got %d %s", w.actors[who].name, a.method, a.path, rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), fx.CourseID.String()) || strings.Contains(rec.Body.String(), fx.LessonKey.String()) {
				t.Errorf("%s %s leaked enrollment data: %s", a.method, a.path, rec.Body.String())
			}
		}
	}
	if w.scalar(`SELECT status FROM enrollments WHERE id = ?`, eA) != "active" {
		t.Error("another user changed the enrollment status")
	}
	if w.scalar(`SELECT count(*)::text FROM lesson_progress WHERE enrollment_id = ? AND completed_at IS NOT NULL`, eA) != "0" {
		t.Error("another user completed lessons on someone else's enrollment")
	}
	if w.scalar(`SELECT enrollment_count::text FROM courses WHERE id = ?`, fx.CourseID) != "1" {
		t.Error("enrollment_count was changed")
	}
	// The owner still can.
	if rec := w.do(studentA, "PUT", "/api/v1/enrollments/"+eAs+"/lessons/"+fx.LessonKey.String()+"/complete", nil); rec.Code != 204 {
		t.Errorf("owner complete: %d", rec.Code)
	}
}

// A student marking an arbitrary (foreign) lesson key complete on their own
// enrollment must not complete the enrollment or create progress rows.
func TestProgress_ForeignOrRandomLessonKey_DoesNotCompleteEnrollment(t *testing.T) {
	w := newWorld(t)
	fx := w.newPublished(instrA)
	other := w.newPublished(instrB)
	e := w.enroll(studentA, fx.CourseID)
	for _, key := range []uuid.UUID{uuid.New(), other.LessonKey, uuid.Nil} {
		rec := w.do(studentA, "PUT", "/api/v1/enrollments/"+e.String()+"/lessons/"+key.String()+"/complete", nil)
		if rec.Code >= 500 {
			t.Errorf("key %s: server error %d", key, rec.Code)
		}
	}
	if st := w.scalar(`SELECT status FROM enrollments WHERE id = ?`, e); st != "active" {
		t.Fatalf("enrollment became %q after marking lessons that are not in the course", st)
	}
	if n := w.scalar(`SELECT count(*)::text FROM lesson_progress WHERE enrollment_id = ?`, e); n != "1" {
		t.Fatalf("progress rows for foreign lesson keys were created: %s rows", n)
	}
}

// Unpublished / archived content never reaches readers, including via caches,
// search and the catalogue.
func TestVisibility_DraftAndArchivedNeverReachReaders(t *testing.T) {
	w := newWorld(t)
	draft := w.newDraft(instrA)
	pub := w.newPublished(instrA)

	// Draft: by slug, by id-as-slug, and absent from every listing.
	for _, p := range []string{"/api/v1/courses/" + draft.Slug, "/api/v1/courses/" + draft.CourseID.String()} {
		for _, who := range []int{anon, studentA, instrB, admin} {
			if rec := w.do(who, "GET", p, nil); rec.Code != 404 || strings.Contains(rec.Body.String(), draft.Title) {
				t.Errorf("%s GET %s: %d %.120s", w.actors[who].name, p, rec.Code, rec.Body.String())
			}
		}
	}
	for _, p := range []string{"/api/v1/courses?limit=100", "/api/v1/courses/search?q=SECRET-TITLE", "/api/v1/courses?q=SECRET"} {
		rec := w.do(studentA, "GET", p, nil)
		if strings.Contains(rec.Body.String(), draft.Slug) || strings.Contains(rec.Body.String(), draft.CourseID.String()) {
			t.Errorf("%s exposes a draft-only course: %s", p, rec.Body.String())
		}
	}

	// Published course is readable and cached; after archive it must disappear despite the warm cache.
	if rec := w.do(anon, "GET", "/api/v1/courses/"+pub.Slug, nil); rec.Code != 200 {
		t.Fatalf("published detail: %d", rec.Code)
	}
	if rec := w.do(instrA, "POST", "/api/v1/courses/"+pub.CourseID.String()+"/archive", nil); rec.Code != 204 {
		t.Fatalf("archive: %d", rec.Code)
	}
	if rec := w.do(anon, "GET", "/api/v1/courses/"+pub.Slug, nil); rec.Code != 404 {
		t.Errorf("archived course still served (cache?): %d %s", rec.Code, rec.Body.String())
	}
	for _, p := range []string{"/api/v1/courses?limit=100", "/api/v1/courses/search?q=SECRET-TITLE"} {
		if rec := w.do(studentA, "GET", p, nil); strings.Contains(rec.Body.String(), pub.Slug) {
			t.Errorf("%s still lists the archived course: %s", p, rec.Body.String())
		}
	}
	if rec := w.do(studentA, "POST", "/api/v1/enrollments", map[string]any{"course_id": pub.CourseID.String()}); okStatus(rec.Code) {
		t.Errorf("enrolled into an archived course: %d", rec.Code)
	}
}

// Mass assignment: every server-owned field smuggled into create/update bodies is ignored.
func TestMassAssignment_CourseCreateUpdateModuleLesson(t *testing.T) {
	w := newWorld(t)
	other := w.actors[instrB].id.String()
	foreignCourse := w.newPublished(instrB)
	foreignVersion := w.scalar(`SELECT published_version_id::text FROM courses WHERE id = ?`, foreignCourse.CourseID)
	evil := map[string]any{
		"instructor_id": other, "id": uuid.NewString(), "enrollment_count": 999,
		"published_version_id": foreignVersion, "draft_version_id": foreignVersion,
		"archived_at": "2020-01-01T00:00:00Z", "state": "ready", "version_number": 99,
		"workflow_id": "x", "publish_error": "x", "created_at": "2001-01-01T00:00:00Z", "role": "admin",
	}
	body := func(extra map[string]any, base map[string]any) map[string]any {
		for k, v := range extra {
			base[k] = v
		}
		return base
	}
	slug := "mass-" + strings.ToLower(uuid.NewString()[:8])
	rec := w.do(instrA, "POST", "/api/v1/courses", body(evil, map[string]any{"title": "Mass", "slug": slug, "enrollment_limit": 5}))
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Course struct {
			ID                 string  `json:"id"`
			InstructorID       string  `json:"instructor_id"`
			EnrollmentCount    int     `json:"enrollment_count"`
			PublishedVersionID *string `json:"published_version_id"`
		} `json:"course"`
		Version struct{ State string } `json:"version"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out.Course.InstructorID != w.actors[instrA].id.String() || out.Course.EnrollmentCount != 0 || out.Course.PublishedVersionID != nil || out.Version.State != "draft" {
		t.Fatalf("mass assignment succeeded on create: %s", rec.Body.String())
	}
	var archived, pubv *string
	row := w.env.DB.Raw(`SELECT archived_at::text, published_version_id::text FROM courses WHERE id = ?`, out.Course.ID).Row()
	_ = row.Scan(&archived, &pubv)
	if archived != nil || pubv != nil {
		t.Fatalf("DB row carries attacker-supplied archived_at/published_version_id: %v %v", archived, pubv)
	}

	// Update draft / module / lesson with protected fields.
	fx := w.newDraft(instrA)
	if rec := w.do(instrA, "PATCH", "/api/v1/courses/"+fx.CourseID.String(), body(evil, map[string]any{"title": "Upd"})); rec.Code != 200 {
		t.Fatalf("update: %d %s", rec.Code, rec.Body.String())
	}
	if w.scalar(`SELECT instructor_id::text FROM courses WHERE id = ?`, fx.CourseID) != w.actors[instrA].id.String() {
		t.Fatal("instructor_id changed through PATCH /courses/:id")
	}
	if w.scalar(`SELECT enrollment_count::text FROM courses WHERE id = ?`, fx.CourseID) != "0" || w.scalar(`SELECT (published_version_id IS NULL)::text FROM courses WHERE id = ?`, fx.CourseID) != "true" {
		t.Fatal("enrollment_count/published_version_id changed through PATCH /courses/:id")
	}
	if rec := w.do(instrA, "PATCH", "/api/v1/modules/"+fx.ModuleID.String(), body(map[string]any{"position": 77, "course_version_id": foreignVersion, "id": uuid.NewString()}, map[string]any{"title": "m-upd"})); rec.Code != 200 {
		t.Fatalf("module update: %d", rec.Code)
	}
	if w.scalar(`SELECT position::text FROM modules WHERE id = ?`, fx.ModuleID) != "1" || w.scalar(`SELECT (course_version_id::text <> ?)::text FROM modules WHERE id = ?`, foreignVersion, fx.ModuleID) != "true" {
		t.Fatal("module position / parent version changed through PATCH /modules/:id")
	}
	if rec := w.do(instrA, "PATCH", "/api/v1/lessons/"+fx.LessonID.String(), body(map[string]any{"position": 55, "module_id": uuid.NewString(), "lesson_key": uuid.NewString(), "asset_key": "../../etc/passwd"}, map[string]any{"title": "l-upd", "content_type": "text"})); rec.Code != 200 {
		t.Fatalf("lesson update: %d", rec.Code)
	}
	if w.scalar(`SELECT lesson_key::text FROM lessons WHERE id = ?`, fx.LessonID) != fx.LessonKey.String() || w.scalar(`SELECT position::text FROM lessons WHERE id = ?`, fx.LessonID) != "1" {
		t.Fatal("lesson key/position changed through PATCH /lessons/:id")
	}
}

// Seat accounting under concurrent enroll attempts across students stays exact when driven over HTTP.
func TestEnroll_OverHTTP_ConcurrentSeats_NeverOversold(t *testing.T) {
	w := newWorld(t)
	fx := w.newDraft(instrA)
	if err := w.env.DB.Exec(`UPDATE courses SET enrollment_limit = 3 WHERE id = ?`, fx.CourseID).Error; err != nil {
		t.Fatal(err)
	}
	w.publish(fx.CourseID, instrA)
	// 12 distinct students
	var tokens []string
	for i := 0; i < 12; i++ {
		id := uuid.Must(uuid.NewV7())
		w.env.DB.Exec(`INSERT INTO users (id, email, password_hash, full_name, role, status) VALUES (?, ?, 'x', 's', 'student', 'active')`, id, id.String()+"@e.com")
		tok, _ := issue(id, "student")
		tokens = append(tokens, tok)
	}
	done := make(chan int, len(tokens))
	for _, tok := range tokens {
		go func(tok string) {
			done <- w.doRaw(tok, "POST", "/api/v1/enrollments", map[string]any{"course_id": fx.CourseID.String()}).Code
		}(tok)
	}
	var created, full, other int
	for range tokens {
		switch c := <-done; c {
		case 201:
			created++
		case 409:
			full++
		default:
			other++
		}
	}
	if created != 3 || full != 9 || other != 0 {
		t.Fatalf("limit 3: created=%d full=%d other=%d", created, full, other)
	}
	if w.scalar(`SELECT enrollment_count::text FROM courses WHERE id = ?`, fx.CourseID) != "3" ||
		w.scalar(`SELECT count(*)::text FROM enrollments WHERE course_id = ? AND status='active'`, fx.CourseID) != "3" {
		t.Fatal("enrollment_count and active rows disagree with the limit")
	}
}

var _ = context.Background
var _ = fmt.Sprintf
