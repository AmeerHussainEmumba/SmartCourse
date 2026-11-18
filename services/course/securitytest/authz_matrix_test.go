//go:build integration

package securitytest_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// want codes: "401" anonymous rejected; "403" role-denied; "D" denied by
// ownership (403 or 404, both are acceptable and must not leak data);
// "2xx" allowed.
type matrixCase struct {
	name string
	// build returns method, path and body for the case, with fresh fixtures, plus the secret marker that
	// must never appear in a denied response.
	build func(w *world) (method, path string, body any, secret string)
	// want per actor index: anon, studentA, studentB, instrA, instrB, admin
	want [6]string
}

var allow200 = "2xx"

func courseMatrix() []matrixCase {
	pub := func(w *world) courseFx { return w.newPublished(instrA) }
	return []matrixCase{
		{"GET /courses (public)", func(w *world) (string, string, any, string) { return "GET", "/api/v1/courses", nil, "" },
			[6]string{allow200, allow200, allow200, allow200, allow200, allow200}},
		{"GET /courses/search (public)", func(w *world) (string, string, any, string) { return "GET", "/api/v1/courses/search?q=sec", nil, "" },
			[6]string{allow200, allow200, allow200, allow200, allow200, allow200}},
		{"GET /courses/:slug published (public)", func(w *world) (string, string, any, string) {
			return "GET", "/api/v1/courses/" + pub(w).Slug, nil, ""
		}, [6]string{allow200, allow200, allow200, allow200, allow200, allow200}},
		{"GET /courses/:slug DRAFT is not readable by anyone, owner included", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "GET", "/api/v1/courses/" + fx.Slug, nil, fx.Title
		}, [6]string{"D", "D", "D", "D", "D", "D"}},
		{"GET /courses/:slug ARCHIVED is gone for everyone", func(w *world) (string, string, any, string) {
			fx := pub(w)
			if err := w.svc.ArchiveCourse(ctxBG, fx.CourseID, w.actors[instrA].id); err != nil {
				w.t.Fatal(err)
			}
			return "GET", "/api/v1/courses/" + fx.Slug, nil, fx.Title
		}, [6]string{"D", "D", "D", "D", "D", "D"}},
		{"POST /courses", func(w *world) (string, string, any, string) {
			return "POST", "/api/v1/courses", map[string]any{"title": "T", "slug": "new-" + w.next() + "-" + strings.ToLower(uuid.NewString()[:6])}, ""
		}, [6]string{"401", "403", "403", allow200, allow200, "403"}},
		{"PATCH /courses/:id (draft)", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "PATCH", "/api/v1/courses/" + fx.CourseID.String(), map[string]any{"title": "Renamed"}, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"POST /courses/:id/modules", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "POST", "/api/v1/courses/" + fx.CourseID.String() + "/modules", map[string]any{"title": "M2", "position": 2}, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"POST /courses/:id/publish", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "POST", "/api/v1/courses/" + fx.CourseID.String() + "/publish", nil, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"GET /courses/:id/publish-status", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "GET", "/api/v1/courses/" + fx.CourseID.String() + "/publish-status", nil, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"POST /courses/:id/archive", func(w *world) (string, string, any, string) {
			fx := pub(w)
			return "POST", "/api/v1/courses/" + fx.CourseID.String() + "/archive", nil, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"POST /modules/:id/lessons", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "POST", "/api/v1/modules/" + fx.ModuleID.String() + "/lessons", map[string]any{"title": "L2", "position": 2, "content_type": "text", "content": map[string]any{"b": "x"}}, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"PATCH /modules/:id", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "PATCH", "/api/v1/modules/" + fx.ModuleID.String(), map[string]any{"title": "hijack"}, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"DELETE /modules/:id", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "DELETE", "/api/v1/modules/" + fx.ModuleID.String(), nil, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"PATCH /lessons/:id", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "PATCH", "/api/v1/lessons/" + fx.LessonID.String(), map[string]any{"title": "hijack", "content_type": "text"}, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"DELETE /lessons/:id", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "DELETE", "/api/v1/lessons/" + fx.LessonID.String(), nil, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		{"POST /assets/upload-url", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "POST", "/api/v1/assets/upload-url", map[string]any{"course_id": fx.CourseID.String(), "content_type": "application/pdf", "size_bytes": 1024}, fx.Title
		}, [6]string{"401", "403", "403", allow200, "D", "403"}},
		// Enrollment / progress: "owner" is studentA (the enrolled student). instructors and admin have no override.
		{"POST /enrollments (any authenticated user may enroll)", func(w *world) (string, string, any, string) {
			fx := pub(w)
			return "POST", "/api/v1/enrollments", map[string]any{"course_id": fx.CourseID.String()}, ""
		}, [6]string{"401", allow200, allow200, allow200, allow200, allow200}},
		{"POST /enrollments into a DRAFT-only course is refused", func(w *world) (string, string, any, string) {
			fx := w.newDraft(instrA)
			return "POST", "/api/v1/enrollments", map[string]any{"course_id": fx.CourseID.String()}, fx.Title
		}, [6]string{"401", "D", "D", "D", "D", "D"}},
		{"GET /enrollments (own list only)", func(w *world) (string, string, any, string) {
			fx := pub(w)
			w.enroll(studentA, fx.CourseID)
			w.enroll(studentB, fx.CourseID)
			return "GET", "/api/v1/enrollments", nil, ""
		}, [6]string{"401", allow200, allow200, allow200, allow200, allow200}},
		{"GET /enrollments/:id", func(w *world) (string, string, any, string) {
			fx := pub(w)
			e := w.enroll(studentA, fx.CourseID)
			return "GET", "/api/v1/enrollments/" + e.String(), nil, fx.CourseID.String()
		}, [6]string{"401", allow200, "D", "D", "D", "D"}},
		{"DELETE /enrollments/:id", func(w *world) (string, string, any, string) {
			fx := pub(w)
			e := w.enroll(studentA, fx.CourseID)
			return "DELETE", "/api/v1/enrollments/" + e.String(), nil, fx.CourseID.String()
		}, [6]string{"401", allow200, "D", "D", "D", "D"}},
		{"GET /enrollments/:id/progress", func(w *world) (string, string, any, string) {
			fx := pub(w)
			e := w.enroll(studentA, fx.CourseID)
			return "GET", "/api/v1/enrollments/" + e.String() + "/progress", nil, fx.LessonKey.String()
		}, [6]string{"401", allow200, "D", "D", "D", "D"}},
		{"PUT /enrollments/:id/lessons/:key/complete", func(w *world) (string, string, any, string) {
			fx := pub(w)
			e := w.enroll(studentA, fx.CourseID)
			return "PUT", "/api/v1/enrollments/" + e.String() + "/lessons/" + fx.LessonKey.String() + "/complete", nil, ""
		}, [6]string{"401", allow200, "D", "D", "D", "D"}},
	}
}

// TestAuthzMatrix_EveryCourseEnrollmentProgressRoute walks every registered
// route as anonymous / student / other student / instructor (owner) / other
// instructor / admin against freshly-built fixtures and asserts allow/deny.
func TestAuthzMatrix_EveryCourseEnrollmentProgressRoute(t *testing.T) {
	w := newWorld(t)
	covered := map[string]bool{}
	for _, c := range courseMatrix() {
		for idx := 0; idx < 6; idx++ {
			method, path, body, secret := c.build(w)
			rec := w.do(idx, method, path, body)
			label := fmt.Sprintf("%s as %s", c.name, w.actors[idx].name)
			covered[method+" "+routeShape(path)] = true

			switch want := c.want[idx]; want {
			case "401":
				if rec.Code != 401 {
					t.Errorf("%s: want 401, got %d %.150s", label, rec.Code, rec.Body.String())
				}
			case "403":
				if rec.Code != 403 {
					t.Errorf("%s: want 403, got %d %.150s", label, rec.Code, rec.Body.String())
				}
			case "D":
				if rec.Code != 403 && rec.Code != 404 && rec.Code != 409 {
					t.Errorf("%s: want a denial (403/404/409), got %d %.150s", label, rec.Code, rec.Body.String())
				}
			case "2xx":
				if !okStatus(rec.Code) {
					t.Errorf("%s: want 2xx, got %d %.200s", label, rec.Code, rec.Body.String())
				}
			}
			if rec.Code >= 500 {
				t.Errorf("%s: server error %d", label, rec.Code)
			}
			assertNoInternalLeak(t, label, rec)
			if c.want[idx] != "2xx" && secret != "" && strings.Contains(rec.Body.String(), secret) {
				t.Errorf("%s: denied response leaks the victim's data (%q): %.200s", label, secret, rec.Body.String())
			}
			if c.want[idx] == "2xx" && c.name == "GET /enrollments (own list only)" {
				assertOnlyOwnEnrollments(t, w, idx, rec.Body.Bytes())
			}
		}
	}
	t.Logf("authz matrix exercised %d distinct method+route shapes across 6 actors", len(covered))
	// Every route registered by the three packages must be exercised by this matrix.
	for _, route := range w.r.Routes() {
		if route.Path == "" {
			continue
		}
		key := route.Method + " " + routeShape(route.Path)
		if !covered[key] && !coveredByShape(covered, route.Method, route.Path) {
			t.Errorf("route %s %s is registered but not covered by the authz matrix: add it", route.Method, route.Path)
		}
	}
}

// assertOnlyOwnEnrollments: the list endpoint must never include another student's rows.
func assertOnlyOwnEnrollments(t *testing.T, w *world, idx int, body []byte) {
	var out struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Errorf("list body: %v", err)
		return
	}
	for _, it := range out.Items {
		var owner string
		_ = w.env.DB.Raw(`SELECT student_id::text FROM enrollments WHERE id = ?`, it.ID).Row().Scan(&owner)
		if owner != w.actors[idx].id.String() {
			t.Errorf("%s's enrollment list contains %s owned by %s", w.actors[idx].name, it.ID, owner)
		}
	}
	if idx == studentA && len(out.Items) == 0 {
		t.Error("studentA sees none of their own enrollments")
	}
}

// routeShape normalises concrete ids/slugs so matrix paths can be compared to gin route templates.
func routeShape(p string) string {
	if i := strings.Index(p, "?"); i >= 0 {
		p = p[:i]
	}
	parts := strings.Split(p, "/")
	for i, s := range parts {
		if _, err := uuid.Parse(s); err == nil {
			parts[i] = ":id"
		}
	}
	return strings.Join(parts, "/")
}

// coveredByShape matches gin templates (":id", ":slug", ":lessonKey") with the normalised matrix paths.
func coveredByShape(covered map[string]bool, method, tmpl string) bool {
	norm := tmpl
	for _, ph := range []string{":slug", ":lessonKey"} {
		norm = strings.ReplaceAll(norm, ph, ":id")
	}
	if covered[method+" "+norm] {
		return true
	}
	// public slug routes use a human slug, not a uuid
	if strings.HasSuffix(tmpl, "/courses/:slug") {
		for k := range covered {
			if strings.HasPrefix(k, method+" /api/v1/courses/sec-") {
				return true
			}
		}
	}
	return false
}
