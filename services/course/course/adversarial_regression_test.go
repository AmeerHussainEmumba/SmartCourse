//go:build integration

// Adversarial security tests for the course module's Week 3 surface (security
// tester). TestBugrepro_* fail today and reproduce a finding in
// docs/plans/week-3-findings-security.md; TestAdversarial_* passed when written
// (attack HELD). Run:
//
//	cd services/course && go test -tags='integration bugrepro' -run 'Bugrepro|Adversarial' ./course/
package course_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// HELD: hostile upload-url requests through the real handler.
func TestAdversarial_UploadURL_HostileRequests(t *testing.T) {
	w := newAssetWorld(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	course.RegisterRoutes(r.Group("/api/v1"), w.svc, testJWTCfg())
	tok, _ := auth.IssueAccessToken(testJWTCfg(), w.instr, "instructor", 1)
	otherTok, _ := auth.IssueAccessToken(testJWTCfg(), seedInstructor(t, w.env), "instructor", 1)
	studentTok, _ := auth.IssueAccessToken(testJWTCfg(), uuid.New(), "student", 1)
	do := func(token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/assets/upload-url", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec
	}
	cid := w.courseID.String()
	ok := `{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":10}`
	if rec := do(tok, ok); rec.Code != 200 && rec.Code != 201 {
		t.Fatalf("control: %d %s", rec.Code, rec.Body.String())
	}
	// Other instructor and a student: 404 / 403, identical to an unknown course for the former.
	a, b := do(otherTok, ok), do(otherTok, strings.Replace(ok, cid, uuid.NewString(), 1))
	if a.Code != 404 || a.Body.String() != b.Body.String() {
		t.Errorf("non-owner vs unknown course: %d vs %d (existence oracle)", a.Code, b.Code)
	}
	if rec := do(studentTok, ok); rec.Code != 403 {
		t.Errorf("student: %d", rec.Code)
	}
	bad := []string{
		`{"course_id":"` + cid + `","content_type":"text/html","size_bytes":10}`,
		`{"course_id":"` + cid + `","content_type":"image/svg+xml","size_bytes":10}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf; charset=utf-8","size_bytes":10}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf\r\nX-Evil: 1","size_bytes":10}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf\u0000","size_bytes":10}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":0}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":-5}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":9223372036854775807}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":99999999999999999999}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":1.5}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":"10"}`,
		`{"course_id":"../../etc","content_type":"application/pdf","size_bytes":10}`,
		`{"course_id":"` + cid + `","lesson_id":"../x","content_type":"application/pdf","size_bytes":10}`,
		`{"course_id":"` + cid + `","lesson_id":"` + uuid.NewString() + `","content_type":"application/pdf","size_bytes":10}`,
		`{"course_id":"` + cid + `","content_type":"application/pdf","size_bytes":10,"key":"courses/x/../../y","asset_key":"evil","bucket":"other"}`,
		`[]`, `null`, `{`, ``,
	}
	for _, body := range bad {
		rec := do(tok, body)
		if rec.Code >= 500 {
			t.Errorf("%.60q: %d %s", body, rec.Code, rec.Body.String())
		}
		// A mass-assigned key/bucket field must not influence the key.
		if strings.Contains(body, `"asset_key":"evil"`) && (rec.Code == 200 || rec.Code == 201) &&
			(strings.Contains(rec.Body.String(), "evil") || strings.Contains(rec.Body.String(), "courses/x")) {
			t.Errorf("mass-assigned key reached the response: %s", rec.Body.String())
		}
	}
	// The issued key is scoped to the course and the URL to the configured bucket.
	rec := do(tok, ok)
	if !strings.Contains(rec.Body.String(), `courses/`+cid+`/assets/`) {
		t.Errorf("key not scoped to the course: %s", rec.Body.String())
	}
}
