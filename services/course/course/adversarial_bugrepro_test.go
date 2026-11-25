//go:build integration && bugrepro

// Adversarial security tests for the course module's Week 3 surface (security
// tester). TestBugrepro_* fail today and reproduce a finding in
// docs/plans/week-3-findings-security.md; TestAdversarial_* passed when written
// (attack HELD). Run:
//
//	cd services/course && go test -tags='integration bugrepro' -run 'Bugrepro|Adversarial' ./course/
package course_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// S-08: VerifyAssets' storage failure carries the raw SDK/driver error text
// (internal host, port, bucket path) in the error's Error() string. The
// publish workflow's failPublish stores appErr.Message() (= that text) as
// course_versions.publish_error and in course.publish_failed's reason, which
// the worker emails to the instructor and the analytics issues table keeps. An
// instructor (a low-trust user) can therefore read internal infrastructure
// detail by publishing a lesson whose asset exists while storage is degraded,
// or by any activity error that wraps a driver error.
type failingStat struct{ err error }

func (f failingStat) PresignUpload(context.Context, string, string, int64, time.Duration) (course.PresignedUpload, error) {
	return course.PresignedUpload{}, f.err
}
func (f failingStat) Stat(context.Context, string) (course.AssetInfo, error) {
	return course.AssetInfo{}, f.err
}

func TestBugrepro_VerifyAssets_StorageErrorTextLeaksIntoPublishReason(t *testing.T) {
	w := newAssetWorld(t)
	key := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	w.lesson(t, 1, &key)
	internal := errors.New(`operation error S3: HeadObject, https response error StatusCode: 0, request send failed, Head "http://minio.internal.svc:9000/smartcourse-assets/` + key + `": dial tcp 10.42.7.19:9000: connect: connection refused`)
	svc := w.svc.WithAssetStore(failingStat{internal})
	err := svc.VerifyAssets(context.Background(), w.draftID)
	if err == nil {
		t.Fatal("want an error")
	}
	// This string is what Temporal's ApplicationError.Message() carries to failPublish.
	if s := err.Error(); strings.Contains(s, "10.42.7.19") || strings.Contains(s, "minio.internal") {
		t.Fatalf("the activity error text that becomes the instructor-visible publish reason leaks internals: %q", s)
	}
}

// S-09 (the F-03 class on the instructor surface): the course routes use
// RequireFreshPrivilege(rdb) without a durable source, so after a Redis
// flush, restart or eviction a demoted or suspended instructor's stale access
// token is accepted again for every authoring write (create, edit, publish,
// archive, upload-url) for the rest of its TTL.
func TestBugrepro_Course_RedisFlush_ResurrectsSuspendedInstructorToken(t *testing.T) {
	env := testenv.Setup(t)
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	gin.SetMode(gin.TestMode)
	r := gin.New()
	course.RegisterRoutes(r.Group("/api/v1"), svc, testJWTCfg())
	instr := seedInstructor(t, env)
	tok, _ := auth.IssueAccessToken(testJWTCfg(), instr, "instructor", 1)
	post := func(slug string) *httptest.ResponseRecorder {
		body := `{"title":"T","slug":"` + slug + `"}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/courses", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec
	}
	if err := env.RDB.Set(context.Background(), auth.TokenVersionRedisKey(instr.String()), 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if rec := post("s09-before"); rec.Code != 401 {
		t.Fatalf("precondition: stale token must be refused, got %d", rec.Code)
	}
	if err := env.RDB.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if rec := post("s09-after"); rec.Code == http.StatusCreated {
		t.Fatalf("suspended instructor's stale token created a course after a Redis flush (201)")
	}
}

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
