//go:build integration

package securitytest_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/services/course/assets"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/internal/miniotest"
)

// HTTP-level asset upload against a real MinIO: the route's error contract,
// that client-supplied path-like fields never reach the object key, and the
// upload -> reference -> verify round trip.

type uploadResp struct {
	UploadURL    string            `json:"upload_url"`
	Method       string            `json:"method"`
	Headers      map[string]string `json:"headers"`
	AssetKey     string            `json:"asset_key"`
	ExpiresAt    string            `json:"expires_at"`
	MaxSizeBytes int64             `json:"max_size_bytes"`
}

func TestAssetsHTTP_UploadReferenceVerifyRoundTrip(t *testing.T) {
	w := newWorld(t)
	m := miniotest.Setup(t)
	store, err := assets.New(m.Config)
	if err != nil {
		t.Fatal(err)
	}
	w.svc.WithAssetStore(store)
	fx := w.newDraft(instrA)

	body := []byte("%PDF-1.7 round trip")
	rec := w.do(instrA, "POST", "/api/v1/assets/upload-url", map[string]any{
		"course_id": fx.CourseID.String(), "lesson_id": fx.LessonID.String(), "content_type": "application/pdf", "size_bytes": len(body),
	})
	if rec.Code != 200 {
		t.Fatalf("upload-url: %d %s", rec.Code, rec.Body)
	}
	var up uploadResp
	if err := json.Unmarshal(rec.Body.Bytes(), &up); err != nil {
		t.Fatal(err)
	}
	if up.Method != "PUT" || up.ExpiresAt == "" || !strings.HasPrefix(up.AssetKey, "courses/"+fx.CourseID.String()+"/lessons/"+fx.LessonID.String()+"/") {
		t.Fatalf("response = %+v", up)
	}
	if got := miniotest.Upload(t, up.UploadURL, up.Headers, body); got != 200 {
		t.Fatalf("upload status %d", got)
	}

	// Reference the asset from a new lesson over HTTP, then verify the draft.
	rec = w.do(instrA, "POST", "/api/v1/modules/"+fx.ModuleID.String()+"/lessons", map[string]any{
		"title": "With asset", "position": 2, "content_type": "pdf", "content": map[string]any{}, "asset_key": up.AssetKey,
	})
	if rec.Code != 201 {
		t.Fatalf("add lesson with asset_key: %d %s", rec.Code, rec.Body)
	}
	c, err := w.svc.GetCourse(ctxBG, fx.CourseID)
	if err != nil || c.DraftVersionID == nil {
		t.Fatalf("GetCourse: %v", err)
	}
	if err := w.svc.VerifyAssets(ctxBG, *c.DraftVersionID); err != nil {
		t.Fatalf("VerifyAssets after a real upload: %v", err)
	}
}

func TestAssetsHTTP_ErrorContractAndClientCannotChooseTheKey(t *testing.T) {
	w := newWorld(t)
	fx := w.newDraft(instrA)
	post := func(idx int, body any) (int, string) {
		rec := w.do(idx, "POST", "/api/v1/assets/upload-url", body)
		return rec.Code, rec.Body.String()
	}
	ok := map[string]any{"course_id": fx.CourseID.String(), "content_type": "application/pdf", "size_bytes": 100}
	with := func(k string, v any) map[string]any {
		m := map[string]any{}
		for kk, vv := range ok {
			m[kk] = vv
		}
		m[k] = v
		return m
	}

	// Path-like extras are ignored or refused; they never appear in the key.
	for _, extra := range []map[string]any{
		with("key", "../../etc/passwd"), with("asset_key", "courses/other/x.pdf"), with("filename", "../../evil.pdf"),
		with("path", "/x"), with("bucket", "other-bucket"),
	} {
		code, body := post(instrA, extra)
		if code == 200 {
			var up uploadResp
			_ = json.Unmarshal([]byte(body), &up)
			if !strings.HasPrefix(up.AssetKey, "courses/"+fx.CourseID.String()+"/assets/") || strings.Contains(body, "evil") || strings.Contains(body, "passwd") || strings.Contains(body, "other-bucket") {
				t.Errorf("client-supplied field reached the response: %s", body)
			}
		} else if code != 400 {
			t.Errorf("extras %v: unexpected status %d %s", extra, code, body)
		}
	}

	cases := []struct {
		name string
		body any
		want int
		code string
	}{
		{"disallowed type", with("content_type", "text/html"), 400, "unsupported_content_type"},
		{"executable", with("content_type", "application/x-msdownload"), 400, "unsupported_content_type"},
		{"oversize", with("size_bytes", int64(101)*1024*1024), 413, "asset_too_large"},
		{"zero size", with("size_bytes", 0), 400, "validation_failed"},
		{"negative size", with("size_bytes", -5), 400, "validation_failed"},
		{"missing course", map[string]any{"content_type": "application/pdf", "size_bytes": 1}, 400, "validation_failed"},
		{"course id traversal", with("course_id", "../"+fx.CourseID.String()), 400, "validation_failed"},
		{"empty body", "{}", 400, "validation_failed"},
		{"not json", "{", 400, ""},
	}
	for _, tc := range cases {
		code, body := post(instrA, tc.body)
		if code != tc.want || (tc.code != "" && !strings.Contains(body, tc.code)) {
			t.Errorf("%s: got %d %s, want %d %s", tc.name, code, body, tc.want, tc.code)
		}
		if code >= 500 {
			t.Errorf("%s: server error", tc.name)
		}
	}

	// Authorisation, spelled out (the matrix covers it too): student, other
	// instructor, admin, unauthenticated.
	for _, tc := range []struct {
		idx  int
		want int
	}{{anon, 401}, {studentA, 403}, {instrB, 404}, {admin, 403}} {
		if code, body := post(tc.idx, ok); code != tc.want {
			t.Errorf("%s: got %d %s, want %d", w.actors[tc.idx].name, code, body, tc.want)
		}
	}
	// A token for a nonexistent role/garbage token is unauthenticated.
	if rec := w.doRaw("not-a-token", http.MethodPost, "/api/v1/assets/upload-url", ok); rec.Code != 401 {
		t.Errorf("garbage token: %d", rec.Code)
	}
	// The other instructor cannot learn anything about the course from the denial.
	if _, body := post(instrB, ok); strings.Contains(body, fx.Title) {
		t.Errorf("denial leaks the course: %s", body)
	}
}
