//go:build integration

// Adversarial checks for the analytics HTTP surface (area C/D/E).
package analytics_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/auth"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
)

type secEnv struct {
	t   *testing.T
	env *testenv.Env
	r   *gin.Engine
}

func newSecEnv(t *testing.T) *secEnv {
	env := testenv.Setup(t)
	svc := analytics.NewService(analytics.NewRepository(env.DB))
	return &secEnv{t: t, env: env, r: newTestEngine(t, svc)}
}

func (s *secEnv) token(role string) string {
	tok, err := auth.IssueAccessToken(testJWTCfg(), uuid.New(), role, 1)
	if err != nil {
		s.t.Fatal(err)
	}
	return tok
}

func (s *secEnv) get(path, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.r.ServeHTTP(rec, req)
	return rec
}

func TestSecurity_Analytics_AnonymousAndForgedTokens_Rejected_OnEveryRoute(t *testing.T) {
	s := newSecEnv(t)
	cid := uuid.NewString()
	routes := []string{"/api/v1/analytics/platform", "/api/v1/analytics/courses/" + cid, "/api/v1/analytics/courses/" + cid + "/daily-enrollments"}
	forged := []string{"", "garbage", "Bearer", s.token("admin") + "x", "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ4Iiwicm9sZSI6ImFkbWluIn0."}
	for _, p := range routes {
		for _, tok := range forged {
			rec := s.get(p, tok)
			if rec.Code != 401 {
				t.Errorf("%s with token %.20q: want 401, got %d", p, tok, rec.Code)
			}
		}
		if rec := s.get(p, s.token("admin")); rec.Code != 200 {
			t.Errorf("control: admin call to %s should be 200, got %d", p, rec.Code)
		}
	}
}

func TestSecurity_Analytics_InvalidCourseIDs_AreClientErrors_AndLeakNothing(t *testing.T) {
	s := newSecEnv(t)
	tok := s.token("instructor")
	for _, id := range []string{"abc", "1", "' OR 1=1--", "../../etc/passwd", "%00", strings.Repeat("a", 5000), "00000000-0000-0000-0000-00000000000g", "‮"} {
		for _, suffix := range []string{"", "/daily-enrollments"} {
			rec := s.get("/api/v1/analytics/courses/"+url.PathEscape(id)+suffix, tok)
			if rec.Code != 400 && rec.Code != 404 {
				t.Errorf("id %.30q%s: %d %.120s", id, suffix, rec.Code, rec.Body.String())
			}
			lower := strings.ToLower(rec.Body.String())
			for _, leak := range []string{"pq:", "pgx", "sqlstate", "gorm", "analytics.", "goroutine", ".go:", "uuid:"} {
				if strings.Contains(lower, leak) {
					t.Errorf("id %.30q leaks %q: %s", id, leak, rec.Body.String())
				}
			}
		}
	}
}

// A well-formed but unknown course yields neutral zeros for an admin (documented) rather than an error that
// would let a caller distinguish existing from non-existing courses; and the daily series is empty.
// (Instructors get a 404 for it: they are owner-gated, see instructor_ownership_integration_test.go.)
func TestSecurity_Analytics_UnknownCourse_NeutralResponse(t *testing.T) {
	s := newSecEnv(t)
	tok := s.token("admin")
	a := s.get("/api/v1/analytics/courses/"+uuid.NewString(), tok)
	b := s.get("/api/v1/analytics/courses/"+uuid.NewString(), tok)
	if a.Code != 200 || a.Body.String() != b.Body.String() {
		t.Fatalf("unknown courses are distinguishable: %d %s vs %d %s", a.Code, a.Body.String(), b.Code, b.Body.String())
	}
	if rec := s.get("/api/v1/analytics/courses/"+uuid.NewString()+"/daily-enrollments", tok); rec.Code != 200 || strings.Contains(rec.Body.String(), "null") {
		t.Fatalf("daily-enrollments for unknown course: %d %s", rec.Code, rec.Body.String())
	}
}

// Hostile HTTP shapes must not reach a handler or crash it.
func TestSecurity_Analytics_MethodAndPathTampering(t *testing.T) {
	s := newSecEnv(t)
	tok := s.token("admin")
	for _, m := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		req := httptest.NewRequest(m, "/api/v1/analytics/platform", strings.NewReader(`{"total_students":999999}`))
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		s.r.ServeHTTP(rec, req)
		if rec.Code != 404 && rec.Code != 405 {
			t.Errorf("%s /analytics/platform: %d (read-only module accepted a write verb)", m, rec.Code)
		}
	}
	for _, p := range []string{"/api/v1/analytics/", "/api/v1/analytics", "/api/v1/analytics/courses/", "/api/v1/analytics/../platform", "/api/v1/analytics/%2e%2e/platform"} {
		rec := s.get(p, "")
		if rec.Code == 200 {
			t.Errorf("%s answered 200 without a token", p)
		}
	}
	if w := fmt.Sprint(s.get("/api/v1/analytics/platform", tok).Header()); strings.Contains(strings.ToLower(w), "server:") {
		t.Errorf("Server header present: %s", w)
	}
}
